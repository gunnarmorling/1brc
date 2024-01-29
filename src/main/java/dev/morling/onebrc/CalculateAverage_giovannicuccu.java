/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

/*
 Solution without unsafe that borrows the ideas of splullara, thomasvue, royvanrijn and merykitty
 */

public class CalculateAverage_giovannicuccu {

    private static final String FILE = "./measurements.txt";

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
    private static final int BYTE_SPECIES_LANES = BYTE_SPECIES.length();
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();
    public static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;
    public static final int INT_SPECIES_LANES = INT_SPECIES.length();

    public static final int KEY_SIZE = 128;

    public static record PartitionBoundary(Path path, long start, long end) {
    }

    public static interface PartitionCalculator {
        List<PartitionBoundary> computePartitionsBoundaries(Path path);
    }

    public static class ProcessorPartitionCalculator implements PartitionCalculator {

        public List<PartitionBoundary> computePartitionsBoundaries(Path path) {
            try {
                int numberOfSegments = Runtime.getRuntime().availableProcessors();
                long fileSize = path.toFile().length();
                long segmentSize = fileSize / numberOfSegments;
                List<PartitionBoundary> segmentBoundaries = new ArrayList<>(numberOfSegments);
                try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r")) {
                    long segStart = 0;
                    long segEnd = segmentSize;
                    for (int i = 0; i < numberOfSegments; i++) {
                        segEnd = findEndSegment(randomAccessFile, segEnd, fileSize);
                        segmentBoundaries.add(new PartitionBoundary(path, segStart, segEnd));
                        segStart = segEnd;
                        segEnd = Math.min(segEnd + segmentSize, fileSize);
                    }
                }
                return segmentBoundaries;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private long findEndSegment(RandomAccessFile raf, long location, long fileSize) throws IOException {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == 10)
                    break;
            }
            return location;
        }
    }

    private static class MeasurementAggregatorVectorized {

        private int min;
        private int max;
        private double sum;
        private long count;
        private final int len;
        private final int hash;

        private final int offset;
        private byte[] data;

        public MeasurementAggregatorVectorized(byte[] data, int offset, int len, int hash, int initialValue) {
            min = initialValue;
            max = initialValue;
            sum = initialValue;
            count = 1;
            this.len = len;
            this.hash = hash;
            this.offset = offset;
            this.data = data;
        }

        public void add(int value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
        }

        public void merge(MeasurementAggregatorVectorized other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        @Override
        public String toString() {
            return round(min / 10.) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public int getMin() {
            return min;
        }

        public int getHash() {
            return hash;
        }

        public int getLen() {
            return len;
        }

        public boolean dataEquals(byte[] data, int offset) {
            return Arrays.equals(this.data, this.offset, this.offset + len, data, offset, offset + len);

        }

        public String getName() {
            return new String(data, offset, len, StandardCharsets.UTF_8);
        }

        public int getOffset() {
            return offset;
        }

        public byte[] getData() {
            return data;
        }
    }

    private static class MeasurementListVectorized {
        private static final int SIZE = 1024 * 64;
        private final MeasurementAggregatorVectorized[] measurements = new MeasurementAggregatorVectorized[SIZE];
        private final byte[] keyData = new byte[SIZE * KEY_SIZE];

        private final MemorySegment dataSegment = MemorySegment.ofArray(keyData);

        private final byte[] lineData = new byte[SIZE];

        private final MemorySegment lineSegment = MemorySegment.ofArray(lineData);

        public void add(int len, int hash, int value, MemorySegment memorySegment, long offset) {
            MemorySegment.copy(memorySegment, offset, lineSegment, 0, len);
            int index = hash & (SIZE - 1);
            while (measurements[index] != null) {
                if (measurements[index].getHash() == hash && measurements[index].getLen() == len) {
                    if (Arrays.equals(keyData, index * KEY_SIZE, index * KEY_SIZE + len, lineData, 0, len)) {
                        measurements[index].add(value);
                        return;
                    }
                }
                index = (index + 1) & (SIZE - 1);
            }
            MemorySegment.copy(memorySegment, offset, dataSegment, (long) index * KEY_SIZE, len);
            measurements[index] = new MeasurementAggregatorVectorized(keyData, index * KEY_SIZE, len, hash, value);
        }

        public void addWithByteVector(ByteVector chunk1, int len, int hash, int value, MemorySegment memorySegment, long offset) {
            int index = hash & (SIZE - 1);
            while (measurements[index] != null) {
                if (measurements[index].getLen() == len && measurements[index].getHash() == hash) {
                    var nodeKey = ByteVector.fromArray(BYTE_SPECIES, keyData, index * KEY_SIZE);
                    long eqMask = chunk1.compare(VectorOperators.EQ, nodeKey).toLong();
                    long validMask = -1L >>> (64 - len);
                    if ((eqMask & validMask) == validMask) {
                        measurements[index].add(value);
                        return;
                    }
                }
                index = (index + 1) & (SIZE - 1);
            }
            MemorySegment.copy(memorySegment, offset, dataSegment, (long) index * KEY_SIZE, len);
            measurements[index] = new MeasurementAggregatorVectorized(keyData, index * KEY_SIZE, len, hash, value);
        }

        public void merge(MeasurementAggregatorVectorized measurementAggregator) {
            int index = measurementAggregator.getHash() & (SIZE - 1);
            while (measurements[index] != null) {
                if (measurements[index].getLen() == measurementAggregator.getLen() && measurements[index].getHash() == measurementAggregator.getHash()) {
                    if (measurementAggregator.dataEquals(measurements[index].getData(), measurements[index].getOffset())) {
                        measurements[index].merge(measurementAggregator);
                        return;
                    }
                }
                index = (index + 1) & (SIZE - 1);
            }
            measurements[index] = measurementAggregator;
        }

        public MeasurementAggregatorVectorized[] getMeasurements() {
            return measurements;
        }

    }

    private static class MMapReaderMemorySegment {

        private final Path path;
        private final List<PartitionBoundary> boundaries;
        private final boolean serial;
        private static final ValueLayout.OfLong JAVA_LONG_LT = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

        public MMapReaderMemorySegment(Path path, PartitionCalculator partitionCalculator, boolean serial) {
            this.path = path;
            this.serial = serial;
            boundaries = partitionCalculator.computePartitionsBoundaries(path);
        }

        public TreeMap<String, MeasurementAggregatorVectorized> elaborate() throws IOException {
            try (ExecutorService executor = Executors.newFixedThreadPool(boundaries.size());
                    FileChannel fileChannel = (FileChannel) Files.newByteChannel((path), StandardOpenOption.READ);
                    var arena = Arena.ofShared()) {

                List<Future<MeasurementListVectorized>> futures = new ArrayList<>();
                for (PartitionBoundary boundary : boundaries) {
                    if (serial) {
                        FutureTask<MeasurementListVectorized> future = new FutureTask<>(() -> computeListForPartition(
                                fileChannel, boundary));
                        future.run();
                        futures.add(future);
                    }
                    else {
                        Future<MeasurementListVectorized> future = executor.submit(() -> computeListForPartition(
                                fileChannel, boundary));
                        futures.add(future);
                    }
                }
                TreeMap<String, MeasurementAggregatorVectorized> ris = reduce(futures);
                return ris;
            }
        }

        private TreeMap<String, MeasurementAggregatorVectorized> reduce(List<Future<MeasurementListVectorized>> futures) {
            try {
                TreeMap<String, MeasurementAggregatorVectorized> risMap = new TreeMap<>();
                MeasurementListVectorized ris = new MeasurementListVectorized();
                for (Future<MeasurementListVectorized> future : futures) {
                    MeasurementListVectorized results = future.get();
                    merge(ris, results);
                }
                for (MeasurementAggregatorVectorized m : ris.getMeasurements()) {
                    if (m != null) {
                        risMap.put(m.getName(), m);
                    }
                }
                return risMap;
            }
            catch (InterruptedException | ExecutionException ie) {
                System.err.println(ie);
                throw new RuntimeException(ie);
            }
        }

        private void merge(MeasurementListVectorized result, MeasurementListVectorized partial) {
            for (MeasurementAggregatorVectorized m : partial.getMeasurements()) {
                if (m != null) {
                    result.merge(m);
                }
            }
        }

        private final long ALL_ONE = -1L;
        private static final long DELIMITER_MASK = 0x3B3B3B3B3B3B3B3BL;

        private static final byte SEPARATOR = ';';
        private final static ByteVector SEPARATORS = ByteVector.broadcast(BYTE_SPECIES, SEPARATOR);

        private MeasurementListVectorized computeListForPartition(FileChannel fileChannel, PartitionBoundary boundary) {
            try (var arena = Arena.ofConfined()) {
                var memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, boundary.start(), boundary.end() - boundary.start(), arena);
                MeasurementListVectorized list = new MeasurementListVectorized();
                long size = memorySegment.byteSize();
                long offset = 0;
                long safe = size - KEY_SIZE;
                while (offset < safe) {
                    int len = 0;
                    var line = ByteVector.fromMemorySegment(BYTE_SPECIES, memorySegment, offset, NATIVE_ORDER);
                    len = line.compare(VectorOperators.EQ, SEPARATORS).firstTrue();
                    if (len == BYTE_SPECIES_LANES) {
                        int position1 = -1;
                        int incr = BYTE_SPECIES_LANES;
                        while (position1 == -1) {
                            long readBuffer = memorySegment.get(JAVA_LONG_LT, offset + incr);
                            long comparisonResult1 = (readBuffer ^ DELIMITER_MASK);
                            long highBitMask1 = (comparisonResult1 - 0x0101010101010101L) & (~comparisonResult1 & 0x8080808080808080L);

                            boolean noContent1 = highBitMask1 == 0;
                            position1 = noContent1 ? -1 : Long.numberOfTrailingZeros(highBitMask1) >> 3;
                            len += noContent1 ? 8 : position1;
                            incr += 8;
                        }
                        int hash = hash(memorySegment, offset, len);
                        long prevOffset = offset;
                        offset += len + 1;

                        long numberWord = memorySegment.get(JAVA_LONG_LT, offset);
                        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
                        int value = convertIntoNumber(decimalSepPos, numberWord);
                        offset += (decimalSepPos >>> 3) + 3;
                        list.add(len, hash, value, memorySegment, prevOffset);
                    }
                    else {
                        int hash = hash(memorySegment, offset, len);
                        long prevOffset = offset;
                        offset += len + 1;

                        long numberWord = memorySegment.get(JAVA_LONG_LT, offset);
                        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
                        int value = convertIntoNumber(decimalSepPos, numberWord);
                        offset += (decimalSepPos >>> 3) + 3;
                        list.addWithByteVector(line, len, hash, value, memorySegment, prevOffset);
                    }
                }

                while (offset < size) {
                    int len = 0;
                    while (memorySegment.get(ValueLayout.JAVA_BYTE, offset + len) != ';') {
                        len++;
                    }
                    int hash = hash(memorySegment, offset, len);
                    long prevOffset = offset;
                    offset += len + 1;

                    int value = 0;
                    if (offset < size - 8) {
                        long numberWord = memorySegment.get(JAVA_LONG_LT, offset);
                        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
                        value = convertIntoNumber(decimalSepPos, numberWord);
                        offset += (decimalSepPos >>> 3) + 3;
                    }
                    else {
                        long currentPosition = offset;
                        int sign = 1;
                        byte b = memorySegment.get(ValueLayout.JAVA_BYTE, currentPosition++);
                        if (b == '-') {
                            sign = -1;
                        }
                        else {
                            value = b - '0';
                        }
                        while ((b = memorySegment.get(ValueLayout.JAVA_BYTE, currentPosition++)) != '.') {
                            value = value * 10 + (b - '0');
                        }
                        b = memorySegment.get(ValueLayout.JAVA_BYTE, currentPosition);
                        value = value * 10 + (b - '0');
                        if (sign == -1) {
                            value = -value;
                        }
                        offset = currentPosition + 2;
                    }
                    list.add(len, hash, value, memorySegment, prevOffset);
                }
                return list;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static final int GOLDEN_RATIO = 0x9E3779B9;
        private static final int HASH_LROTATE = 5;

        private static int hash(MemorySegment memorySegment, long start, int len) {
            int x;
            int y;
            if (len >= Integer.BYTES) {
                x = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, start);
                y = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, start + len - Integer.BYTES);
            }
            else {
                x = memorySegment.get(ValueLayout.JAVA_BYTE, start);
                y = memorySegment.get(ValueLayout.JAVA_BYTE, start + len - Byte.BYTES);
            }
            return (Integer.rotateLeft(x * GOLDEN_RATIO, HASH_LROTATE) ^ y) * GOLDEN_RATIO;
        }

        private static int convertIntoNumber(int decimalSepPos, long numberWord) {
            int shift = 28 - decimalSepPos;
            // signed is -1 if negative, 0 otherwise
            long signed = (~numberWord << 59) >> 63;
            long designMask = ~(signed & 0xFF);
            // Align the number to a specific position and transform the ascii code
            // to actual digit value in each byte
            long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;

            // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
            // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
            // 0x000000UU00TTHH00 +
            // 0x00UU00TTHH000000 * 10 +
            // 0xUU00TTHH00000000 * 100
            // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
            // This results in our value lies in the bit 32 to 41 of this product
            // That was close :)
            long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            long value = (absValue ^ signed) - signed;
            return (int) value;
        }

    }

    public static void main(String[] args) throws IOException {
        MMapReaderMemorySegment reader = new MMapReaderMemorySegment(Paths.get(FILE), new ProcessorPartitionCalculator(), false);
        Map<String, MeasurementAggregatorVectorized> measurements = reader.elaborate();
        System.out.println(measurements);

    }
}
