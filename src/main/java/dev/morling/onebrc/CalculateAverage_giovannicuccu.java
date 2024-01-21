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

import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.io.RandomAccessFile;
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
 Solution without unsafe that borrows the ideas of splullara, thomasvue, royvanrijn
 */

public class CalculateAverage_giovannicuccu {

    private static final String FILE = "./measurements.txt";

    public static record PartitionBoundary(long start, long end) {
    }

    public static interface PartitionCalculator {
        PartitionBoundary[] computePartitionsBoundaries(Path path);
    }

    public static class ProcessorPartitionCalculator implements PartitionCalculator {

        public PartitionBoundary[] computePartitionsBoundaries(Path path) {
            try {
                int numberOfSegments = Runtime.getRuntime().availableProcessors();
                long fileSize = path.toFile().length();
                long segmentSize = fileSize / numberOfSegments;
                PartitionBoundary[] segmentBoundaries = new PartitionBoundary[numberOfSegments];
                try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r")) {
                    long segStart = 0;
                    long segEnd = segmentSize;
                    for (int i = 0; i < numberOfSegments; i++) {
                        segEnd = findEndSegment(randomAccessFile, segEnd, fileSize);
                        segmentBoundaries[i] = new PartitionBoundary(segStart, segEnd);
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

    public static class MeasurementAggregator {
        private final int hash;
        private int min;
        private int max;
        private double sum;
        private long count;
        private final byte[] station;
        private final int offset;
        private final String name;

        private final long[] data;
        private final int dataOffset;

        public MeasurementAggregator(byte[] station, int offset, int hash, int initialValue, long[] data, int dataOffset) {
            min = initialValue;
            max = initialValue;
            sum = initialValue;
            count = 1;
            this.station = station;
            this.offset = offset;
            this.hash = hash;
            this.data = data;
            this.dataOffset = dataOffset;
            this.name = new String(station, 0, offset, StandardCharsets.UTF_8);
        }

        public MeasurementAggregator(byte[] station, int offset, int hash, int initialValue) {
            min = initialValue;
            max = initialValue;
            sum = initialValue;
            count = 1;
            this.station = station;
            this.offset = offset;
            this.hash = hash;
            this.data = new long[0];
            this.dataOffset = 0;
            this.name = new String(station, 0, offset, StandardCharsets.UTF_8);
        }

        public boolean hasSameStation(byte[] stationIn, int offsetIn) {
            return Arrays.equals(stationIn, 0, offsetIn, station, 0, offset);
        }

        public boolean hasSameStation(long[] dataIn, int offsetIn) {
            return Arrays.equals(dataIn, 0, offsetIn, data, 0, dataOffset);
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

        public void merge(MeasurementAggregator other) {
            // System.out.println("min=" +min + " other min=" +other.min);
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        @Override
        public String toString() {
            return round((double) min / 10) + "/" + round((sum / (double) count) / 10) + "/" + round((double) max / 10);
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

        public String getName() {
            return name;
        }

        public byte[] getStation() {
            return station;
        }

        public int getOffset() {
            return offset;
        }

        public long[] getData() {
            return data;
        }

    }

    public static class MeasurementList {

        private static final int SIZE = 1024 * 64;
        private final MeasurementAggregator[] measurements = new MeasurementAggregator[SIZE];

        public void add(byte[] station, int offset, int hash, int value) {
            int index = hash & (SIZE - 1);
            if (measurements[index] == null) {
                measurements[index] = new MeasurementAggregator(station.clone(), offset, hash, value);
            }
            else {
                if (measurements[index].hasSameStation(station, offset)) {
                    measurements[index].add(value);
                }
                else {
                    while (measurements[index] != null && !measurements[index].hasSameStation(station, offset)) {
                        index = (index + 1) & (SIZE - 1);
                    }
                    if (measurements[index] == null) {
                        measurements[index] = new MeasurementAggregator(station.clone(), offset, hash, value);
                    }
                    else {
                        measurements[index].add(value);
                    }
                }
            }
        }

        public void merge(MeasurementAggregator measurementAggregator) {
            int index = (measurementAggregator.getHash() & (SIZE - 1));
            if (measurements[index] == null) {
                measurements[index] = measurementAggregator;
            }
            else {
                while (measurements[index] != null && !measurements[index].hasSameStation(measurementAggregator.getStation(), measurementAggregator.getOffset())) {
                    index = (index + 1) & (SIZE - 1);
                }
                if (measurements[index] == null) {
                    measurements[index] = measurementAggregator;
                }
                else {
                    measurements[index].merge(measurementAggregator);
                }
            }
        }

        public MeasurementAggregator[] getMeasurements() {
            return measurements;
        }
    }

    public static class MMapReader {
        private final Path path;
        private final PartitionBoundary[] boundaries;

        private final boolean serial;

        public MMapReader(Path path, PartitionCalculator partitionCalculator, boolean serial) {
            this.path = path;
            this.serial = serial;
            boundaries = partitionCalculator.computePartitionsBoundaries(path);
        }

        public TreeMap<String, MeasurementAggregator> elaborate() {
            try (ExecutorService executor = Executors.newFixedThreadPool(boundaries.length)) {
                List<Future<MeasurementList>> futures = new ArrayList<>();
                for (PartitionBoundary boundary : boundaries) {
                    if (serial) {
                        FutureTask<MeasurementList> future = new FutureTask<>(() -> computeListForPartition(boundary.start(), boundary.end()));
                        future.run();
                        // System.out.println("done with partition " + boundary);
                        futures.add(future);
                    }
                    else {
                        Future<MeasurementList> future = executor.submit(() -> computeListForPartition(boundary.start(), boundary.end()));
                        futures.add(future);
                    }
                }
                TreeMap<String, MeasurementAggregator> ris = reduce(futures);
                return ris;
            }
        }

        private TreeMap<String, MeasurementAggregator> reduce(List<Future<MeasurementList>> futures) {
            try {
                TreeMap<String, MeasurementAggregator> risMap = new TreeMap<>();
                MeasurementList ris = new MeasurementList();
                for (Future<MeasurementList> future : futures) {
                    MeasurementList results = future.get();
                    merge(ris, results);
                }
                for (MeasurementAggregator m : ris.getMeasurements()) {
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

        private void merge(MeasurementList result, MeasurementList partial) {
            for (MeasurementAggregator m : partial.getMeasurements()) {
                if (m != null) {
                    result.merge(m);
                }
            }
        }

        private MeasurementList computeListForPartition(long start, long end) {
            MeasurementList list = new MeasurementList();
            try {
                try (FileChannel fileChannel = (FileChannel) Files.newByteChannel((path), StandardOpenOption.READ)) {
                    MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, end - start);
                    mappedByteBuffer.order(BYTE_ORDER.LITTLE_ENDIAN);
                    int limit = mappedByteBuffer.limit();
                    int startLine;
                    byte[] stationb = new byte[100];
                    while ((startLine = mappedByteBuffer.position()) < limit - 110) {
                        int currentPosition = startLine;
                        byte b = 0;
                        int i = 0;
                        int hash = 0;

                        while ((b = mappedByteBuffer.get(currentPosition++)) != ';') {
                            stationb[i++] = b;
                            hash = 31 * hash + b;
                        }
                        if (hash < 0) {
                            hash = -hash;
                        }

                        long numberWord = mappedByteBuffer.getLong(currentPosition);
                        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
                        int value = convertIntoNumber(decimalSepPos, numberWord);
                        mappedByteBuffer.position(currentPosition + (decimalSepPos >>> 3) + 3);

                        list.add(stationb, i, hash, value);

                    }
                    while ((startLine = mappedByteBuffer.position()) < limit) {
                        int currentPosition = startLine;
                        byte b = 0;
                        int i = 0;
                        int hash = 0;
                        while ((b = mappedByteBuffer.get(currentPosition++)) != ';') {
                            stationb[i++] = b;
                            hash = 31 * hash + b;
                        }
                        if (hash < 0) {
                            hash = -hash;
                        }

                        int value = 0;
                        if (currentPosition <= limit - 8) {
                            long numberWord = mappedByteBuffer.getLong(currentPosition);
                            int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
                            value = convertIntoNumber(decimalSepPos, numberWord);
                            mappedByteBuffer.position(currentPosition + (decimalSepPos >>> 3) + 3);
                        }
                        else {
                            int sign = 1;
                            b = mappedByteBuffer.get(currentPosition++);
                            if (b == '-') {
                                sign = -1;
                            }
                            else {
                                value = b - '0';
                            }
                            while ((b = mappedByteBuffer.get(currentPosition++)) != '.') {
                                value = value * 10 + (b - '0');
                            }
                            b = mappedByteBuffer.get(currentPosition);
                            value = value * 10 + (b - '0');
                            if (sign == -1) {
                                value = -value;
                            }
                            mappedByteBuffer.position(currentPosition + 2);
                        }

                        list.add(stationb, i, hash, value);
                    }
                }
            }
            catch (IOException e) {
                System.out.println("Error");
                System.err.println(e);
            }
            return list;
        }

        private static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

        private static long getLongLittleEndian(long value) {
            value = Long.reverseBytes(value);
            return value;
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

        private static long[] masks = new long[]{ 0x0000000000000000, 0xFF00000000000000L, 0xFFFF000000000000L,
                0xFFFFFF0000000000L, 0xFFFFFFFF00000000L, 0xFFFFFFFFFF000000L, 0xFFFFFFFFFF0000L, 0xFFFFFFFFFFFF00L };

    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        MMapReader reader = new MMapReader(Paths.get(FILE), new ProcessorPartitionCalculator(), false);
        Map<String, MeasurementAggregator> measurements = reader.elaborate();
        // System.out.println("ela=" + (System.currentTimeMillis() - start));
        System.out.println(measurements);

    }
}
