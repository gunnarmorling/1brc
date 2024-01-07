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

import jdk.incubator.vector.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

// based on spullara's submission

public class CalculateAverage_asun {
    private static final String FILE = "./measurements.txt";

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;
    private static final int VECTOR_SIZE = 32;

    private static final ByteVector ASC;
    static {
        byte[] bytes = new byte[VECTOR_SIZE];
        for (int i = 0; i < VECTOR_SIZE; i++) {
            bytes[i] = (byte) i;
        }

        ASC = ByteVector.fromArray(BYTE_SPECIES, bytes, 0);
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        var filename = args.length == 0 ? FILE : args[0];
        var file = new File(filename);

        List<FileSegment> fileSegments = getFileSegments(file);
        // System.out.println(System.currentTimeMillis() - start);
        var resultsMap = fileSegments.stream().map(segment -> {
            var resultMap = new ByteArrayToResultMap();
            long segmentEnd = segment.end();
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(filename), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());
                var ms = MemorySegment.ofBuffer(bb);

                // Up to 100 characters for a city name
                var buffer = new byte[100 + VECTOR_SIZE];
                long startLine;
                long pos = 0;
                long limit = ms.byteSize();
                long vectorLimit = limit - VECTOR_SIZE;

                // int[] lastHashMult = new int[]{ 7, 31, 63, 15, 255, 127, 3, 511 };
                // IntVector lastMul = IntVector.fromArray(INT_SPECIES, lastHashMult, 0);

                vector: while ((startLine = pos) < vectorLimit) {
                    long currentPosition = startLine;
                    ByteVector r;
                    VectorMask<Byte> m;
                    int offset = 0;

                    IntVector h = IntVector.zero(INT_SPECIES);
                    while (true) {
                        if (currentPosition >= vectorLimit) {
                            break vector;
                        }

                        r = ByteVector.fromMemorySegment(BYTE_SPECIES, ms, currentPosition, ByteOrder.LITTLE_ENDIAN);
                        r.intoArray(buffer, offset);
                        offset += VECTOR_SIZE;
                        m = r.eq((byte) ';');
                        if (m.anyTrue()) {
                            int firstTrue = m.firstTrue();
                            currentPosition += firstTrue;
                            // note: target platform likely does not have AVX-512, so manipulating and using m directly is likely to be slow
                            ByteVector lastMask = (ByteVector) ASC.lt((byte) firstTrue).toVector();
                            h = h.mul(31);
                            h = h.add(r.and(lastMask).reinterpretAsInts());
                            break;
                        }
                        else {
                            currentPosition += VECTOR_SIZE;
                            h = h.mul(31);
                            h = h.add(r.reinterpretAsInts());
                        }
                    }

                    // h = h.mul(lastMul);

                    int hash = h.reduceLanes(VectorOperators.ADD);

                    // currentPosition now has index of semicolon
                    int nameLen = (int) (currentPosition - startLine);
                    currentPosition++;

                    if (currentPosition >= limit - 8) {
                        break;
                    }

                    long g = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, currentPosition);
                    int negative = (g & 0xff) == '-' ? -1 : 1;

                    // 00101101 MINUS
                    // 00101110 PERIOD
                    // 00001101 CR
                    // 00001010 LF
                    // 00110000 0
                    // 00111001 9

                    // scan for LF
                    long lf = ~g & 0x20202020202020L;
                    int tzc = Long.numberOfTrailingZeros(lf);
                    long bytesToLF = tzc / 8;

                    int shift = 64 - tzc & 0b111000;

                    long reversedDigits = Long.reverseBytes(g) >> shift;
                    long digitBits = reversedDigits & (0x1010101010101010L >> shift);
                    long digitsExt = (digitBits >> 1 | digitBits >> 2 | digitBits >> 3 | digitBits >> 4);

                    long digitsOnly = Long.compress(reversedDigits, digitsExt);

                    long temp = (digitsOnly & 0xf)
                            + 10 * ((digitsOnly >> 4) & 0xf)
                            + 100 * ((digitsOnly >> 8) & 0xf);

                    temp *= negative;

                    currentPosition += bytesToLF + 1;

                    resultMap.putOrMerge(buffer, 0, nameLen, temp, hash);
                    pos = currentPosition;

                }

                while ((startLine = pos) < limit) {
                    long currentPosition = startLine;
                    byte b;
                    int offset = 0;

                    while (currentPosition != segmentEnd && (b = ms.get(ValueLayout.JAVA_BYTE, currentPosition++)) != ';') {
                        buffer[offset++] = b;
                    }
                    // Invariant: the remaining length is less than VECTOR_SIZE, so we can just run the last round of hashing
                    int hash = ByteVector.fromArray(BYTE_SPECIES, buffer, 0, ASC.lt((byte) offset))
                            .reinterpretAsInts()
                            // .mul(lastMul)
                            .reduceLanes(VectorOperators.ADD);

                    int temp;
                    int negative = 1;
                    // Inspired by @yemreinci to unroll this even further
                    if (ms.get(ValueLayout.JAVA_BYTE, currentPosition) == '-') {
                        negative = -1;
                        currentPosition++;
                    }
                    if (ms.get(ValueLayout.JAVA_BYTE, currentPosition + 1) == '.') {
                        temp = negative * ((ms.get(ValueLayout.JAVA_BYTE, currentPosition) - '0') * 10 + (ms.get(ValueLayout.JAVA_BYTE, currentPosition + 2) - '0'));
                        currentPosition += 3;
                    }
                    else {
                        temp = negative * ((ms.get(ValueLayout.JAVA_BYTE, currentPosition) - '0') * 100
                                + ((ms.get(ValueLayout.JAVA_BYTE, currentPosition + 1) - '0') * 10 + (ms.get(ValueLayout.JAVA_BYTE, currentPosition + 3) - '0')));
                        currentPosition += 4;
                    }
                    if (ms.get(ValueLayout.JAVA_BYTE, currentPosition) == '\r') {
                        currentPosition++;
                    }
                    currentPosition++;
                    resultMap.putOrMerge(buffer, 0, offset, temp, hash);
                    pos = currentPosition;
                }
                return resultMap;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel().flatMap(partition -> partition.getAll().stream())
                .collect(Collectors.toMap(e -> new String(e.key()), Entry::value, CalculateAverage_asun::merge, TreeMap::new));

        System.out.println(resultsMap);

        // System.out.println(System.currentTimeMillis() - start);

        Runtime.getRuntime().halt(0);
    }

    private static List<FileSegment> getFileSegments(File file) throws IOException {
        int numberOfSegments = Runtime.getRuntime().availableProcessors() * 8;
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfSegments;
        List<FileSegment> segments = new ArrayList<>(numberOfSegments);
        // Pointless to split small files
        if (segmentSize < 1_000_000) {
            segments.add(new FileSegment(0, fileSize));
            return segments;
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < numberOfSegments; i++) {
                long segStart = i * segmentSize;
                long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
                segStart = findSegment(i, 0, randomAccessFile, segStart, segEnd);
                segEnd = findSegment(i, numberOfSegments - 1, randomAccessFile, segEnd, fileSize);

                segments.add(new FileSegment(segStart, segEnd));
            }
        }
        return segments;
    }

    private static Result merge(Result v, Result value) {
        return merge(v, value.min, value.max, value.sum, value.count);
    }

    private static Result merge(Result v, long value, long value1, long value2, long value3) {
        v.min = Math.min(v.min, value);
        v.max = Math.max(v.max, value1);
        v.sum += value2;
        v.count += value3;
        return v;
    }

    private static long findSegment(int i, int skipSegment, RandomAccessFile raf, long location, long fileSize) throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    break;
            }
        }
        return location;
    }

    static class Result {
        long min, max, sum;
        long count;

        Result(long value) {
            min = max = sum = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round((sum / 10.0) / count) + "/" + round(max);
        }

        double round(double v) {
            return Math.round(v * 10.0) / 10.0;
        }

        double round(long v) {
            return v / 10.0;
        }

    }

    record Entry(byte[] key, Result value) {
    }

    record FileSegment(long start, long end) {
    }

    static class ByteArrayToResultMap {
        public static final int MAPSIZE = 1024 * 128;
        Result[] slots = new Result[MAPSIZE];
        byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(byte[] key, int offset, int size, long temp, int hash) {
            int slot = hash & (slots.length - 1);
            var slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            Result value = slotValue;
            if (value == null) {
                slots[slot] = new Result(temp);
                byte[] bytes = new byte[size];
                System.arraycopy(key, offset, bytes, 0, size);
                keys[slot] = bytes;
            }
            else {
                value.min = Math.min(value.min, temp);
                value.max = Math.max(value.max, temp);
                value.sum += temp;
                value.count += 1;
            }
        }

        // Get all pairs
        public List<Entry> getAll() {
            List<Entry> result = new ArrayList<>(slots.length);
            for (int i = 0; i < slots.length; i++) {
                Result slotValue = slots[i];
                if (slotValue != null) {
                    result.add(new Entry(keys[i], slotValue));
                }
            }
            return result;
        }
    }
}
