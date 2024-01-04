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

import org.radughiorma.Arguments;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Changelog:
 *
 * Initial submission:          62000 ms
 * Chunked reader:              16000 ms
 * Optimized parser:            13000 ms
 * Branchless methods:          11000 ms
 * Adding memory mapped files:  6500 ms (based on bjhara's submission)
 * Skipping string creation:    4700 ms
 * Custom hashmap...            4200 ms
 * Added SWAR token checks:     3900 ms
 * Skipped String creation:     3500 ms (idea from kgonia)
 * Improved String skip:        3250 ms
 * Segmenting files:            3150 ms (based on spullara's code)
 * Not using SWAR for EOL:      2850 ms
 *
 * Best performing JVM on MacBook M2 Pro: 21.0.1-graal
 * `sdk use java 21.0.1-graal`
 *
 */
public class CalculateAverage_royvanrijn {

    private static String FILE;

    // mutable state now instead of records, ugh, less instantiation.
    static final class Measurement {
        int min, max, count;
        long sum;

        public Measurement() {
            this.min = 10000;
            this.max = -10000;
        }

        public Measurement updateWith(int measurement) {
            min = min(min, measurement);
            max = max(max, measurement);
            sum += measurement;
            count++;
            return this;
        }

        public Measurement updateWith(Measurement measurement) {
            min = min(min, measurement.min);
            max = max(max, measurement.max);
            sum += measurement.sum;
            count += measurement.count;
            return this;
        }

        public String toString() {
            return round(min) + "/" + round((1.0 * sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    public static final void main(String[] args) throws Exception {
        FILE = Arguments.measurmentsFilename(args);
        new CalculateAverage_royvanrijn().run();
    }

    private void run() throws Exception {

        var results = getFileSegments(new File(FILE)).stream().map(segment -> {

            long segmentEnd = segment.end();
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());
                var buffer = new byte[64];

                // Force little endian:
                bb.order(ByteOrder.LITTLE_ENDIAN);

                BitTwiddledMap measurements = new BitTwiddledMap();

                int startPointer;
                int limit = bb.limit();
                while ((startPointer = bb.position()) < limit) {

                    // SWAR is faster for ';'
                    int separatorPointer = findNextSWAR(bb, SEPARATOR_PATTERN, startPointer + 3, limit);

                    // Simple is faster for '\n' (just three options)
                    int endPointer;
                    if (bb.get(separatorPointer + 4) == '\n') {
                        endPointer = separatorPointer + 4;
                    }
                    else if (bb.get(separatorPointer + 5) == '\n') {
                        endPointer = separatorPointer + 5;
                    }
                    else {
                        endPointer = separatorPointer + 6;
                    }

                    // Read the entry in a single get():
                    bb.get(buffer, 0, endPointer - startPointer);
                    bb.position(endPointer + 1); // skip to next line.

                    // Extract the measurement value (10x):
                    final int nameLength = separatorPointer - startPointer;
                    final int valueLength = endPointer - separatorPointer - 1;
                    final int measured = branchlessParseInt(buffer, nameLength + 1, valueLength);
                    measurements.getOrCreate(buffer, nameLength).updateWith(measured);
                }
                return measurements;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel().flatMap(v -> v.values.stream())
                .collect(Collectors.toMap(e -> new String(e.key), BitTwiddledMap.Entry::measurement, (m1, m2) -> m1.updateWith(m2), TreeMap::new));

        // Seems to perform better than actually using a TreeMap:
        System.out.println(results);
    }

    /**
     * -------- This section contains SWAR code (SIMD Within A Register) which processes a bytebuffer as longs to find values:
     */
    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');

    private int findNextSWAR(ByteBuffer bb, long pattern, int start, int limit) {
        int i;
        for (i = start; i <= limit - 8; i += 8) {
            long word = bb.getLong(i);
            int index = firstAnyPattern(word, pattern);
            if (index < Long.BYTES) {
                return i + index;
            }
        }
        // Handle remaining bytes
        for (; i < limit; i++) {
            if (bb.get(i) == (byte) pattern) {
                return i;
            }
        }
        return limit; // delimiter not found
    }

    private static long compilePattern(byte value) {
        return ((long) value << 56) | ((long) value << 48) | ((long) value << 40) | ((long) value << 32) |
                ((long) value << 24) | ((long) value << 16) | ((long) value << 8) | (long) value;
    }

    private static int firstAnyPattern(long word, long pattern) {
        final long match = word ^ pattern;
        long mask = match - 0x0101010101010101L;
        mask &= ~match;
        mask &= 0x8080808080808080L;
        return Long.numberOfTrailingZeros(mask) >>> 3;
    }

    record FileSegment(long start, long end) {
    }

    /** Using this way to segment the file is much prettier, from spullara */
    private static List<FileSegment> getFileSegments(File file) throws IOException {
        final int numberOfSegments = Runtime.getRuntime().availableProcessors();
        final long fileSize = file.length();
        final long segmentSize = fileSize / numberOfSegments;
        final List<FileSegment> segments = new ArrayList<>();
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

    private static long findSegment(int i, int skipSegment, RandomAccessFile raf, long location, long fileSize) throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    return location;
            }
        }
        return location;
    }

    /**
     * Branchless parser, goes from String to int (10x):
     * "-1.2" to -12
     * "40.1" to 401
     * etc.
     *
     * @param input
     * @return int value x10
     */
    private static int branchlessParseInt(final byte[] input, int start, int length) {
        // 0 if positive, 1 if negative
        final int negative = ~(input[start] >> 4) & 1;
        // 0 if nr length is 3, 1 if length is 4
        final int has4 = ((length - negative) >> 2) & 1;

        final int digit1 = input[start + negative] - '0';
        final int digit2 = input[start + negative + has4];
        final int digit3 = input[start + negative + has4 + 2];

        return (-negative ^ (has4 * (digit1 * 100) + digit2 * 10 + digit3 - 528) - negative); // 528 == ('0' * 10 + '0')
    }

    // branchless max (unprecise for large numbers, but good enough)
    static int max(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    // branchless min (unprecise for large numbers, but good enough)
    static int min(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
    }

    /**
     * A normal Java HashMap does all these safety things like boundary checks... we don't need that, we need speeeed.
     *
     * So I've written an extremely simple linear probing hashmap that should work well enough.
     */
    class BitTwiddledMap {
        private static final int SIZE = 16384; // A bit larger than the number of keys, needs power of two
        private int[] indices = new int[SIZE]; // Hashtable is just an int[]

        BitTwiddledMap() {
            // Optimized fill with -1, fastest method:
            int len = indices.length;
            if (len > 0) {
                indices[0] = -1;
            }
            // Value of i will be [1, 2, 4, 8, 16, 32, ..., len]
            for (int i = 1; i < len; i += i) {
                System.arraycopy(indices, 0, indices, i, i);
            }
        }

        private List<Entry> values = new ArrayList<>(512);

        record Entry(int hash, byte[] key, Measurement measurement) {
            @Override
            public String toString() {
                return new String(key) + "=" + measurement;
            }
        }

        /**
         * Who needs methods like add(), merge(), compute() etc, we need one, getOrCreate.
         * @param key
         * @return
         */
        public Measurement getOrCreate(byte[] key, int length) {
            int inHash;
            int index = (SIZE - 1) & (inHash = hashCode(key, length));
            int valueIndex;
            Entry retrievedEntry = null;
            while ((valueIndex = indices[index]) != -1 && (retrievedEntry = values.get(valueIndex)).hash != inHash) {
                index = (index + 1) % SIZE;
            }
            if (valueIndex >= 0) {
                return retrievedEntry.measurement;
            }
            // New entry, insert into table and return.
            indices[index] = values.size();

            // Only parse this once:
            byte[] actualKey = new byte[length];
            System.arraycopy(key, 0, actualKey, 0, length);

            Entry toAdd = new Entry(inHash, actualKey, new Measurement());
            values.add(toAdd);
            return toAdd.measurement;
        }

        private static int hashCode(byte[] a, int length) {
            int result = 1;
            for (int i = 0; i < length; i++) {
                result = 31 * result + a[i];
            }
            return result;
        }
    }

}
