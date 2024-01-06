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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 * Inlining hash calculation:   2450 ms
 * Replacing branchless code:   2200 ms (sometimes we need to kill the things we love)
 *
 * Best performing JVM on MacBook M2 Pro: 21.0.1-graal
 * `sdk use java 21.0.1-graal`
 *
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./src/test/resources/samples/measurements-10000-unique-keys.txt";

    static final class Measurement {
        int min, max, count;
        long sum;

        public Measurement() {
            this.min = 1000;
            this.max = -1000;
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

    public static void main(String[] args) throws Exception {
        new CalculateAverage_royvanrijn().run();
        // new CalculateAverage_royvanrijn().runTests();
    }

    private void run() throws Exception {

        var results = getFileSegments(new File(FILE)).stream().map(segment -> {

            long segmentEnd = segment.end();
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());

                // Work with any UTF-8 city name, up to 100 in length:
                var cityNameAsLongArray = new long[16];
                var delimiterPointerAndHash = new int[2];

                // Calculate using native ordering (fastest?):
                bb.order(ByteOrder.nativeOrder());

                // Record the order it is and calculate accordingly:
                final boolean bufferIsBigEndian = bb.order().equals(ByteOrder.BIG_ENDIAN);
                MeasurementRepository measurements = new MeasurementRepository();

                int startPointer;
                int limit = bb.limit();
                while ((startPointer = bb.position()) < limit) {

                    int delimiterPointer, endPointer;

                    // SWAR method to find delimiter *and* record the cityname as long[] *and* calculate a hash:
                    findNextDelimiterAndCalculateHash(bb, SEPARATOR_PATTERN, startPointer, limit, delimiterPointerAndHash, cityNameAsLongArray, bufferIsBigEndian);
                    delimiterPointer = delimiterPointerAndHash[0];

                    // Simple lookup is faster for '\n' (just three options)
                    if (delimiterPointer >= limit) {
                        return measurements;
                    }
                    // Extract the measurement value (10x):
                    final int cityNameLength = delimiterPointer - startPointer;

                    int measuredValue;
                    int neg = 1;
                    if (bb.get(++delimiterPointer) == '-') {
                        neg = -1;
                        delimiterPointer++;
                    }
                    byte dot;
                    if ((dot = (bb.get(delimiterPointer + 1))) == '.') {
                        measuredValue = neg * ((bb.get(delimiterPointer)) * 10 + (bb.get(delimiterPointer + 2)) - 528);
                        endPointer = delimiterPointer + 3;
                    }
                    else {
                        measuredValue = neg * (bb.get(delimiterPointer) * 100 + dot * 10 + bb.get(delimiterPointer + 3) - 5328);
                        endPointer = delimiterPointer + 4;
                    }

                    // Store everything in a custom hashtable:
                    measurements.update(cityNameAsLongArray, bb, cityNameLength, delimiterPointerAndHash[1]).updateWith(measuredValue);

                    bb.position(endPointer + 1); // skip to next line.
                }
                return measurements;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel()
                .flatMap(v -> v.get())
                .collect(Collectors.toMap(e -> e.cityName, MeasurementRepository.Entry::measurement, Measurement::updateWith, TreeMap::new));

        System.out.println(results);

        // System.out.println("Processed: " + results.entrySet().stream().mapToLong(e -> e.getValue().count).sum());
    }

    /**
     * -------- This section contains SWAR code (SIMD Within A Register) which processes a bytebuffer as longs to find values:
     */
    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');

    /**
     * Already looping the longs here, lets shoehorn in making a hash
     */
    private void findNextDelimiterAndCalculateHash(final ByteBuffer bb, final long pattern, final int start, final int limit, final int[] output,
                                                   final long[] asLong, final boolean bufferBigEndian) {
        int hash = 1;
        int i;
        int lCnt = 0;
        for (i = start; i <= limit - 8; i += 8) {
            long word = bb.getLong(i);
            if (bufferBigEndian) {
                word = Long.reverseBytes(word); // Reversing the bytes is the cheapest way to do this
            }
            final long match = word ^ pattern;
            long mask = ((match - 0x0101010101010101L) & ~match) & 0x8080808080808080L;

            if (mask != 0) {
                final int index = Long.numberOfTrailingZeros(mask) >> 3;
                output[0] = (i + index);

                final long partialHash = word & ((mask >> 7) - 1);
                asLong[lCnt] = partialHash;
                output[1] = longHashStep(hash, partialHash);
                return;
            }
            asLong[lCnt++] = word;
            hash = longHashStep(hash, word);
        }
        // Handle remaining bytes near the limit of the buffer:
        long partialHash = 0;
        int len = 0;
        for (; i < limit; i++) {
            byte read;
            if ((read = bb.get(i)) == (byte) pattern) {
                asLong[lCnt] = partialHash;
                output[0] = i;
                output[1] = longHashStep(hash, partialHash);
                return;
            }
            partialHash = partialHash | ((long) read << (len << 3));
            len++;
        }
        output[0] = limit; // delimiter not found
    }

    private static int longHashStep(final int hash, final long word) {
        return 31 * hash + (int) (word ^ (word >>> 32));
    }

    private static long compilePattern(final byte value) {
        return ((long) value << 56) | ((long) value << 48) | ((long) value << 40) | ((long) value << 32) |
                ((long) value << 24) | ((long) value << 16) | ((long) value << 8) | (long) value;
    }

    record FileSegment(long start, long end) {
    }

    private static List<FileSegment> getFileSegments(final File file) throws IOException {
        final int numberOfSegments = Runtime.getRuntime().availableProcessors();
        final long fileSize = file.length();
        final long segmentSize = fileSize / numberOfSegments;
        final List<FileSegment> segments = new ArrayList<>();
        if (segmentSize < 1000) {
            segments.add(new FileSegment(0, fileSize));
            return segments;
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long segStart = 0;
            long segEnd = segmentSize;
            while (segStart < fileSize) {
                segEnd = findSegment(randomAccessFile, segEnd, fileSize);
                segments.add(new FileSegment(segStart, segEnd));
                segStart = segEnd; // Just re-use the end and go from there.
                segEnd = Math.min(fileSize, segEnd + segmentSize);
            }
        }
        return segments;
    }

    private static long findSegment(RandomAccessFile raf, long location, final long fileSize) throws IOException {
        raf.seek(location);
        while (location < fileSize) {
            location++;
            if (raf.read() == '\n')
                return location;
        }
        return location;
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
    class MeasurementRepository {
        private int tableSize = 1 << 20; // can grow in theory, made large enough not to (this is faster)
        private int tableMask = (tableSize - 1);
        private int tableLimit = (int) (tableSize * LOAD_FACTOR);
        private int tableFilled = 0;
        private static final float LOAD_FACTOR = 0.8f;

        private Entry[] table = new Entry[tableSize];

        record Entry(int hash, long[] nameBytesInLong, String cityName, Measurement measurement) {
            @Override
            public String toString() {
                return cityName + "=" + measurement;
            }
        }

        public Measurement update(long[] nameBytesInLong, ByteBuffer bb, int length, int calculatedHash) {

            final int nameBytesInLongLength = 1 + (length >>> 3);

            int index = calculatedHash & tableMask;
            Entry tableEntry;
            while ((tableEntry = table[index]) != null
                    && (tableEntry.hash != calculatedHash || !arrayEquals(tableEntry.nameBytesInLong, nameBytesInLong, nameBytesInLongLength))) { // search for the right spot
                index = (index + 1) & tableMask;
            }

            if (tableEntry != null) {
                return tableEntry.measurement;
            }

            // --- This is a brand new entry, insert into the hashtable and do the extra calculations (once!) do slower calculations here.
            Measurement measurement = new Measurement();

            // Now create a string:
            byte[] buffer = new byte[length];
            bb.get(buffer, 0, length);
            String cityName = new String(buffer, 0, length);

            // Store the long[] for faster equals:
            long[] nameBytesInLongCopy = new long[nameBytesInLongLength];
            System.arraycopy(nameBytesInLong, 0, nameBytesInLongCopy, 0, nameBytesInLongLength);

            // And add entry:
            Entry toAdd = new Entry(calculatedHash, nameBytesInLongCopy, cityName, measurement);
            table[index] = toAdd;

            // Resize the table if filled too much:
            if (++tableFilled > tableLimit) {
                resizeTable();
            }

            return toAdd.measurement;
        }

        private void resizeTable() {
            // Resize the table:
            Entry[] oldEntries = table;
            table = new Entry[tableSize <<= 2]; // x2
            tableMask = (tableSize - 1);
            tableLimit = (int) (tableSize * LOAD_FACTOR);

            for (Entry entry : oldEntries) {
                if (entry != null) {
                    int updatedTableIndex = entry.hash & tableMask;
                    while (table[updatedTableIndex] != null) {
                        updatedTableIndex = (updatedTableIndex + 1) & tableMask;
                    }
                    table[updatedTableIndex] = entry;
                }
            }
        }

        public Stream<Entry> get() {
            return Arrays.stream(table).filter(Objects::nonNull);
        }
    }

    /**
     * For case multiple hashes are equal (however unlikely) check the actual key (using longs)
     */
    private boolean arrayEquals(final long[] a, final long[] b, final int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }

    public void runTests() {
        // Method used for debugging purposes, easy to make mistakes with all the bit hacking.

        // These all have the same hashes:
        testInput("Delft;-12.4", 0, true, new int[]{ 5, 1718384401 }, new long[]{ 499934586180L });
        testInput("aDelft;-12.4", 1, true, new int[]{ 6, 1718384401 }, new long[]{ 499934586180L });

        testInput("Delft;-12.4", 0, false, new int[]{ 5, 1718384401 }, new long[]{ 499934586180L });
        testInput("aDelft;-12.4", 1, false, new int[]{ 6, 1718384401 }, new long[]{ 499934586180L });

        testInput("Rotterdam;-12.4", 0, true, new int[]{ 9, -784321989 }, new long[]{ 7017859899421126482L, 109L });
        testInput("abcdefghijklmnpoqrstuvwxyzRotterdam;-12.4", 26, true, new int[]{ 35, -784321989 }, new long[]{ 7017859899421126482L, 109L });
        testInput("abcdefghijklmnpoqrstuvwxyzARotterdam;-12.4", 27, true, new int[]{ 36, -784321989 }, new long[]{ 7017859899421126482L, 109L });

        testInput("Rotterdam;-12.4", 0, false, new int[]{ 9, -784321989 }, new long[]{ 7017859899421126482L, 109L });
        testInput("abcdefghijklmnpoqrstuvwxyzRotterdam;-12.4", 26, false, new int[]{ 35, -784321989 }, new long[]{ 7017859899421126482L, 109L });
        testInput("abcdefghijklmnpoqrstuvwxyzARotterdam;-12.4", 27, false, new int[]{ 36, -784321989 }, new long[]{ 7017859899421126482L, 109L });

        // These have different hashes from the strings above:
        testInput("abcdefghijklmnpoqrstuvwxyzAROtterdam;-12.4", 27, true, new int[]{ 36, -792194501 }, new long[]{ 7017859899421118290L, 109L });
        testInput("abcdefghijklmnpoqrstuvwxyzAROtterdam;-12.4", 27, false, new int[]{ 36, -792194501 }, new long[]{ 7017859899421118290L, 109L });
    }

    private void testInput(final String inputString, final int start, final boolean bigEndian, final int[] expectedDelimiterAndHash, final long[] expectedCityNameLong) {

        byte[] input = inputString.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.wrap(input).order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);

        int[] output = new int[2];
        long[] cityName = new long[128];
        findNextDelimiterAndCalculateHash(buffer, SEPARATOR_PATTERN, start, buffer.limit(), output, cityName, bigEndian);

        if (!Arrays.equals(output, expectedDelimiterAndHash)) {
            System.out.println("Error in delimiter or hash");
            System.out.println("Expected: " + Arrays.toString(expectedDelimiterAndHash));
            System.out.println("Received: " + Arrays.toString(output));
        }
        int amountLong = 1 + ((output[0] - start) >>> 3);
        if (!Arrays.equals(cityName, 0, amountLong, expectedCityNameLong, 0, amountLong)) {
            System.out.println("Error in long array");
            System.out.println("Expected: " + Arrays.toString(expectedCityNameLong));
            System.out.println("Received: " + Arrays.toString(cityName));
        }
    }
}
