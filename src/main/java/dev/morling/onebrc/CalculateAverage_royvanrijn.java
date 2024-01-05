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
 * Inlining hash calculation:   2450 ms
 *
 * Best performing JVM on MacBook M2 Pro: 21.0.1-graal
 * `sdk use java 21.0.1-graal`
 *
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";

    // mutable state now instead of records, ugh, less instantiation.
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

    private void run() throws Exception {

        var results = getFileSegments(new File(FILE)).stream().map(segment -> {

            long segmentEnd = segment.end();
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());

                // Work with any UTF-8 city name, up to 100 in length:
                var buffer = new byte[106]; // 100 + ; + -XX.X + \n
                var cityNameAsLongArray = new long[13]; // 13*8=104=kenough.
                var delimiterPointerAndHash = new int[2];

                // Calculate using native ordering (fastest?):
                bb.order(ByteOrder.nativeOrder());

                // Record the order it is and calculate accordingly:
                final boolean bufferIsBigEndian = bb.order().equals(ByteOrder.BIG_ENDIAN);
                MeasurementRepository measurements = new MeasurementRepository();

                int startPointer;
                int limit = bb.limit();
                while ((startPointer = bb.position()) < limit) {

                    // SWAR method to find delimiter *and* record the cityname as long[] *and* calculate a hash:
                    findNextDelimiterAndCalculateHash(bb, SEPARATOR_PATTERN, startPointer, limit, delimiterPointerAndHash, cityNameAsLongArray, bufferIsBigEndian);
                    int delimiterPointer = delimiterPointerAndHash[0];

                    // Simple lookup is faster for '\n' (just three options)
                    int endPointer;

                    if (delimiterPointer >= limit) {
                        bb.position(limit); // skip to next line.
                        return measurements;
                    }

                    if (bb.get(delimiterPointer + 4) == '\n') {
                        endPointer = delimiterPointer + 4;
                    }
                    else if (bb.get(delimiterPointer + 5) == '\n') {
                        endPointer = delimiterPointer + 5;
                    }
                    else {
                        endPointer = delimiterPointer + 6;
                    }

                    // Read the entry in a single get():
                    bb.get(buffer, 0, endPointer - startPointer);
                    bb.position(endPointer + 1); // skip to next line.

                    // Extract the measurement value (10x):
                    final int cityNameLength = delimiterPointer - startPointer;
                    final int measuredValueLength = endPointer - delimiterPointer - 1;
                    final int measuredValue = branchlessParseInt(buffer, cityNameLength + 1, measuredValueLength);

                    // Store everything in a custom hashtable:
                    measurements.update(buffer, cityNameLength, delimiterPointerAndHash[1], cityNameAsLongArray).updateWith(measuredValue);
                }
                return measurements;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel()
                .flatMap(v -> v.values.stream())
                .collect(Collectors.toMap(e -> e.cityName, MeasurementRepository.Entry::measurement, Measurement::updateWith, TreeMap::new));

        System.out.println(results);
    }

    /**
     * -------- This section contains SWAR code (SIMD Within A Register) which processes a bytebuffer as longs to find values:
     */
    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');
    private static final long[] PARTIAL_INDEX_MASKS = new long[]{ 0L, 255L, 65535L, 16777215L, 4294967295L, 1099511627775L, 281474976710655L, 72057594037927935L };

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

        MeasurementRepository repository = new MeasurementRepository();

        // Simulate adding two entries with the same hash:
        byte[] b1 = "City1;10.0".getBytes();
        byte[] b2 = "City2;41.1".getBytes();
        repository.update(b1, 5, 1234, new long[]{ 1234L });
        repository.update(b2, 5, 1234, new long[]{ 4321L });
        // And update the same record shouldn't add a third (this happened):
        repository.update(b1, 5, 1234, new long[]{ 1234L });

        if (repository.values.size() != 2) {
            System.out.println("Error, should have two entries:");
            System.out.println(repository.values);
        }

        MeasurementRepository.Entry firstInserted = repository.values.getFirst();
        if (!firstInserted.cityName.equals("City1")) {
            System.out.println("Error, should have correct name: " + firstInserted.cityName);
        }
    }

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
            if (bufferBigEndian)
                word = Long.reverseBytes(word); // Reversing the bytes is the cheapest way to do this
            int index = firstAnyPattern(word, pattern);
            if (index < Long.BYTES) {
                final long partialHash = word & PARTIAL_INDEX_MASKS[index];
                asLong[lCnt] = partialHash;
                hash = 961 * hash + 31 * (int) (partialHash >>> 32) + (int) partialHash;
                output[0] = (i + index);
                output[1] = hash;
                return;
            }
            asLong[lCnt++] = word;
            hash = 961 * hash + 31 * (int) (word >>> 32) + (int) word;
        }
        // Handle remaining bytes
        long partialHash = 0;
        for (; i < limit; i++) {
            byte read;
            if ((read = bb.get(i)) == (byte) pattern) {
                asLong[lCnt] = partialHash;
                hash = 961 * hash + 31 * (int) (partialHash >>> 32) + (int) partialHash;
                output[0] = i;
                output[1] = hash;
                return;
            }
            partialHash = partialHash << 8 | read;
        }
        output[0] = limit; // delimiter not found
        output[1] = hash;
    }

    private static long compilePattern(final byte value) {
        return ((long) value << 56) | ((long) value << 48) | ((long) value << 40) | ((long) value << 32) |
                ((long) value << 24) | ((long) value << 16) | ((long) value << 8) | (long) value;
    }

    private static int firstAnyPattern(final long word, final long pattern) {
        final long match = word ^ pattern;
        long mask = match - 0x0101010101010101L;
        mask &= ~match;
        mask &= 0x8080808080808080L;
        return Long.numberOfTrailingZeros(mask) >> 3;
    }

    record FileSegment(long start, long end) {
    }

    /** Using this way to segment the file is much prettier, from spullara */
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

    private static long findSegment(final int i, final int skipSegment, RandomAccessFile raf, long location, final long fileSize) throws IOException {
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
    private static int branchlessParseInt(final byte[] input, final int start, final int length) {
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
    class MeasurementRepository {
        private int size = 16384;// 16384; // Much larger than the number of cities, needs power of two
        private int[] indices = new int[size]; // Hashtable is just an int[]

        MeasurementRepository() {
            populateEmptyIndices(indices);
        }

        private void populateEmptyIndices(int[] array) {
            // Optimized fill with -1, fastest method:
            int len = array.length;
            array[0] = -1;
            // Value of i will be [1, 2, 4, 8, 16, 32, ..., len]
            for (int i = 1; i < len; i += i) {
                System.arraycopy(array, 0, array, i, i);
            }
        }

        private final List<Entry> values = new ArrayList<>(512);

        record Entry(int hash, long[] cityNameAsLong, String cityName, Measurement measurement) {
            @Override
            public String toString() {
                return cityName + "=" + measurement;
            }
        }

        public Measurement update(byte[] buffer, int length, int calculatedHash, long[] cityNameAsLongArray) {

            final int cityNameAsLongLength = 1 + (length >>> 3); // amount of longs that captures this cityname

            int hashtableIndex = (size - 1) & calculatedHash;
            int valueIndex;

            Entry retrievedEntry = null;

            while (true) { // search for the right spot
                if ((valueIndex = indices[hashtableIndex]) == -1) {
                    break; // Empty slot found, stop the loop
                }
                else {
                    // Non-empty slot, retrieve entry
                    if ((retrievedEntry = values.get(valueIndex)).hash == calculatedHash &&
                            arrayEquals(retrievedEntry.cityNameAsLong, cityNameAsLongArray, cityNameAsLongLength)) {
                        break; // Both hash and cityname match, stop the loop
                    }
                }
                // Move to the next index
                hashtableIndex = (hashtableIndex + 1) % size;
            }

            if (valueIndex >= 0) {
                return retrievedEntry.measurement;
            }

            // --- This is a brand new entry, insert into the hashtable and do the extra calculations (once!)

            // Keep the already processed longs for fast equals:
            long[] cityNameAsLongArrayCopy = new long[cityNameAsLongLength];
            System.arraycopy(cityNameAsLongArray, 0, cityNameAsLongArrayCopy, 0, cityNameAsLongLength);

            Entry toAdd = new Entry(calculatedHash, cityNameAsLongArrayCopy, new String(buffer, 0, length), new Measurement());

            // Code to regrow (if we get more unique entries): (not needed/not optimized yet)
            // if (values.size() > size / 2) {
            // // We probably don't want this...
            //
            // int newSize = size << 1;
            // int[] newIndices = new int[newSize];
            // populateEmptyIndices(newIndices);
            // for (int i = 0; i < values.size(); i++) {
            // Entry e = values.get(i);
            // int updatedIndex = (newSize - 1) & e.hash;
            // newIndices[updatedIndex] = i;
            // }
            // indices = newIndices;
            // size = newSize;
            // }
            indices[hashtableIndex] = values.size();

            values.add(toAdd);
            return toAdd.measurement;
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
}
