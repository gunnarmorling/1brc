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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import sun.misc.Unsafe;

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
 * Added unsafe memory access:  1900 ms (keeping the long[] small and local)
 *
 * Best performing JVM on MacBook M2 Pro: 21.0.1-graal
 * `sdk use java 21.0.1-graal`
 *
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();
    private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        new CalculateAverage_royvanrijn().run();
    }

    public void run() throws Exception {

        // Calculate input segments.
        int numberOfChunks = Runtime.getRuntime().availableProcessors();
        long[] chunks = getSegments(numberOfChunks);

        // Parallel processing of segments.
        TreeMap<String, Measurement> results = IntStream.range(0, chunks.length - 1)
                .mapToObj(chunkIndex -> process(chunks[chunkIndex], chunks[chunkIndex + 1])).parallel()
                .flatMap(MeasurementRepository::get)
                .collect(Collectors.toMap(e -> e.city, MeasurementRepository.Entry::measurement, Measurement::updateWith, TreeMap::new));

        System.out.println(results);
    }

    private static long[] getSegments(int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            long[] chunks = new long[numberOfChunks + 1];
            long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = mappedAddress;
            long endAddress = mappedAddress + fileSize;
            for (int i = 1; i < numberOfChunks; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (chunkAddress < endAddress && UNSAFE.getByte(chunkAddress++) != '\n') {
                    // nop
                }
                chunks[i] = Math.min(chunkAddress, endAddress);
            }
            chunks[numberOfChunks] = endAddress;
            return chunks;
        }
    }

    private MeasurementRepository process(long fromAddress, long toAddress) {

        MeasurementRepository repository = new MeasurementRepository();
        long ptr = fromAddress;
        long[] dataBuffer = new long[16];
        while ((ptr = processEntity(dataBuffer, ptr, toAddress, repository)) < toAddress)
            ;

        return repository;
    }

    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');

    /**
     * Already looping the longs here, lets shoehorn in making a hash
     */
    private long processEntity(final long[] data, final long start, final long limit, final MeasurementRepository measurementRepository) {
        int hash = 1;
        long i;
        int dataPtr = 0;
        for (i = start; i <= limit - 8; i += 8) {
            long word = UNSAFE.getLong(i);
            if (isBigEndian) {
                word = Long.reverseBytes(word); // Reversing the bytes is the cheapest way to do this
            }
            final long match = word ^ SEPARATOR_PATTERN;
            long mask = ((match - 0x0101010101010101L) & ~match) & 0x8080808080808080L;

            if (mask != 0) {

                final long partialWord = word & ((mask >> 7) - 1);
                hash = longHashStep(hash, partialWord);
                data[dataPtr] = partialWord;

                final int index = Long.numberOfTrailingZeros(mask) >> 3;
                return process(start, i + index, hash, data, measurementRepository);
            }
            data[dataPtr++] = word;
            hash = longHashStep(hash, word);
        }
        // Handle remaining bytes near the limit of the buffer:
        long partialWord = 0;
        int len = 0;
        for (; i < limit; i++) {
            byte read;
            if ((read = UNSAFE.getByte(i)) == ';') {
                hash = longHashStep(hash, partialWord);
                data[dataPtr] = partialWord;
                return process(start, i, hash, data, measurementRepository);
            }
            partialWord = partialWord | ((long) read << (len << 3));
            len++;
        }
        return limit;
    }

    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    private long process(final long startAddress, final long delimiterAddress, final int hash, final long[] data, final MeasurementRepository measurementRepository) {

        long word = UNSAFE.getLong(delimiterAddress + 1);
        if (isBigEndian) {
            word = Long.reverseBytes(word);
        }
        final long invWord = ~word;
        final int decimalSepPos = Long.numberOfTrailingZeros(invWord & DOT_BITS);
        final long signed = (invWord << 59) >> 63;
        final long designMask = ~(signed & 0xFF);
        final long digits = ((word & designMask) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        final int measurement = (int) ((absValue ^ signed) - signed);

        // Store:
        measurementRepository.update(startAddress, data, (int) (delimiterAddress - startAddress), hash, measurement);

        return delimiterAddress + (decimalSepPos >> 3) + 4; // Determine next start:
        // return nextAddress;
    }

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

    private static int longHashStep(final int hash, final long word) {
        return 31 * hash + (int) (word ^ (word >>> 32));
    }

    private static long compilePattern(final byte value) {
        return ((long) value << 56) | ((long) value << 48) | ((long) value << 40) | ((long) value << 32) |
                ((long) value << 24) | ((long) value << 16) | ((long) value << 8) | (long) value;
    }

    /**
     * A normal Java HashMap does all these safety things like boundary checks... we don't need that, we need speeeed.
     *
     * So I've written an extremely simple linear probing hashmap that should work well enough.
     */
    class MeasurementRepository {
        private int tableSize = 1 << 20; // large enough for the contest.
        private int tableMask = (tableSize - 1);

        private MeasurementRepository.Entry[] table = new MeasurementRepository.Entry[tableSize];

        record Entry(long address, long[] data, int length, int hash, String city, Measurement measurement) {

            @Override
            public String toString() {
                return city + "=" + measurement;
            }
        }

        public void update(long address, long[] data, int length, int hash, int temperature) {

            int dataLength = length >> 3;
            int index = hash & tableMask;
            MeasurementRepository.Entry tableEntry;
            while ((tableEntry = table[index]) != null
                    && (tableEntry.hash != hash || tableEntry.length != length || !arrayEquals(tableEntry.data, data, dataLength))) { // search for the right spot
                index = (index + 1) & tableMask;
            }

            if (tableEntry != null) {
                tableEntry.measurement.updateWith(temperature);
                return;
            }

            // --- This is a brand new entry, insert into the hashtable and do the extra calculations (once!) do slower calculations here.
            Measurement measurement = new Measurement();

            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = UNSAFE.getByte(address + i);
            }
            String city = new String(bytes);

            long[] dataCopy = new long[dataLength];
            System.arraycopy(data, 0, dataCopy, 0, dataLength);

            // And add entry:
            MeasurementRepository.Entry toAdd = new MeasurementRepository.Entry(address, dataCopy, length, hash, city, measurement);
            table[index] = toAdd;

            toAdd.measurement.updateWith(temperature);
        }

        public Stream<MeasurementRepository.Entry> get() {
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

}
