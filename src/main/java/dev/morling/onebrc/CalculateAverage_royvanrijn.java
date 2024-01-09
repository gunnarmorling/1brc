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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import sun.misc.Unsafe;

/**
 * Changelog:
 *
 * Initial submission:               62000 ms
 * Chunked reader:                   16000 ms
 * Optimized parser:                 13000 ms
 * Branchless methods:               11000 ms
 * Adding memory mapped files:       6500 ms (based on bjhara's submission)
 * Skipping string creation:         4700 ms
 * Custom hashmap...                 4200 ms
 * Added SWAR token checks:          3900 ms
 * Skipped String creation:          3500 ms (idea from kgonia)
 * Improved String skip:             3250 ms
 * Segmenting files:                 3150 ms (based on spullara's code)
 * Not using SWAR for EOL:           2850 ms
 * Inlining hash calculation:        2450 ms
 * Replacing branchless code:        2200 ms (sometimes we need to kill the things we love)
 * Added unsafe memory access:       1900 ms (keeping the long[] small and local)
 * Fixed bug, UNSAFE bytes String:   1850 ms
 * Separate hash from entries:       1550 ms
 * Various tweaks for Linux/cache    1550 ms (should/could make a difference on target machine)
 *
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();
    private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    private static Unsafe initUnsafe() {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
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
        final int numberOfChunks = Runtime.getRuntime().availableProcessors();
        final long[] chunks = getSegments(numberOfChunks);

        final List<MeasurementRepository> repositories = IntStream.range(0, chunks.length - 1)
                .mapToObj(chunkIndex -> processMemoryArea(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .parallel()
                .toList();

        // Sometimes simple is better:
        final HashMap<String, MeasurementRepository.Entry> measurements = HashMap.newHashMap(1 << 10);
        for (MeasurementRepository repository : repositories) {
            for (MeasurementRepository.Entry entry : repository.table) {
                if (entry != null)
                    measurements.merge(entry.city, entry, MeasurementRepository.Entry::mergeWith);
            }
        }

        System.out.print("{" +
                measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    /**
     * Simpler way to get the segments and launch parallel processing by thomaswue
     */
    private static long[] getSegments(final int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            final long fileSize = fileChannel.size();
            final long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            final long[] chunks = new long[numberOfChunks + 1];
            final long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = mappedAddress;
            final long endAddress = mappedAddress + fileSize;
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

    private MeasurementRepository processMemoryArea(final long fromAddress, final long toAddress) {

        MeasurementRepository repository = new MeasurementRepository();
        long ptr = fromAddress;
        final long[] dataBuffer = new long[16];
        while ((ptr = processName(dataBuffer, ptr, toAddress, repository)) < toAddress) {
            // empty loop
        }

        return repository;
    }

    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');

    /**
     * Already looping the longs here, lets shoehorn in making a hash
     */
    private long processName(final long[] data, final long start, final long limit, final MeasurementRepository measurementRepository) {
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
                return processNumber(start, i + index, hash, data, measurementRepository);
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
                return processNumber(start, i, hash, data, measurementRepository);
            }
            partialWord = partialWord | ((long) read << len);
            len += 8;
        }
        return limit;
    }

    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    /**
     * Awesome branchless parser by merykitty.
     */
    private long processNumber(final long startAddress, final long delimiterAddress, final int hash, final long[] data,
                               final MeasurementRepository measurementRepository) {

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

        // Store this entity:
        measurementRepository.update(startAddress, data, (int) (delimiterAddress - startAddress), hash, measurement);

        // Return the next address:
        return delimiterAddress + (decimalSepPos >> 3) + 4;
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
     * Extremely simple linear probing hashmap that should work well enough.
     */
    static class MeasurementRepository {
        private static final int TABLE_SIZE = 1 << 18; // large enough for the contest.
        private static final int TABLE_MASK = (TABLE_SIZE - 1);

        private final Entry[] table = new Entry[TABLE_SIZE];
        /**
         * Separated hashtable, keeps memory local, idea of from franz1981.
         * And colocate measurements in Entry:
         */
        private final int[] hashTable = new int[TABLE_SIZE];

        static final class Entry {
            private final long[] data;
            private final String city;
            private int min, max, count;
            private long sum;

            Entry(long[] data, String city) {
                this.data = data;
                this.city = city;
                this.min = 1000;
                this.max = -1000;
            }

            public void updateWith(int measurement) {
                min = min(min, measurement);
                max = max(max, measurement);
                sum += measurement;
                count++;
            }

            public Entry mergeWith(Entry entry) {
                min = min(min, entry.min);
                max = max(max, entry.max);
                sum += entry.sum;
                count += entry.count;
                return this;
            }

            public String toString() {
                return round(min) + "/" + round((1.0 * sum) / count) + "/" + round(max);
            }

            private static double round(double value) {
                return Math.round(value) / 10.0;
            }
        }

        public void update(final long address, final long[] data, final int length, final int hash, final int temperature) {

            final int dataLength = (length >> 3) + 1;

            int index = hash & TABLE_MASK;
            Entry tableEntry = null;

            // Find the entry:
            while (true) {
                int hashTableEntry;
                if ((hashTableEntry = hashTable[index]) == 0) {
                    // Slot is empty
                    break;
                }
                else if (hashTableEntry == hash) {
                    tableEntry = table[index];
                    // Match the entire long[] with all name-bytes:
                    if (Arrays.mismatch(tableEntry.data, 0, dataLength, data, 0, dataLength) < 0) {
                        // Found a matching entry
                        break;
                    }
                }
                // Move to the next index
                index = (index + 1) & TABLE_MASK;
            }

            if (tableEntry == null) {
                tableEntry = createNewEntry(address, data, length, hash, dataLength, index);
            }

            tableEntry.updateWith(temperature);
        }

        private Entry createNewEntry(final long address, final long[] data, final int length, final int hash, final int dataLength, final int index) {

            // --- This is a brand new entry, insert into the hashtable and do the extra calculations (once!) do slower calculations here.
            final byte[] bytes = new byte[length];
            UNSAFE.copyMemory(null, address, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
            final String city = new String(bytes, StandardCharsets.UTF_8);

            final long[] dataCopy = new long[dataLength];
            System.arraycopy(data, 0, dataCopy, 0, dataLength);

            // Add the entry:
            final Entry tableEntry = new Entry(dataCopy, city);
            table[index] = tableEntry;
            hashTable[index] = hash;
            return tableEntry;
        }
    }

}
