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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
 * Improved layout/predictability:   1400 ms
 * Delayed String creation again:    1350 ms
 *
 * Big thanks to Francesco Nigro, Thomas Wuerthinger, Quan Anh Mai for ideas.
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

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

        // Calculate input segments.
        final int numberOfChunks = Runtime.getRuntime().availableProcessors();
        final long[] chunks = getSegments(numberOfChunks);

        final List<Entry[]> repositories = IntStream.range(0, chunks.length - 1)
                .mapToObj(chunkIndex -> processMemoryArea(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .parallel()
                .toList();

        // Sometimes simple is better:
        final HashMap<String, Entry> measurements = HashMap.newHashMap(1 << 10);
        for (Entry[] entries : repositories) {
            for (Entry entry : entries) {
                if (entry != null)
                    measurements.merge(extractedCityFromLongArray(entry.data, entry.length), entry, Entry::mergeWith);
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

    private static final int TABLE_SIZE = 1 << 19; // large enough for the contest.
    private static final int TABLE_MASK = (TABLE_SIZE - 1);

    static final class Entry {
        private final long[] data;
        private int min, max, count, length;
        private long sum;

        Entry(final long[] data, int length, int temp) {
            this.data = data;
            this.length = length;
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        public void updateWith(int measurement) {
            min = Math.min(min, measurement);
            max = Math.max(max, measurement);
            sum += measurement;
            count++;
        }

        public Entry mergeWith(Entry entry) {
            min = Math.min(min, entry.min);
            max = Math.max(max, entry.max);
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

    /**
     * Delay String creation until the end:
     * @param data
     * @param length
     * @return
     */
    private static String extractedCityFromLongArray(final long[] data, final int length) {
        // Initiate as late as possible:
        final byte[] bytes = new byte[length];
        UNSAFE.copyMemory(data, Unsafe.ARRAY_LONG_BASE_OFFSET, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Entry createNewEntry(final long[] buffer, final int lengthLongs, final int lengthBytes, final int temp) {

        final long[] bufferCopy = new long[lengthLongs];
        System.arraycopy(buffer, 0, bufferCopy, 0, lengthLongs);

        // Add the entry:
        return new Entry(bufferCopy, lengthBytes, temp);
    }

    private static Entry[] processMemoryArea(final long fromAddress, final long toAddress) {

        final Entry[] table = new Entry[TABLE_SIZE];
        final long[] buffer = new long[16];

        long ptr = fromAddress;
        int bufferPtr;
        long hash;
        long word;
        long mask;

        while (ptr < toAddress) {

            final long startAddress = ptr;

            bufferPtr = 0;
            hash = 1;
            word = UNSAFE.getLong(ptr);
            mask = getDelimiterMask(word);

            while (mask == 0) {
                buffer[bufferPtr++] = word;
                hash ^= word;
                ptr += 8;

                word = UNSAFE.getLong(ptr);
                mask = getDelimiterMask(word);
            }
            // Found delimiter:
            final long delimiterAddress = ptr + (Long.numberOfTrailingZeros(mask) >> 3);
            final long numberBits = UNSAFE.getLong(delimiterAddress + 1);

            // Finish the masks and hash:
            word = word & ((mask >> 7) - 1);
            buffer[bufferPtr++] = word;
            hash ^= word;

            final long invNumberBits = ~numberBits;
            final int decimalSepPos = Long.numberOfTrailingZeros(invNumberBits & DOT_BITS);

            // Update counter asap, lets CPU predict.
            ptr = delimiterAddress + (decimalSepPos >> 3) + 4;

            // Awesome idea of merykitty:
            final int temp = extractTemp(numberBits, invNumberBits, decimalSepPos);

            int intHash = (int) (hash ^ (hash >>> 33)); // offset for extra entropy
            int index = intHash & TABLE_MASK;

            // Find or insert the entry:
            while (true) {
                Entry tableEntry = table[index];
                if (tableEntry == null) {
                    final int length = (int) (delimiterAddress - startAddress);
                    table[index] = createNewEntry(buffer, bufferPtr, length, temp);
                    break;
                }
                else if (bufferPtr == tableEntry.data.length) {
                    if (!arrayEquals(buffer, tableEntry.data, bufferPtr)) {
                        index = (index + 1) & TABLE_MASK;
                        continue;
                    }
                    // No differences in array
                    tableEntry.updateWith(temp);
                    break;
                }
                // Move to the next index
                index = (index + 1) & TABLE_MASK;
            }
        }
        return table;
    }

    private static int extractTemp(final long numberBits, final long invNumberBits, final int decimalSepPos) {
        final long signed = (invNumberBits << 59) >> 63;
        final long minusFilter = ~(signed & 0xFF);
        final long digits = ((numberBits & minusFilter) << (28 - decimalSepPos)) & 0x0F000F0F00L;
        final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF; // filter just the result
        final int temp = (int) ((absValue + signed) ^ signed); // non-patented method of doing the same trick
        return temp;
    }

    private static long getDelimiterMask(final long word) {
        long match = word ^ SEPARATOR_PATTERN;
        return (match - 0x0101010101010101L) & ~match & 0x8080808080808080L;
    }

    private static final long SEPARATOR_PATTERN = 0x3B3B3B3B3B3B3B3BL;
    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    /**
     * For case multiple hashes are equal (however unlikely) check the actual key (using longs)
     */
    static boolean arrayEquals(final long[] a, final long[] b, final int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }
}
