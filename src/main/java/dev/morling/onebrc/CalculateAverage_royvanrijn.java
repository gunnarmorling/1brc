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
 * Remove writing to buffer:         1335 ms
 * Optimized collecting at the end:  1310 ms
 * Adding a lot of comments:         priceless
 *
 * Big thanks to Francesco Nigro, Thomas Wuerthinger, Quan Anh Mai for ideas.
 *
 * Follow me at: @royvanrijn
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

        final Map<String, Entry> measurements = HashMap.newHashMap(1 << 10);
        IntStream.range(0, chunks.length - 1)
                .mapToObj(chunkIndex -> processMemoryArea(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .parallel()
                .forEachOrdered(repo -> { // make sure it's ordered, no concurrent map
                    for (Entry entry : repo) {
                        if (entry != null)
                            measurements.merge(turnLongArrayIntoString(entry.data, entry.length), entry, Entry::mergeWith);
                    }
                });

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

    // This is where I store the hashtable entry data in the "hot loop"
    // The long[] contains the name in bytes (yeah, confusing)
    // I've tried flyweight-ing, carrying all the data in a single byte[],
    // where you offset type-indices: min:int,max:int,count:int,etc.
    //
    // The performance was just a little worse than this simple class.
    static final class Entry {

        private int min, max, count;
        private byte length;
        private long sum;
        private final long[] data;

        Entry(final long[] data, byte length, int temp) {
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

    // Only parse the String at the final end, when we have only the needed entries left that we need to output:
    private static String turnLongArrayIntoString(final long[] data, final int length) {
        // Create our target byte[]
        final byte[] bytes = new byte[length];
        // The power of magic allows us to just copy the memory in there.
        UNSAFE.copyMemory(data, Unsafe.ARRAY_LONG_BASE_OFFSET, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
        // And construct a String()
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Entry createNewEntry(final long fromAddress, final int lengthLongs, final byte lengthBytes, final int temp) {
        // Make a copy of our working buffer, store this in a new Entry:
        final long[] bufferCopy = new long[lengthLongs];
        // Just copy everything over, bytes into the long[]
        UNSAFE.copyMemory(null, fromAddress, bufferCopy, Unsafe.ARRAY_BYTE_BASE_OFFSET, lengthBytes);
        return new Entry(bufferCopy, lengthBytes, temp);
    }

    private static final int TABLE_SIZE = 1 << 19;
    private static final int TABLE_MASK = (TABLE_SIZE - 1);

    private static Entry[] processMemoryArea(final long fromAddress, final long toAddress) {

        int packedBytes;
        long hash;
        long ptr = fromAddress;
        long word;
        long mask;

        final Entry[] table = new Entry[TABLE_SIZE];

        // Go from start to finish address through the bytes:
        while (ptr < toAddress) {

            final long startAddress = ptr;

            packedBytes = 1;
            hash = 0;
            word = UNSAFE.getLong(ptr);
            mask = getDelimiterMask(word);

            // Removed writing to a buffer here, why would we, we know the address and we'll need to check there anyway.
            while (mask == 0) {
                // If the mask is zero, we have no ';'
                packedBytes++;
                // So we continue building the hash:
                hash ^= word;
                ptr += 8;

                // And getting a new value and mask:
                word = UNSAFE.getLong(ptr);
                mask = getDelimiterMask(word);
            }

            // Found delimiter:
            final int delimiterByte = Long.numberOfTrailingZeros(mask);
            final long delimiterAddress = ptr + (delimiterByte >> 3);

            // Finish the masks and hash:
            final long partialWord = word & ((mask >>> 7) - 1);
            hash ^= partialWord;

            // Read a long value from memory starting from the delimiter + 1, the number part:
            final long numberBytes = UNSAFE.getLong(delimiterAddress + 1);
            final long invNumberBytes = ~numberBytes;

            // Adjust our pointer
            final int decimalSepPos = Long.numberOfTrailingZeros(invNumberBytes & DOT_BITS);
            ptr = delimiterAddress + (decimalSepPos >> 3) + 4;

            // Calculate the final hash and index of the table:
            int intHash = (int) (hash ^ (hash >> 32));
            intHash = intHash ^ (intHash >> 17);
            int index = intHash & TABLE_MASK;

            // Find or insert the entry:
            while (true) {
                Entry tableEntry = table[index];
                if (tableEntry == null) {
                    final int temp = extractTemp(decimalSepPos, invNumberBytes, numberBytes);
                    // Create a new entry:
                    final byte length = (byte) (delimiterAddress - startAddress);
                    table[index] = createNewEntry(startAddress, packedBytes, length, temp);
                    break;
                }
                // Don't bother re-checking things here like hash or length.
                // we'll need to check the content anyway if it's a hit, which is most times
                else if (memoryEqualsEntry(startAddress, tableEntry.data, partialWord, packedBytes)) {
                    // temperature, you're not temporary my friend
                    final int temp = extractTemp(decimalSepPos, invNumberBytes, numberBytes);
                    // No differences, same entry:
                    tableEntry.updateWith(temp);
                    break;
                }
                // Move to the next in the table, linear probing:
                index = (index + 1) & TABLE_MASK;
            }
        }
        return table;
    }

    /*
     * `___` ___ ___ _ ___` ` ___ ` _ ` _ ` _` ___
     * / ` \| _ \ __| \| \ \ / /_\ | | | | | | __|
     * | () | _ / __|| . |\ V / _ \| |_| |_| | ._|
     * \___/|_| |___|_|\_| \_/_/ \_\___|\___/|___|
     * ---------------- BETTER SOFTWARE, FASTER --
     *
     * https://www.openvalue.eu/
     *
     * Made you look.
     *
     */

    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    private static int extractTemp(final int decimalSepPos, final long invNumberBits, final long numberBits) {
        // Awesome idea of merykitty:
        int min28 = (28 - decimalSepPos);
        // Calculates the sign
        final long signed = (invNumberBits << 59) >> 63;
        final long minusFilter = ~(signed & 0xFF);
        // Use the pre-calculated decimal position to adjust the values
        final long digits = ((numberBits & minusFilter) << min28) & 0x0F000F0F00L;
        // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
        final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        // And perform abs()
        final int temp = (int) ((absValue + signed) ^ signed); // non-patented method of doing the same trick
        return temp;
    }

    private static final long SEPARATOR_PATTERN = 0x3B3B3B3B3B3B3B3BL;

    // Takes a long and finds the bytes where this exact pattern is present.
    // Cool bit manipulation technique: SWAR (SIMD as a Register).
    private static long getDelimiterMask(final long word) {
        final long match = word ^ SEPARATOR_PATTERN;
        return (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);
        // I've put some brackets separating the first and second part, this is faster.
        // Now they run simultaneous after 'match' is altered, instead of waiting on each other.
    }

    /**
     * For case multiple hashes are equal (however unlikely) check the actual key (using longs)
     */
    private static boolean memoryEqualsEntry(final long startAddress, final long[] entry, final long finalBytes, final int amountLong) {
        for (int i = 0; i < (amountLong - 1); i++) {
            int step = i << 3; // step by 8 bytes
            if (UNSAFE.getLong(startAddress + step) != entry[i])
                return false;
        }
        // If all previous 'whole' 8-packed byte-long values are equal
        // We still need to check the final bytes that don't fit.
        // and we've already calculated them for the hash.
        return finalBytes == entry[amountLong - 1];
    }
}
