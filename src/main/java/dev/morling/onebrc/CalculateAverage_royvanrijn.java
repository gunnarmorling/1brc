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

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
 * Changed to flyweight byte[]:      1290 ms (adds even more Unsafe, was initially slower, now faster)
 * More LOC now parallel:            1260 ms (moved more to processMemoryArea, recombining in ConcurrentHashMap)
 * Storing only the address:         1240 ms (this is now faster, tried before, was slower)
 * Unrolling scan-loop:              1200 ms (seems to help, perhaps even more on target machine)
 *
 * I've tried making a version that scans in both directions; one starts at the beginning of a segment, the other at the end and processes backwards.
 * This has one big advantage: you can update the pivot if one thread is faster than the other, you'll get "free" work-sharing.
 * But I couldn't get the backwards logic to work reliably, so I ditched the idea for now.
 *
 * Big thanks to Francesco Nigro, Thomas Wuerthinger, Quan Anh Mai and many others for ideas.
 *
 * Follow me at: @royvanrijn
 */
public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "src/test/resources/samples/measurements-1.txt";

    private static final Unsafe UNSAFE = initUnsafe();

    // Twice the processors, smoothens things out.
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

    /**
     * Flyweight entry in a byte[], max 128 bytes.
     *
     * long: sum
     * int:  min
     * int:  max
     * int:  count
     * byte: length
     * byte[]: cityname
     */
    // ------------------------------------------------------------------------
    private static final int ENTRY_LENGTH = (Unsafe.ARRAY_BYTE_BASE_OFFSET);
    private static final int ENTRY_SUM = (ENTRY_LENGTH + Byte.BYTES);
    private static final int ENTRY_MIN = (ENTRY_SUM + Long.BYTES);
    private static final int ENTRY_MAX = (ENTRY_MIN + Integer.BYTES);
    private static final int ENTRY_COUNT = (ENTRY_MAX + Integer.BYTES);
    private static final int ENTRY_NAME = (ENTRY_COUNT + Integer.BYTES);
    private static final int ENTRY_BASESIZE_WHITESPACE = ENTRY_NAME + 7; // with enough empty bytes to fill a long
    // ------------------------------------------------------------------------
    private static final int PREMADE_MAX_SIZE = 1 << 5; // pre-initialize some entries in memory, keep them close
    private static final int PREMADE_ENTRIES = 512; // amount of pre-created entries we should use
    private static final int TABLE_SIZE = 1 << 19; // large enough for the contest.
    private static final int TABLE_MASK = (TABLE_SIZE - 1);

    public static void main(String[] args) throws Exception {

        // Calculate input segments.
        final FileChannel fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
        final long fileSize = fileChannel.size();
        final long segmentSize = (fileSize + PROCESSORS - 1) / PROCESSORS;
        final long mapAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();

        final Thread[] parallelThreads = new Thread[PROCESSORS - 1];

        // This is where the entries will land:
        final ConcurrentHashMap<String, byte[]> measurements = new ConcurrentHashMap(1 << 10);

        // We create separate threads for twice the amount of processors.
        long lastAddress = mapAddress;
        final long endOfFile = mapAddress + fileSize;
        for (int i = 0; i < PROCESSORS - 1; ++i) {

            final long fromAddress = lastAddress;
            final long toAddress = Math.min(endOfFile, fromAddress + segmentSize);

            final Thread thread = new Thread(() -> {
                // The actual work is done here:
                final byte[][] table = processMemoryArea(fromAddress, toAddress, fromAddress == mapAddress);

                for (byte[] entry : table) {
                    if (entry != null) {
                        measurements.merge(entryToName(entry), entry, CalculateAverage_royvanrijn::mergeEntry);
                    }
                }
            });
            thread.start(); // start a.s.a.p.
            parallelThreads[i] = thread;
            lastAddress = toAddress;
        }

        // Use the current thread for the part of memory:
        final byte[][] table = processMemoryArea(lastAddress, mapAddress + fileSize, false);

        for (byte[] entry : table) {
            if (entry != null) {
                measurements.merge(entryToName(entry), entry, CalculateAverage_royvanrijn::mergeEntry);
            }
        }
        // Wait for all threads to finish:
        for (Thread thread : parallelThreads) {
            // Can we implement work-stealing? Not sure how...
            thread.join();
        }

        // If we don't reach start of file,
        System.out.print("{" +
                measurements.entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .map(entry -> entry.getKey() + '=' + entryValuesToString(entry.getValue()))
                        .collect(Collectors.joining(", ")));
        System.out.println("}");

        // System.out.println(measurements.entrySet().stream().mapToLong(e -> UNSAFE.getInt(e.getValue(), ENTRY_COUNT + Unsafe.ARRAY_BYTE_BASE_OFFSET)).sum());
    }

    private static byte[] fillEntry(final byte[] entry, final long fromAddress, final int length, final int temp) {
        UNSAFE.putLong(entry, ENTRY_SUM, temp);
        UNSAFE.putInt(entry, ENTRY_MIN, temp);
        UNSAFE.putInt(entry, ENTRY_MAX, temp);
        UNSAFE.putInt(entry, ENTRY_COUNT, 1);
        UNSAFE.putByte(entry, ENTRY_LENGTH, (byte) length);
        UNSAFE.copyMemory(null, fromAddress, entry, ENTRY_NAME, length);
        return entry;
    }

    public static void updateExistingEntry(final byte[] entry, final int temp) {

        int entryMin = UNSAFE.getInt(entry, ENTRY_MIN);
        int entryMax = UNSAFE.getInt(entry, ENTRY_MAX);
        entryMin = Math.min(temp, entryMin);
        entryMax = Math.max(temp, entryMax);

        long entrySum = UNSAFE.getLong(entry, ENTRY_SUM) + temp;
        int entryCount = UNSAFE.getInt(entry, ENTRY_COUNT) + 1;

        UNSAFE.putInt(entry, ENTRY_MIN, entryMin);
        UNSAFE.putInt(entry, ENTRY_MAX, entryMax);
        UNSAFE.putInt(entry, ENTRY_COUNT, entryCount);
        UNSAFE.putLong(entry, ENTRY_SUM, entrySum);
    }

    public static byte[] mergeEntry(final byte[] entry, final byte[] merge) {

        long sum = UNSAFE.getLong(merge, ENTRY_SUM);
        final int mergeMin = UNSAFE.getInt(merge, ENTRY_MIN);
        final int mergeMax = UNSAFE.getInt(merge, ENTRY_MAX);
        int count = UNSAFE.getInt(merge, ENTRY_COUNT);

        sum += UNSAFE.getLong(entry, ENTRY_SUM);
        int entryMin = UNSAFE.getInt(entry, ENTRY_MIN);
        int entryMax = UNSAFE.getInt(entry, ENTRY_MAX);
        count += UNSAFE.getInt(entry, ENTRY_COUNT);

        entryMin = Math.min(entryMin, mergeMin);
        entryMax = Math.max(entryMax, mergeMax);

        UNSAFE.putLong(entry, ENTRY_SUM, sum);
        UNSAFE.putInt(entry, ENTRY_MIN, entryMin);
        UNSAFE.putInt(entry, ENTRY_MAX, entryMax);
        UNSAFE.putInt(entry, ENTRY_COUNT, count);
        return entry;
    }

    private static String entryToName(final byte[] entry) {
        // Get the length from memory:
        int length = UNSAFE.getByte(entry, ENTRY_LENGTH);

        byte[] name = new byte[length];
        UNSAFE.copyMemory(entry, ENTRY_NAME, name, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);

        // Create a new String with the existing byte[]:
        return new String(name, StandardCharsets.UTF_8);
    }

    private static String entryValuesToString(final byte[] entry) {
        return round(UNSAFE.getInt(entry, ENTRY_MIN))
                + "/" +
                round((1.0 * UNSAFE.getLong(entry, ENTRY_SUM)) /
                        UNSAFE.getInt(entry, ENTRY_COUNT))
                + "/" +
                round(UNSAFE.getInt(entry, ENTRY_MAX));
    }

    // Print a piece of memory:
    // For debug.
    private static String printMemory(final Object target, final long address, int length) {
        String result = "";
        for (int i = 0; i < length; i++) {
            result += (char) UNSAFE.getByte(target, address + i);
        }
        return result;
    }

    private static double round(final double value) {
        return Math.round(value) / 10.0;
    }

    private static byte[][] processMemoryArea(final long startAddress, final long endAddress, boolean hasFileStart) {

        final byte[][] table = new byte[TABLE_SIZE][];
        final byte[][] preConstructedEntries = new byte[PREMADE_ENTRIES][ENTRY_BASESIZE_WHITESPACE + PREMADE_MAX_SIZE];

        byte[] entry;
        int entryCount = 0;
        int index;
        int additionalLongs;
        int dotPosition;
        int temperature;
        long numberBytes;
        long invNumberBytes;
        long hash;
        long word;
        long mask;
        long ptr;
        long delimiterAddress;
        long partialWord;
        long rowStartAddress;

        // Find the correct starting position
        ptr = startAddress;
        if (!hasFileStart) {
            ptr--;
            while (ptr < endAddress) {
                if (UNSAFE.getByte(ptr++) == '\n') {
                    break;
                }
            }
            if (ptr >= endAddress) {
                // Early escape for these small testcases
                return table;
            }
        }

        while (ptr < endAddress) {

            rowStartAddress = ptr;

            additionalLongs = 0;
            hash = 0;

            word = UNSAFE.getLong(ptr);
            mask = getMaskedByte(word, DELIMITER_MASK);

            while (mask == 0) {
                hash ^= word;
                ptr += 8;

                word = UNSAFE.getLong(ptr);
                mask = getMaskedByte(word, DELIMITER_MASK);

                additionalLongs++;
            }

            // Found delimiter:
            delimiterAddress = ptr + (Long.numberOfTrailingZeros(mask) >> 3);

            // Finish the masks and hash:
            partialWord = word & ((mask >>> 7) - 1);
            hash ^= partialWord;

            // This is the number part: X.X, -X.X, XX.x or -XX.X
            numberBytes = UNSAFE.getLong(delimiterAddress + 1);
            invNumberBytes = ~numberBytes;

            hash ^= hash >> 32;
            hash ^= hash >> 17;
            index = (int) (hash & TABLE_MASK);

            // Adjust our pointer
            dotPosition = Long.numberOfTrailingZeros(invNumberBytes & DOT_BITS);

            // Read the temperature bytes and process this in a single go:
            temperature = extractTemp(dotPosition, invNumberBytes, numberBytes);

            // Find or insert the entry:
            while (true) {
                entry = table[index];
                if (entry == null) {
                    byte length = (byte) (delimiterAddress - rowStartAddress);
                    byte[] entryBytes = (length < PREMADE_MAX_SIZE && entryCount < PREMADE_ENTRIES) ? preConstructedEntries[entryCount++]
                            : new byte[ENTRY_BASESIZE_WHITESPACE + length];
                    table[index] = fillEntry(entryBytes, rowStartAddress, length, temperature);
                    break;
                }
                else if (addressEqualsStored(rowStartAddress, entry, partialWord, additionalLongs)) {
                    updateExistingEntry(entry, temperature);
                    break;
                }
                // Move to the next index
                index = (index + 1) & TABLE_MASK;
            }
            ptr = delimiterAddress + (dotPosition >> 3) + 4;

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
        long digits = ((numberBits & minusFilter) << min28) & 0x0F000F0F00L;
        // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
        final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
        // And perform abs()
        return (int) ((absValue + signed) ^ signed); // non-patented method of doing the same trick
    }

    private static final long DELIMITER_MASK = 0x3B3B3B3B3B3B3B3BL;

    // Takes a long and finds the bytes where this exact pattern is present.
    // Cool bit manipulation technique: SWAR (SIMD as a Register).
    private static long getMaskedByte(final long word, final long separator) {
        final long match = word ^ separator;
        return (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);
        // I've put some brackets separating the first and second part, this is faster.
        // Now they run simultaneous after 'match' is altered, instead of waiting on each other.
    }

    /**
     * For case multiple hashes are equal (however unlikely) check the actual key (using longs)
     */
    static boolean addressEqualsStored(final long startAddress, final byte[] entry, final long partialWord, final long additionalLongs) {
        for (int i = 0; i < additionalLongs; i++) {
            int step = i << 3;
            if (UNSAFE.getLong(startAddress + step) != UNSAFE.getLong(entry, ENTRY_NAME + step))
                return false;
        }
        return partialWord == UNSAFE.getLong(entry, ENTRY_NAME + (additionalLongs << 3));
    }

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
}
