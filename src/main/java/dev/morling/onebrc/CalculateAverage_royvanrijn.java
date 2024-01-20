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
 * Adding more readable reader:      1300 ms (scores got worse on target machine anyway)
 *
 * I've ditched my M2 for an older x86-64 MacBook, this allows me to run `perf` and I'm trying to get lower numbers by trail and error.
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
    private static final int ENTRY_NAME_8 = ENTRY_NAME + 8;
    private static final int ENTRY_NAME_16 = ENTRY_NAME + 16;

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

    public static void updateEntry(final byte[] entry, final int temp) {

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

    // Print a piece of memory:
    // For debug.
    private static String printMemory(final long value, int length) {
        String result = "";
        for (int i = 0; i < length; i++) {
            result += (char) ((value >> (i << 3)) & 0xFF);
        }
        return result;
    }

    private static double round(final double value) {
        return Math.round(value) / 10.0;
    }

    private static final class Reader {

        private long ptr;
        private long delimiterMask;
        private long lastRead;
        private long lastReadMinOne;

        private long hash;
        private long entryStart;
        private long entryDelimiter;

        private final long endAddress;

        Reader(final long startAddress, final long endAddress, final boolean isFileStart) {

            this.ptr = startAddress;
            this.endAddress = endAddress;

            // Adjust start to next delimiter:
            if (!isFileStart) {
                ptr--;
                while (ptr < endAddress) {
                    if (UNSAFE.getByte(ptr++) == '\n') {
                        break;
                    }
                }
            }
        }

        private void processStart() {
            hash = 0;
            entryStart = ptr;
        }

        private boolean hasNext() {
            return (ptr < endAddress);
        }

        private static final long DELIMITER_MASK = 0x3B3B3B3B3B3B3B3BL;

        private boolean readFirst() {
            lastRead = UNSAFE.getLong(ptr);

            final long match = lastRead ^ DELIMITER_MASK;
            delimiterMask = (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);

            return delimiterMask == 0;
        }

        private boolean readNext() {
            lastReadMinOne = lastRead;
            return readFirst();
        }

        private void processName() {
            hash ^= lastRead;
            ptr += 8;
        }

        private int processEndAndGetTemperature() {
            processFinalBytes();

            finalizeHash();
            finalizeDelimiter();

            return readTemperature();
        }

        private void processFinalBytes() {
            // Shift and read the last bytes:
            lastRead &= ((delimiterMask >>> 7) - 1);
        }

        private void finalizeHash() {
            // Finalize hash:
            hash ^= lastRead;
            hash ^= hash >> 32;
            hash ^= hash >> 17; // extra entropy
        }

        private void finalizeDelimiter() {
            // Found delimiter:
            entryDelimiter = ptr + (Long.numberOfTrailingZeros(delimiterMask) >> 3);
        }

        private static final long DOT_BITS = 0x10101000;
        private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

        // Awesome idea of merykitty:
        private int readTemperature() {
            // This is the number part: X.X, -X.X, XX.x or -XX.X
            long numberBytes = UNSAFE.getLong(entryDelimiter + 1);
            long invNumberBytes = ~numberBytes;

            int dotPosition = Long.numberOfTrailingZeros(invNumberBytes & DOT_BITS);

            // Update the pointer here, bit awkward, but we have all the data
            ptr = entryDelimiter + (dotPosition >> 3) + 4;

            int min28 = (28 - dotPosition);
            // Calculates the sign
            final long signed = (invNumberBytes << 59) >> 63;
            final long minusFilter = ~(signed & 0xFF);
            // Use the pre-calculated decimal position to adjust the values
            long digits = ((numberBytes & minusFilter) << min28) & 0x0F000F0F00L;
            // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
            final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            // And perform abs()
            return (int) ((absValue + signed) ^ signed); // non-patented method of doing the same trick
        }

        private boolean matchesEntryFull(final byte[] entry) {
            int longs = (int) (entryDelimiter - entryStart) >> 3;
            int step = 0;
            for (int i = 0; i < longs - 2; i++) {
                if (UNSAFE.getLong(entryStart + step) != UNSAFE.getLong(entry, ENTRY_NAME + step)) {
                    return false;
                }
                step += 8;
            }
            if (lastReadMinOne != UNSAFE.getLong(entry, (ENTRY_NAME_8) + step)) {
                return false;
            }
            if (lastRead != UNSAFE.getLong(entry, (ENTRY_NAME_16) + step)) {
                return false;
            }
            return true;

        }

        private boolean matchesEntryMedium(final byte[] entry) {
            if (UNSAFE.getLong(entryStart) != UNSAFE.getLong(entry, ENTRY_NAME)) {
                return false;
            }
            if (lastReadMinOne != UNSAFE.getLong(entry, ENTRY_NAME_8)) {
                return false;
            }
            if (lastRead != UNSAFE.getLong(entry, ENTRY_NAME_16)) {
                return false;
            }
            return true;
        }

        private boolean matchesEntryShort(final byte[] entry) {
            if (lastReadMinOne != UNSAFE.getLong(entry, ENTRY_NAME)) {
                return false;
            }
            if (lastRead != UNSAFE.getLong(entry, ENTRY_NAME_8)) {
                return false;
            }
            return true;
        }

        private boolean matchesEnding(final byte[] entry) {
            return lastRead == UNSAFE.getLong(entry, ENTRY_NAME);
        }

        private int length() {
            return (int) (entryDelimiter - entryStart);

        }

    }

    private static byte[][] processMemoryArea(final long startAddress, final long endAddress, boolean isFileStart) {

        final byte[][] table = new byte[TABLE_SIZE][];
        final byte[][] preConstructedEntries = new byte[PREMADE_ENTRIES][ENTRY_BASESIZE_WHITESPACE + PREMADE_MAX_SIZE];

        final Reader reader = new Reader(startAddress, endAddress, isFileStart);

        byte[] entry;
        int entryCount = 0;

        // Find the correct starting position
        while (reader.hasNext()) {

            reader.processStart();

            if (!reader.readFirst()) {
                int temperature = reader.processEndAndGetTemperature();

                // Find or insert the entry:
                int index = (int) (reader.hash & TABLE_MASK);
                while (true) {
                    entry = table[index];
                    if (entry == null) {
                        int length = reader.length();
                        byte[] entryBytes = (entryCount < PREMADE_ENTRIES) ? preConstructedEntries[entryCount++]
                                : new byte[ENTRY_BASESIZE_WHITESPACE + length];
                        table[index] = fillEntry(entryBytes, reader.entryStart, length, temperature);
                        break;
                    }
                    else if (reader.matchesEnding(entry)) {
                        updateEntry(entry, temperature);
                        break;
                    }
                    else {
                        // Move to the next index
                        index = (index + 1) & TABLE_MASK;
                    }
                }
            }
            else {
                reader.processName();

                if (!reader.readNext()) {

                    int temperature = reader.processEndAndGetTemperature();

                    // Find or insert the entry:
                    int index = (int) (reader.hash & TABLE_MASK);
                    while (true) {
                        entry = table[index];
                        if (entry == null) {
                            int length = reader.length();
                            byte[] entryBytes = (entryCount < PREMADE_ENTRIES) ? preConstructedEntries[entryCount++]
                                    : new byte[ENTRY_BASESIZE_WHITESPACE + length];
                            table[index] = fillEntry(entryBytes, reader.entryStart, length, temperature);
                            break;
                        }
                        else if (reader.matchesEntryShort(entry)) {
                            updateEntry(entry, temperature);
                            break;
                        }
                        else {
                            // Move to the next index
                            index = (index + 1) & TABLE_MASK;
                        }
                    }
                }
                else {
                    reader.processName();

                    if (!reader.readNext()) {
                        int temperature = reader.processEndAndGetTemperature();

                        // Find or insert the entry:
                        int index = (int) (reader.hash & TABLE_MASK);
                        while (true) {
                            entry = table[index];
                            if (entry == null) {
                                int length = reader.length();
                                byte[] entryBytes = (entryCount < PREMADE_ENTRIES) ? preConstructedEntries[entryCount++]
                                        : new byte[ENTRY_BASESIZE_WHITESPACE + length];
                                table[index] = fillEntry(entryBytes, reader.entryStart, length, temperature);
                                break;
                            }
                            else if (reader.matchesEntryMedium(entry)) {
                                updateEntry(entry, temperature);
                                break;
                            }
                            else {
                                // Move to the next index
                                index = (index + 1) & TABLE_MASK;
                            }
                        }

                    }
                    else {

                        reader.processName();
                        while (reader.readNext()) {
                            reader.processName();
                        }

                        int temperature = reader.processEndAndGetTemperature();

                        // Find or insert the entry:
                        int index = (int) (reader.hash & TABLE_MASK);
                        while (true) {
                            entry = table[index];
                            if (entry == null) {
                                int length = reader.length();
                                byte[] entryBytes = (length < PREMADE_MAX_SIZE && entryCount < PREMADE_ENTRIES) ? preConstructedEntries[entryCount++]
                                        : new byte[ENTRY_BASESIZE_WHITESPACE + length]; // with enough room
                                table[index] = fillEntry(entryBytes, reader.entryStart, length, temperature);
                                break;
                            }
                            else if (reader.matchesEntryFull(entry)) {
                                updateEntry(entry, temperature);
                                break;
                            }
                            else {
                                // Move to the next index
                                index = (index + 1) & TABLE_MASK;
                            }
                        }
                    }
                }
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
     */

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
