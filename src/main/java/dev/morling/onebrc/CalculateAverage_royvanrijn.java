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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Using old x86 MacBook and perf:   3500 ms (different machine for testing)
 * Decided to rewrite loop for 16 b: 3050 ms
 * Small changes, limited heap:      2950 ms
 *
 * I have some instructions that could be removed, but faster with...
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
     * <p>
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
    private static final int SEGMENT_SIZE = 1024 * 1024 * 2;
    // ------------------------------------------------------------------------

    // Idea of thomaswue, don't wait for slow unmap:
    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder()
                .command(workerCommand)
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    public static void main(String[] args) throws Exception {

        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }

        // Calculate input segments.
        final FileChannel fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
        final long fileSize = fileChannel.size();
        final long mapAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();

        final Thread[] parallelThreads = new Thread[PROCESSORS - 1];

        // This is where the entries will land:
        final ConcurrentHashMap<String, byte[]> measurements = new ConcurrentHashMap(1 << 10);

        AtomicInteger segmentId = new AtomicInteger(0);

        // We create separate threads for twice the amount of processors.
        long lastAddress = mapAddress;
        final long endOfFile = mapAddress + fileSize;
        for (int i = 0; i < PROCESSORS - 1; ++i) {
            final Thread thread = new Thread(() -> {
                // The actual work is done here:
                final byte[][] table = processMemoryArea(segmentId, mapAddress, endOfFile);

                for (byte[] entry : table) {
                    if (entry != null) {
                        measurements.merge(entryToName(entry), entry, CalculateAverage_royvanrijn::mergeEntry);
                    }
                }
            });
            thread.start(); // start a.s.a.p.
            parallelThreads[i] = thread;
        }

        // Use the current thread for the part of memory:
        final byte[][] table = processMemoryArea(segmentId, mapAddress, endOfFile);

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

        System.out.close(); // close the stream to stop
    }

    private static byte[] fillEntry(final byte[] entry, final long fromAddress, final int entryLength, final int temp, final long readBuffer1, final long readBuffer2) {
        UNSAFE.putLong(entry, ENTRY_SUM, temp);
        UNSAFE.putInt(entry, ENTRY_MIN, temp);
        UNSAFE.putInt(entry, ENTRY_MAX, temp);
        UNSAFE.putInt(entry, ENTRY_COUNT, 1);
        UNSAFE.putByte(entry, ENTRY_LENGTH, (byte) entryLength);
        UNSAFE.copyMemory(null, fromAddress, entry, ENTRY_NAME, entryLength - 16);
        UNSAFE.putLong(entry, ENTRY_NAME + entryLength - 16, readBuffer1);
        UNSAFE.putLong(entry, ENTRY_NAME + entryLength - 8, readBuffer2);
        return entry;
    }

    private static byte[] fillEntry16(final byte[] entry, final int entryLength, final int temp, final long readBuffer1, final long readBuffer2) {
        UNSAFE.putLong(entry, ENTRY_SUM, temp);
        UNSAFE.putInt(entry, ENTRY_MIN, temp);
        UNSAFE.putInt(entry, ENTRY_MAX, temp);
        UNSAFE.putInt(entry, ENTRY_COUNT, 1);
        UNSAFE.putByte(entry, ENTRY_LENGTH, (byte) entryLength);
        UNSAFE.putLong(entry, ENTRY_NAME + entryLength - 16, readBuffer1);
        UNSAFE.putLong(entry, ENTRY_NAME + entryLength - 8, readBuffer2);
        return entry;
    }

    private static byte[] fillEntry8(final byte[] entry, final int entryLength, final int temp, final long readBuffer1) {
        UNSAFE.putLong(entry, ENTRY_SUM, temp);
        UNSAFE.putInt(entry, ENTRY_MIN, temp);
        UNSAFE.putInt(entry, ENTRY_MAX, temp);
        UNSAFE.putInt(entry, ENTRY_COUNT, 1);
        UNSAFE.putByte(entry, ENTRY_LENGTH, (byte) entryLength);
        UNSAFE.putLong(entry, ENTRY_NAME + entryLength - 8, readBuffer1);
        return entry;
    }

    public static void updateEntry(final byte[] entry, final int temp) {

        int entryMin = UNSAFE.getInt(entry, ENTRY_MIN);
        int entryMax = UNSAFE.getInt(entry, ENTRY_MAX);
        long entrySum = UNSAFE.getLong(entry, ENTRY_SUM) + temp;
        int entryCount = UNSAFE.getInt(entry, ENTRY_COUNT) + 1;

        if (temp < entryMin) {
            UNSAFE.putInt(entry, ENTRY_MIN, temp);
        }
        else if (temp > entryMax) {
            UNSAFE.putInt(entry, ENTRY_MAX, temp);
        }
        UNSAFE.putInt(entry, ENTRY_COUNT, entryCount);
        UNSAFE.putLong(entry, ENTRY_SUM, entrySum);
    }

    public static byte[] mergeEntry(final byte[] entry, final byte[] merge) {

        long sum = UNSAFE.getLong(merge, ENTRY_SUM);
        final int mergeMin = UNSAFE.getInt(merge, ENTRY_MIN);
        final int mergeMax = UNSAFE.getInt(merge, ENTRY_MAX);
        int count = UNSAFE.getInt(merge, ENTRY_COUNT);

        sum += UNSAFE.getLong(entry, ENTRY_SUM);
        count += UNSAFE.getInt(entry, ENTRY_COUNT);

        int entryMin = UNSAFE.getInt(entry, ENTRY_MIN);
        int entryMax = UNSAFE.getInt(entry, ENTRY_MAX);
        entryMin = Math.min(entryMin, mergeMin);
        entryMax = Math.max(entryMax, mergeMax);
        UNSAFE.putInt(entry, ENTRY_MIN, entryMin);
        UNSAFE.putInt(entry, ENTRY_MAX, entryMax);

        UNSAFE.putLong(entry, ENTRY_SUM, sum);
        UNSAFE.putInt(entry, ENTRY_COUNT, count);
        return entry;
    }

    private static String entryToName(final byte[] entry) {
        // Get the length from memory:
        int length = UNSAFE.getByte(entry, ENTRY_LENGTH);

        byte[] name = new byte[length];
        UNSAFE.copyMemory(entry, ENTRY_NAME, name, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);

        // Create a new String with the existing byte[]:
        return new String(name, StandardCharsets.UTF_8).trim();
    }

    private static String entryValuesToString(final byte[] entry) {
        return (round(UNSAFE.getInt(entry, ENTRY_MIN))
                + "/" +
                round((1.0 * UNSAFE.getLong(entry, ENTRY_SUM)) /
                        UNSAFE.getInt(entry, ENTRY_COUNT))
                + "/" +
                round(UNSAFE.getInt(entry, ENTRY_MAX)));
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
        private long readBuffer1;
        private long readBuffer2;

        private long hash;
        private long entryStart;
        private int entryLength; // in bytes rounded to nearest 16

        private final long endAddress;
        private boolean finished;

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

        private void reset() {
            hash = 0;
            entryStart = ptr;
            entryLength = 0;
            finished = false;
        }

        private boolean hasNext() {
            return (ptr < endAddress);
        }

        private static final long DELIMITER_MASK = 0x3B3B3B3B3B3B3B3BL;

        private boolean isFinished() {
            return finished;
        }

        private void readNext() {

            entryLength += 16;

            long lastRead = UNSAFE.getLong(ptr);

            // Find delimiter and create mask for first long value"
            final long highBitMask1 = getHighBitMask(lastRead);
            if (highBitMask1 != 0) {
                long mask = ~((highBitMask1 >>> 7) - 1);
                readBuffer1 = lastRead & ~mask;
                readBuffer2 = 0;
                hash ^= readBuffer1;
                ptr += 1 + (Long.numberOfTrailingZeros(highBitMask1) >> 3);
                finished = true;
                return;
            }
            readBuffer1 = lastRead;
            hash ^= readBuffer1;
            ptr += 8;

            lastRead = UNSAFE.getLong(ptr);

            // Repeat for second long value
            final long highBitMask2 = getHighBitMask(lastRead);
            if (highBitMask2 != 0) {
                long mask = ~((highBitMask2 >>> 7) - 1);
                readBuffer2 = lastRead & ~mask;
                hash ^= readBuffer2;
                ptr += 1 + (Long.numberOfTrailingZeros(highBitMask2) >> 3);
                finished = true;
                return;
            }
            // Apply masks
            readBuffer2 = lastRead;
            hash ^= readBuffer2;
            ptr += 8;
        }

        private static long getHighBitMask(final long lastRead) {
            long comparisonResult2 = (lastRead ^ DELIMITER_MASK);
            return (comparisonResult2 - 0x0101010101010101L) & (~comparisonResult2 & 0x8080808080808080L);
        }

        private int processEndAndGetTemperature() {
            finalizeHash();
            return readTemperature();
        }

        private void finalizeHash() {
            hash ^= hash >> 32;
            hash ^= hash >> 17; // extra entropy
        }

        private static final long DOT_BITS = 0x10101000;
        private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

        // Awesome idea of merykitty:
        private int readTemperature() {
            // This is the number part: X.X, -X.X, XX.x or -XX.X
            final long numberBytes = UNSAFE.getLong(ptr);
            final long invNumberBytes = ~numberBytes;

            final int dotPosition = Long.numberOfTrailingZeros(invNumberBytes & DOT_BITS);

            // Calculates the sign
            final long signed = (invNumberBytes << 59) >> 63;
            final int min28 = (dotPosition ^ 0b11100);
            final long minusFilter = ~(signed & 0xFF);
            // Use the pre-calculated decimal position to adjust the values
            final long digits = ((numberBytes & minusFilter) << min28) & 0x0F000F0F00L;

            // Update the pointer here, bit awkward, but we have all the data
            ptr += (dotPosition >> 3) + 3;

            // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
            final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            // And perform abs()
            return (int) ((absValue + signed) ^ signed); // non-patented method of doing the same trick
        }

        private boolean matches(final byte[] entry) {
            int step = 0;
            for (; step < entryLength - 16;) {
                if (compare(null, entryStart + step, entry, ENTRY_NAME + step)) {
                    return false;
                }
                step += 8;
            }
            if (compare(readBuffer1, entry, ENTRY_NAME + step)) {
                return false;
            }
            step += 8;
            if (compare(readBuffer2, entry, ENTRY_NAME + step)) {
                return false;
            }
            return true;
        }

        private boolean matches16(final byte[] entry) {
            if (compare(readBuffer1, entry, ENTRY_NAME)) {
                return false;
            }
            if (compare(readBuffer2, entry, ENTRY_NAME + 8)) {
                return false;
            }
            return true;
        }
    }

    private static byte[][] processMemoryArea(final AtomicInteger segmentCounter, long fileStart, final long fileEnd) {

        final byte[][] table = new byte[TABLE_SIZE][];

        while (true) {
            // Process more local memory:
            long segementId = segmentCounter.getAndIncrement();

            long fromAddress = fileStart + (segementId * SEGMENT_SIZE);
            if (fromAddress >= fileEnd) {
                return table;
            }

            long toAddress = Math.min(fileEnd, fromAddress + SEGMENT_SIZE);

            long aThird = (toAddress - fromAddress) / 3;

            // Unrolled the readers... a bit.
            // Unable to make it as smooth as thomasque/jerrinot haha.
            final Reader reader1 = new Reader(fromAddress, fromAddress + aThird, fromAddress == fileStart);
            final Reader reader2 = new Reader(fromAddress + aThird, toAddress - aThird, false);
            final Reader reader3 = new Reader(toAddress - aThird, toAddress, false);

            // Find the correct starting position
            while (true) {
                if (!reader1.hasNext()) {
                    break;
                }
                if (!reader2.hasNext()) {
                    break;
                }
                if (!reader3.hasNext()) {
                    break;
                }

                reader1.reset();
                reader2.reset();
                reader3.reset();
                reader1.readNext();
                reader2.readNext();
                reader3.readNext();
                readAndStore(reader1, table);
                readAndStore(reader2, table);
                readAndStore(reader3, table);
            }

            while (reader1.hasNext()) {
                reader1.reset();
                reader1.readNext();
                readAndStore(reader1, table);
            }
            while (reader2.hasNext()) {
                reader2.reset();
                reader2.readNext();
                readAndStore(reader2, table);
            }
            while (reader3.hasNext()) {
                reader3.reset();
                reader3.readNext();
                readAndStore(reader3, table);
            }

        }
    }

    private static void readAndStore(final Reader reader, final byte[][] table) {
        if (reader.isFinished()) {
            // First 16 bytes:
            int temperature = reader.processEndAndGetTemperature();
            insert16(reader, table, temperature);
        }
        else {
            do {
                reader.readNext();
            } while (!reader.isFinished());

            int temperature = reader.processEndAndGetTemperature();
            insertFull(reader, table, temperature);
        }
    }

    private static void insertFull(final Reader reader, final byte[][] table, final int temperature) {
        byte[] entry;
        // Find or insert the entry:
        int index = (int) (reader.hash & TABLE_MASK);
        while (true) {
            entry = table[index];
            if (entry == null) {
                int length = reader.entryLength;
                byte[] entryBytes = new byte[ENTRY_BASESIZE_WHITESPACE + length];
                table[index] = fillEntry(entryBytes, reader.entryStart, length, temperature, reader.readBuffer1, reader.readBuffer2);
                return;
            }
            else if (reader.matches(entry)) {
                updateEntry(entry, temperature);
                return;
            }
            else {
                // Move to the next index
                index = (index + 1) & TABLE_MASK;
            }
        }
    }

    private static void insert16(final Reader reader, final byte[][] table, final int temperature) {
        byte[] entry;
        // Find or insert the entry:
        int index = (int) (reader.hash & TABLE_MASK);
        while (true) {
            entry = table[index];
            if (entry == null) {
                byte[] entryBytes = new byte[ENTRY_BASESIZE_WHITESPACE + 16];
                table[index] = fillEntry16(entryBytes, 16, temperature, reader.readBuffer1, reader.readBuffer2);
                return;
            }
            else if (reader.matches16(entry)) {
                updateEntry(entry, temperature);
                return;
            }
            else {
                // Move to the next index
                index = (index + 1) & TABLE_MASK;
            }
        }
    }

    private static boolean compare(final Object object1, final long address1, final Object object2, final long address2) {
        return UNSAFE.getLong(object1, address1) != UNSAFE.getLong(object2, address2);
    }

    private static boolean compare(final long value1, final Object object2, final long address2) {
        return value1 != UNSAFE.getLong(object2, address2);
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
