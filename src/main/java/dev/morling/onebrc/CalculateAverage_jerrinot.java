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

import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * I figured out it would be very hard to win the main competition of the One Billion Rows Challenge.
 * but I think this code has a good chance to win a special prize for the Ugliest Solution ever! :)
 *
 * Anyway, if you can make sense out of not exactly idiomatic Java code, and you enjoy pushing performance limits
 * then QuestDB - the fastest open-source time-series database - is hiring: https://questdb.io/careers/core-database-engineer/
 * <p>
 * <b>Credit</b>
 * <p>
 * I stand on shoulders of giants. I wouldn't be able to code this without analyzing and borrowing from solutions of others.
 * People who helped me the most:
 * <ul>
 * <li>Thomas Wuerthinger (thomaswue): The munmap() trick and work-stealing. In both cases, I shameless copy-pasted their code.
 *     Including SWAR for detecting new lines. Thomas also gave me helpful hints on how to detect register spilling issues.</li>
 * <li>Quan Anh Mai (merykitty): I borrowed their phenomenal branch-free parser.</li>
 * <li>Marko Topolnik (mtopolnik): I use a hashing function I saw in his code. It seems the produce good quality hashes
 *     and it's next-level in speed. Marko joined the challenge before me and our discussions made me to join too!</li>
 * <li>Van Phu DO (abeobk): I saw the idea with simple lookup tables instead of complicated bit-twiddling in their code first.</li>
 * <li>Roy van Rijn (royvanrijn): I borrowed their SWAR code and initially their hash code impl</li>
 * <li>Francesco Nigro (franz1981): For our online discussions about performance. Both before and during this challenge.
 *     Francesco gave me the idea to check register spilling.</li>
 * </ul>
 */
public class CalculateAverage_jerrinot {
    private static final Unsafe UNSAFE = unsafe();
    private static final String MEASUREMENTS_TXT = "measurements.txt";
    // todo: with hyper-threading enable we would be better of with availableProcessors / 2;
    // todo: validate the testing env. params.
    private static final int EXTRA_THREAD_COUNT = Runtime.getRuntime().availableProcessors() - 1;
    // private static final int THREAD_COUNT = 1;

    private static final long SEPARATOR_PATTERN = 0x3B3B3B3B3B3B3B3BL;
    private static final long NEW_LINE_PATTERN = 0x0A0A0A0A0A0A0A0AL;
    private static final int SEGMENT_SIZE = 4 * 1024 * 1024;

    // credits for the idea with lookup tables instead of bit-shifting: abeobk
    private static final long[] HASH_MASKS = new long[]{
            0x0000000000000000L, // semicolon is the first char
            0x00000000000000ffL,
            0x000000000000ffffL,
            0x0000000000ffffffL,
            0x00000000ffffffffL,
            0x000000ffffffffffL,
            0x0000ffffffffffffL,
            0x00ffffffffffffffL, // semicolon is the last char
            0xffffffffffffffffL // there is no semicolon at all
    };

    private static final long[] ADVANCE_MASKS = new long[]{
            0x0000000000000000L,
            0x0000000000000000L,
            0x0000000000000000L,
            0x0000000000000000L,
            0x0000000000000000L,
            0x0000000000000000L,
            0x0000000000000000L,
            0x0000000000000000L,
            0xffffffffffffffffL,
    };

    private static Unsafe unsafe() {
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
        // credits for spawning new workers: thomaswue
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }
        calculate();
    }

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

    static void calculate() throws Exception {
        final File file = new File(MEASUREMENTS_TXT);
        final long length = file.length();
        try (var raf = new RandomAccessFile(file, "r")) {
            long fileStart = raf.getChannel().map(MapMode.READ_ONLY, 0, length, Arena.global()).address();
            long fileEnd = fileStart + length;
            var globalCursor = new AtomicLong(fileStart);

            Processor[] processors = new Processor[EXTRA_THREAD_COUNT];
            Thread[] threads = new Thread[EXTRA_THREAD_COUNT];

            for (int i = 0; i < EXTRA_THREAD_COUNT; i++) {
                Processor processor = new Processor(fileStart, fileEnd, globalCursor);
                Thread thread = new Thread(processor);
                processors[i] = processor;
                threads[i] = thread;
                thread.start();
            }

            Processor processor = new Processor(fileStart, fileEnd, globalCursor);
            processor.run();

            var accumulator = new TreeMap<String, StationStats>();
            processor.accumulateStatus(accumulator);

            for (int i = 0; i < EXTRA_THREAD_COUNT; i++) {
                Thread t = threads[i];
                t.join();
                processors[i].accumulateStatus(accumulator);
            }

            printResults(accumulator);
        }
    }

    private static void printResults(TreeMap<String, StationStats> accumulator) {
        var sb = new StringBuilder(10000);
        boolean first = true;
        for (Map.Entry<String, StationStats> statsEntry : accumulator.entrySet()) {
            if (first) {
                sb.append("{");
                first = false;
            }
            else {
                sb.append(", ");
            }
            var value = statsEntry.getValue();
            var name = statsEntry.getKey();
            int min = value.min;
            int max = value.max;
            int count = value.count;
            long sum2 = value.sum;
            sb.append(String.format("%s=%.1f/%.1f/%.1f", name, min / 10.0, Math.round((double) sum2 / count) / 10.0, max / 10.0));
        }
        sb.append('}');
        System.out.println(sb);
        System.out.close();
    }

    public static int ceilPow2(int i) {
        i--;
        i |= i >> 1;
        i |= i >> 2;
        i |= i >> 4;
        i |= i >> 8;
        i |= i >> 16;
        return i + 1;
    }

    private static class Processor implements Runnable {
        private static final int MAX_UNIQUE_KEYS = 10000;
        private static final int MAPS_SLOT_COUNT = ceilPow2(MAX_UNIQUE_KEYS);
        private static final int STATION_MAX_NAME_BYTES = 104;

        private static final long MAP_COUNT_OFFSET = 0;
        private static final long MAP_MIN_OFFSET = 4;
        private static final long MAP_MAX_OFFSET = 8;
        private static final long MAP_SUM_OFFSET = 12;
        private static final long MAP_LEN_OFFSET = 20;
        private static final long SLOW_MAP_NAME_OFFSET = 24;

        // private int longestChain = 0;

        private static final int SLOW_MAP_ENTRY_SIZE_BYTES = Integer.BYTES // count // 0
                + Integer.BYTES // min // +4
                + Integer.BYTES // max // +8
                + Long.BYTES // sum // +12
                + Integer.BYTES // station name len // +20
                + Long.BYTES; // station name ptr // 24

        private static final long FAST_MAP_NAME_PART1 = 24;
        private static final long FAST_MAP_NAME_PART2 = 32;

        private static final int FAST_MAP_ENTRY_SIZE_BYTES = Integer.BYTES // count // 0
                + Integer.BYTES // min // +4
                + Integer.BYTES // max // +8
                + Long.BYTES // sum // +12
                + Integer.BYTES // station name len // +20
                + Long.BYTES // station name part 1 // 24
                + Long.BYTES; // station name part 2 // 32

        private static final int SLOW_MAP_SIZE_BYTES = MAPS_SLOT_COUNT * SLOW_MAP_ENTRY_SIZE_BYTES;
        private static final int FAST_MAP_SIZE_BYTES = MAPS_SLOT_COUNT * FAST_MAP_ENTRY_SIZE_BYTES;
        private static final int SLOW_MAP_MAP_NAMES_BYTES = MAX_UNIQUE_KEYS * STATION_MAX_NAME_BYTES;
        private static final int MAP_MASK = MAPS_SLOT_COUNT - 1;
        private final AtomicLong globalCursor;

        private long slowMap;
        private long slowMapNamesPtr;
        private long cursorA;
        private long endA;
        private long cursorB;
        private long endB;
        private HashMap<String, CalculateAverage_jerrinot.StationStats> stats = new HashMap<>(1000);
        private final long fileEnd;
        private final long fileStart;

        // credit: merykitty
        private long parseAndStoreTemperature(long startCursor, long baseEntryPtr, long word) {
            long countPtr = baseEntryPtr + MAP_COUNT_OFFSET;
            int cnt = UNSAFE.getInt(countPtr);
            UNSAFE.putInt(countPtr, cnt + 1);

            long minPtr = baseEntryPtr + MAP_MIN_OFFSET;
            long maxPtr = baseEntryPtr + MAP_MAX_OFFSET;
            long sumPtr = baseEntryPtr + MAP_SUM_OFFSET;

            int min = UNSAFE.getInt(minPtr);
            int max = UNSAFE.getInt(maxPtr);
            long sum = UNSAFE.getLong(sumPtr);

            final long negateda = ~word;
            final int dotPos = Long.numberOfTrailingZeros(negateda & 0x10101000);
            final long signed = (negateda << 59) >> 63;
            final long removeSignMask = ~(signed & 0xFF);
            final long digits = ((word & removeSignMask) << (28 - dotPos)) & 0x0F000F0F00L;
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            final int temperature = (int) ((absValue ^ signed) - signed);
            sum += temperature;
            UNSAFE.putLong(sumPtr, sum);

            if (temperature > max) {
                UNSAFE.putInt(maxPtr, temperature);
            }
            if (temperature < min) {
                UNSAFE.putInt(minPtr, temperature);
            }
            return startCursor + (dotPos / 8) + 3;
        }

        private static long getDelimiterMask(final long word) {
            // credit royvanrijn
            final long match = word ^ SEPARATOR_PATTERN;
            return (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);
        }

        void accumulateStatus(TreeMap<String, CalculateAverage_jerrinot.StationStats> accumulator) {
            for (Map.Entry<String, CalculateAverage_jerrinot.StationStats> entry : stats.entrySet()) {
                String name = entry.getKey();
                CalculateAverage_jerrinot.StationStats localStats = entry.getValue();

                CalculateAverage_jerrinot.StationStats globalStats = accumulator.get(name);
                if (globalStats == null) {
                    accumulator.put(name, localStats);
                }
                else {
                    accumulator.put(name, globalStats.mergeWith(localStats));
                }
            }
        }

        Processor(long fileStart, long fileEnd, AtomicLong globalCursor) {
            this.globalCursor = globalCursor;
            this.fileEnd = fileEnd;
            this.fileStart = fileStart;
        }

        private void transferToHeap(long fastMap) {
            for (long baseAddress = slowMap; baseAddress < slowMap + SLOW_MAP_SIZE_BYTES; baseAddress += SLOW_MAP_ENTRY_SIZE_BYTES) {
                long len = UNSAFE.getInt(baseAddress + MAP_LEN_OFFSET);
                if (len == 0) {
                    continue;
                }
                byte[] nameArr = new byte[(int) len];
                long baseNameAddr = UNSAFE.getLong(baseAddress + SLOW_MAP_NAME_OFFSET);
                for (int i = 0; i < len; i++) {
                    nameArr[i] = UNSAFE.getByte(baseNameAddr + i);
                }
                String name = new String(nameArr);
                int min = UNSAFE.getInt(baseAddress + MAP_MIN_OFFSET);
                int max = UNSAFE.getInt(baseAddress + MAP_MAX_OFFSET);
                int count = UNSAFE.getInt(baseAddress + MAP_COUNT_OFFSET);
                long sum = UNSAFE.getLong(baseAddress + MAP_SUM_OFFSET);

                stats.put(name, new CalculateAverage_jerrinot.StationStats(min, max, count, sum));
            }

            for (long baseAddress = fastMap; baseAddress < fastMap + FAST_MAP_SIZE_BYTES; baseAddress += FAST_MAP_ENTRY_SIZE_BYTES) {
                long len = UNSAFE.getInt(baseAddress + MAP_LEN_OFFSET);
                if (len == 0) {
                    continue;
                }
                byte[] nameArr = new byte[(int) len];
                long baseNameAddr = baseAddress + FAST_MAP_NAME_PART1;
                for (int i = 0; i < len; i++) {
                    nameArr[i] = UNSAFE.getByte(baseNameAddr + i);
                }
                String name = new String(nameArr);
                int min = UNSAFE.getInt(baseAddress + MAP_MIN_OFFSET);
                int max = UNSAFE.getInt(baseAddress + MAP_MAX_OFFSET);
                int count = UNSAFE.getInt(baseAddress + MAP_COUNT_OFFSET);
                long sum = UNSAFE.getLong(baseAddress + MAP_SUM_OFFSET);

                var v = stats.get(name);
                if (v == null) {
                    stats.put(name, new CalculateAverage_jerrinot.StationStats(min, max, count, sum));
                }
                else {
                    stats.put(name, new CalculateAverage_jerrinot.StationStats(Math.min(v.min, min), Math.max(v.max, max), v.count + count, v.sum + sum));
                }
            }
        }

        private void doOne(long cursor, long end) {
            while (cursor < end) {
                // it seems that when pulling just from a single chunk
                // then bit-twiddling is faster than lookup tables
                // hypothesis: when processing multiple things at once then LOAD latency is partially hidden
                // but when processing just one thing then it's better to keep things local as much as possible? maybe:)

                long start = cursor;
                long currentWord = UNSAFE.getLong(cursor);
                long mask = getDelimiterMask(currentWord);
                long firstWordMask = ((mask - 1) ^ mask) >>> 8;
                final long isMaskZeroA = ((mask | -mask) >>> 63) ^ 1;
                long ext = -isMaskZeroA;
                firstWordMask |= ext;

                long maskedFirstWord = currentWord & firstWordMask;
                int hash = hash(maskedFirstWord);
                while (mask == 0) {
                    cursor += 8;
                    currentWord = UNSAFE.getLong(cursor);
                    mask = getDelimiterMask(currentWord);
                }
                final int delimiterByte = Long.numberOfTrailingZeros(mask);
                final long semicolon = cursor + (delimiterByte >> 3);
                final long maskedWord = currentWord & ((mask - 1) ^ mask) >>> 8;

                int len = (int) (semicolon - start);
                long baseEntryPtr = getOrCreateEntryBaseOffsetSlow(len, start, hash, maskedWord);
                long temperatureWord = UNSAFE.getLong(semicolon + 1);
                cursor = parseAndStoreTemperature(semicolon + 1, baseEntryPtr, temperatureWord);
            }
        }

        private static int hash(long word) {
            // credit: mtopolnik
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 17;
            //
            long hash = word;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            return (int) hash;
        }

        private static long nextNewLine(long prev) {
            // again: credits to @thomaswue for this code, literally copy'n'paste
            while (true) {
                long currentWord = UNSAFE.getLong(prev);
                long input = currentWord ^ NEW_LINE_PATTERN;
                long pos = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
                if (pos != 0) {
                    prev += Long.numberOfTrailingZeros(pos) >>> 3;
                    break;
                }
                else {
                    prev += 8;
                }
            }
            return prev;
        }

        @Override
        public void run() {
            long fastMap = allocateMem();
            for (;;) {
                long startingPtr = globalCursor.addAndGet(SEGMENT_SIZE) - SEGMENT_SIZE;
                if (startingPtr >= fileEnd) {
                    break;
                }
                setCursors(startingPtr);
                mainLoop(fastMap);
                doOne(cursorA, endA);
                doOne(cursorB, endB);
            }
            transferToHeap(fastMap);
        }

        private long allocateMem() {
            this.slowMap = UNSAFE.allocateMemory(SLOW_MAP_SIZE_BYTES);
            this.slowMapNamesPtr = UNSAFE.allocateMemory(SLOW_MAP_MAP_NAMES_BYTES);
            long fastMap = UNSAFE.allocateMemory(FAST_MAP_SIZE_BYTES);
            UNSAFE.setMemory(slowMap, SLOW_MAP_SIZE_BYTES, (byte) 0);
            UNSAFE.setMemory(fastMap, FAST_MAP_SIZE_BYTES, (byte) 0);
            UNSAFE.setMemory(slowMapNamesPtr, SLOW_MAP_MAP_NAMES_BYTES, (byte) 0);
            return fastMap;
        }

        private void mainLoop(long fastMap) {
            while (cursorA < endA && cursorB < endB) {
                long currentWordA = UNSAFE.getLong(cursorA);
                long currentWordB = UNSAFE.getLong(cursorB);

                long delimiterMaskA = getDelimiterMask(currentWordA);
                long delimiterMaskB = getDelimiterMask(currentWordB);

                long candidateWordA = UNSAFE.getLong(cursorA + 8);
                long candidateWordB = UNSAFE.getLong(cursorB + 8);

                long startA = cursorA;
                long startB = cursorB;

                int trailingZerosA = Long.numberOfTrailingZeros(delimiterMaskA) >> 3;
                int trailingZerosB = Long.numberOfTrailingZeros(delimiterMaskB) >> 3;

                long advanceMaskA = ADVANCE_MASKS[trailingZerosA];
                long advanceMaskB = ADVANCE_MASKS[trailingZerosB];

                long wordMaskA = HASH_MASKS[trailingZerosA];
                long wordMaskB = HASH_MASKS[trailingZerosB];

                long negAdvanceMaskA = ~advanceMaskA;
                long negAdvanceMaskB = ~advanceMaskB;

                cursorA += advanceMaskA & 8;
                cursorB += advanceMaskB & 8;

                long nextWordA = (advanceMaskA & candidateWordA) | (negAdvanceMaskA & currentWordA);
                long nextWordB = (advanceMaskB & candidateWordB) | (negAdvanceMaskB & currentWordB);

                long nextDelimiterMaskA = getDelimiterMask(nextWordA);
                long nextDelimiterMaskB = getDelimiterMask(nextWordB);

                boolean slowA = nextDelimiterMaskA == 0;
                boolean slowB = nextDelimiterMaskB == 0;
                boolean slowSome = (slowA || slowB);

                long maskedFirstWordA = wordMaskA & currentWordA;
                long maskedFirstWordB = wordMaskB & currentWordB;

                int hashA = hash(maskedFirstWordA);
                int hashB = hash(maskedFirstWordB);

                currentWordA = nextWordA;
                currentWordB = nextWordB;

                delimiterMaskA = nextDelimiterMaskA;
                delimiterMaskB = nextDelimiterMaskB;
                if (slowSome) {
                    while (delimiterMaskA == 0) {
                        cursorA += 8;
                        currentWordA = UNSAFE.getLong(cursorA);
                        delimiterMaskA = getDelimiterMask(currentWordA);
                    }

                    while (delimiterMaskB == 0) {
                        cursorB += 8;
                        currentWordB = UNSAFE.getLong(cursorB);
                        delimiterMaskB = getDelimiterMask(currentWordB);
                    }
                }

                trailingZerosA = Long.numberOfTrailingZeros(delimiterMaskA) >> 3;
                trailingZerosB = Long.numberOfTrailingZeros(delimiterMaskB) >> 3;

                final long semicolonA = cursorA + trailingZerosA;
                final long semicolonB = cursorB + trailingZerosB;

                long digitStartA = semicolonA + 1;
                long digitStartB = semicolonB + 1;

                long lastWordMaskA = HASH_MASKS[trailingZerosA];
                long lastWordMaskB = HASH_MASKS[trailingZerosB];

                long temperatureWordA = UNSAFE.getLong(digitStartA);
                long temperatureWordB = UNSAFE.getLong(digitStartB);

                final long maskedLastWordA = currentWordA & lastWordMaskA;
                final long maskedLastWordB = currentWordB & lastWordMaskB;

                int lenA = (int) (semicolonA - startA);
                int lenB = (int) (semicolonB - startB);

                int mapIndexA = hashA & MAP_MASK;
                int mapIndexB = hashB & MAP_MASK;

                long baseEntryPtrA;
                long baseEntryPtrB;

                if (slowSome) {
                    if (slowA) {
                        baseEntryPtrA = getOrCreateEntryBaseOffsetSlow(lenA, startA, hashA, maskedLastWordA);
                    }
                    else {
                        baseEntryPtrA = getOrCreateEntryBaseOffsetFast(mapIndexA, lenA, maskedLastWordA, maskedFirstWordA, fastMap);
                    }

                    if (slowB) {
                        baseEntryPtrB = getOrCreateEntryBaseOffsetSlow(lenB, startB, hashB, maskedLastWordB);
                    }
                    else {
                        baseEntryPtrB = getOrCreateEntryBaseOffsetFast(mapIndexB, lenB, maskedLastWordB, maskedFirstWordB, fastMap);
                    }

                }
                else {
                    baseEntryPtrA = getOrCreateEntryBaseOffsetFast(mapIndexA, lenA, maskedLastWordA, maskedFirstWordA, fastMap);
                    baseEntryPtrB = getOrCreateEntryBaseOffsetFast(mapIndexB, lenB, maskedLastWordB, maskedFirstWordB, fastMap);
                }

                cursorA = parseAndStoreTemperature(digitStartA, baseEntryPtrA, temperatureWordA);
                cursorB = parseAndStoreTemperature(digitStartB, baseEntryPtrB, temperatureWordB);
            }
        }

        private void setCursors(long current) {
            // Credit for the whole work-stealing scheme: @thomaswue
            // I have totally stolen it from him. I changed the order a bit to suite my taste better,
            // but it's his code
            long segmentStart;
            if (current == fileStart) {
                segmentStart = current;
            }
            else {
                segmentStart = nextNewLine(current) + 1;
            }
            long segmentEnd = nextNewLine(Math.min(fileEnd - 1, current + SEGMENT_SIZE));

            long size = (segmentEnd - segmentStart) / 2;
            long mid = nextNewLine(segmentStart + size);

            cursorA = segmentStart;
            endA = mid;
            cursorB = mid + 1;
            endB = segmentEnd;
        }

        private static long getOrCreateEntryBaseOffsetFast(int mapIndexA, int lenA, long maskedLastWord, long maskedFirstWord, long fastMap) {
            for (;;) {
                long basePtr = mapIndexA * FAST_MAP_ENTRY_SIZE_BYTES + fastMap;
                long namePart1 = UNSAFE.getLong(basePtr + FAST_MAP_NAME_PART1);
                long namePart2 = UNSAFE.getLong(basePtr + FAST_MAP_NAME_PART2);
                if (namePart1 == maskedFirstWord && namePart2 == maskedLastWord) {
                    return basePtr;
                }
                long lenPtr = basePtr + MAP_LEN_OFFSET;
                int len = UNSAFE.getInt(lenPtr);
                if (len == 0) {
                    return newEntryFast(lenA, maskedLastWord, maskedFirstWord, lenPtr, basePtr);
                }
                mapIndexA = ++mapIndexA & MAP_MASK;
            }
        }

        private static long newEntryFast(int lenA, long maskedLastWord, long maskedFirstWord, long lenPtr, long basePtr) {
            UNSAFE.putInt(lenPtr, lenA);
            // todo: this could be a single putLong()
            UNSAFE.putInt(basePtr + MAP_MAX_OFFSET, Integer.MIN_VALUE);
            UNSAFE.putInt(basePtr + MAP_MIN_OFFSET, Integer.MAX_VALUE);
            UNSAFE.putLong(basePtr + FAST_MAP_NAME_PART1, maskedFirstWord);
            UNSAFE.putLong(basePtr + FAST_MAP_NAME_PART2, maskedLastWord);
            return basePtr;
        }

        private long getOrCreateEntryBaseOffsetSlow(int lenA, long startPtr, int hash, long maskedLastWord) {
            long fullLen = lenA & ~7L;
            long mapIndexA = hash & MAP_MASK;
            for (;;) {
                long basePtr = mapIndexA * SLOW_MAP_ENTRY_SIZE_BYTES + slowMap;
                long lenPtr = basePtr + MAP_LEN_OFFSET;
                long namePtr = basePtr + SLOW_MAP_NAME_OFFSET;
                int len = UNSAFE.getInt(lenPtr);
                if (len == lenA) {
                    namePtr = UNSAFE.getLong(basePtr + SLOW_MAP_NAME_OFFSET);
                    if (nameMatchSlow(startPtr, namePtr, fullLen, maskedLastWord)) {
                        return basePtr;
                    }
                }
                else if (len == 0) {
                    UNSAFE.putLong(namePtr, slowMapNamesPtr);
                    UNSAFE.putInt(lenPtr, lenA);
                    UNSAFE.putInt(basePtr + MAP_MAX_OFFSET, Integer.MIN_VALUE);
                    UNSAFE.putInt(basePtr + MAP_MIN_OFFSET, Integer.MAX_VALUE);
                    UNSAFE.copyMemory(startPtr, slowMapNamesPtr, lenA);
                    long alignedLen = (lenA & ~7L) + 8;
                    slowMapNamesPtr += alignedLen;
                    return basePtr;
                }
                mapIndexA = ++mapIndexA & MAP_MASK;
            }
        }

        private static boolean nameMatchSlow(long start, long namePtr, long fullLen, long maskedLastWord) {
            long offset;
            for (offset = 0; offset < fullLen; offset += 8) {
                if (UNSAFE.getLong(start + offset) != UNSAFE.getLong(namePtr + offset)) {
                    return false;
                }
            }
            long maskedWordInMap = UNSAFE.getLong(namePtr + fullLen);
            return (maskedWordInMap == maskedLastWord);
        }
    }

    record StationStats(int min, int max, int count, long sum) {
        StationStats mergeWith(StationStats other) {
            return new StationStats(Math.min(min, other.min), Math.max(max, other.max), count + other.count, sum + other.sum);
        }
    }
}
