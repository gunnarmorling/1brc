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

/**
 * I figured out it would be very hard to win the main competition of the One Billion Rows Challenge.
 * but I think this code has a good chance to win a special prize for the Ugliest Solution ever! :)
 *
 * Anyway, if you can make sense out of not exactly idiomatic Java code, and you enjoy pushing performance limits
 * then QuestDB - the fastest open-source time-series database - is hiring: https://questdb.io/careers/core-database-engineer/
 *
 */
public class CalculateAverage_jerrinot {
    private static final Unsafe UNSAFE = unsafe();
    private static final String MEASUREMENTS_TXT = "measurements.txt";
    // todo: with hyper-threading enable we would be better of with availableProcessors / 2;
    // todo: validate the testing env. params.
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    // private static final int THREAD_COUNT = 4;

    private static final long SEPARATOR_PATTERN = 0x3B3B3B3B3B3B3B3BL;

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
        // final int chunkCount = Runtime.getRuntime().availableProcessors();
        int chunkPerThread = 3;
        final int chunkCount = THREAD_COUNT * chunkPerThread;
        final var chunkStartOffsets = new long[chunkCount + 1];
        try (var raf = new RandomAccessFile(file, "r")) {
            // credit - chunking code: mtopolnik
            final var inputBase = raf.getChannel().map(MapMode.READ_ONLY, 0, length, Arena.global()).address();
            for (int i = 1; i < chunkStartOffsets.length - 1; i++) {
                var start = length * i / (chunkStartOffsets.length - 1);
                raf.seek(start);
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start + inputBase;
            }
            chunkStartOffsets[0] = inputBase;
            chunkStartOffsets[chunkCount] = inputBase + length;

            Processor[] processors = new Processor[THREAD_COUNT];
            Thread[] threads = new Thread[THREAD_COUNT];

            for (int i = 0; i < THREAD_COUNT - 1; i++) {
                long startA = chunkStartOffsets[i * chunkPerThread];
                long endA = chunkStartOffsets[i * chunkPerThread + 1];
                long startB = chunkStartOffsets[i * chunkPerThread + 1];
                long endB = chunkStartOffsets[i * chunkPerThread + 2];
                long startC = chunkStartOffsets[i * chunkPerThread + 2];
                long endC = chunkStartOffsets[i * chunkPerThread + 3];

                Processor processor = new Processor(startA, endA, startB, endB, startC, endC);
                processors[i] = processor;
                Thread thread = new Thread(processor);
                threads[i] = thread;
                thread.start();
            }

            int ownIndex = THREAD_COUNT - 1;
            long startA = chunkStartOffsets[ownIndex * chunkPerThread];
            long endA = chunkStartOffsets[ownIndex * chunkPerThread + 1];
            long startB = chunkStartOffsets[ownIndex * chunkPerThread + 1];
            long endB = chunkStartOffsets[ownIndex * chunkPerThread + 2];
            long startC = chunkStartOffsets[ownIndex * chunkPerThread + 2];
            long endC = chunkStartOffsets[ownIndex * chunkPerThread + 3];
            Processor processor = new Processor(startA, endA, startB, endB, startC, endC);
            processor.run();

            var accumulator = new TreeMap<String, Processor.StationStats>();
            processor.accumulateStatus(accumulator);

            for (int i = 0; i < THREAD_COUNT - 1; i++) {
                Thread t = threads[i];
                t.join();
                processors[i].accumulateStatus(accumulator);
            }

            printResults(accumulator);
        }
    }

    private static void printResults(TreeMap<String, Processor.StationStats> accumulator) {
        var sb = new StringBuilder(10000);
        boolean first = true;
        for (Map.Entry<String, Processor.StationStats> statsEntry : accumulator.entrySet()) {
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

        private long slowMap;
        private long slowMapNamesPtr;
        private long slowMapNamesLo;
        // private long fastMap;
        private long cursorA;
        private long endA;
        private long cursorB;
        private long endB;
        private long cursorC;
        private long endC;
        private HashMap<String, StationStats> stats = new HashMap<>(1000);

        // private long maxClusterLen;

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

        // todo: immutability cost us in allocations, but that's probably peanuts in the grand scheme of things. still worth checking
        // maybe JVM trusting Final in Records offsets it ..a test is needed
        record StationStats(int min, int max, int count, long sum) {
            StationStats mergeWith(StationStats other) {
                return new StationStats(Math.min(min, other.min), Math.max(max, other.max), count + other.count, sum + other.sum);
            }
        }

        void accumulateStatus(TreeMap<String, StationStats> accumulator) {
            for (Map.Entry<String, StationStats> entry : stats.entrySet()) {
                String name = entry.getKey();
                StationStats localStats = entry.getValue();

                StationStats globalStats = accumulator.get(name);
                if (globalStats == null) {
                    accumulator.put(name, localStats);
                }
                else {
                    accumulator.put(name, globalStats.mergeWith(localStats));
                }
            }
        }

        Processor(long startA, long endA, long startB, long endB, long startC, long endC) {
            this.cursorA = startA;
            this.cursorB = startB;
            this.cursorC = startC;
            this.endA = endA;
            this.endB = endB;
            this.endC = endC;
        }

        private void doTail(long fastMAp) {
            doOne(cursorA, endA);
            doOne(cursorB, endB);
            doOne(cursorC, endC);

            transferToHeap(fastMAp);
            // UNSAFE.freeMemory(fastMap);
            // UNSAFE.freeMemory(slowMap);
            // UNSAFE.freeMemory(slowMapNamesLo);
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

                stats.put(name, new StationStats(min, max, count, sum));
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
                    stats.put(name, new StationStats(min, max, count, sum));
                }
                else {
                    stats.put(name, new StationStats(Math.min(v.min, min), Math.max(v.max, max), v.count + count, v.sum + sum));
                }
            }
        }

        private void doOne(long cursor, long endA) {
            while (cursor < endA) {
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

        @Override
        public void run() {
            this.slowMap = UNSAFE.allocateMemory(SLOW_MAP_SIZE_BYTES);
            this.slowMapNamesPtr = UNSAFE.allocateMemory(SLOW_MAP_MAP_NAMES_BYTES);
            this.slowMapNamesLo = slowMapNamesPtr;
            long fastMap = UNSAFE.allocateMemory(FAST_MAP_SIZE_BYTES);
            UNSAFE.setMemory(slowMap, SLOW_MAP_SIZE_BYTES, (byte) 0);
            UNSAFE.setMemory(fastMap, FAST_MAP_SIZE_BYTES, (byte) 0);
            UNSAFE.setMemory(slowMapNamesPtr, SLOW_MAP_MAP_NAMES_BYTES, (byte) 0);

            while (cursorA < endA && cursorB < endB && cursorC < endC) {
                long currentWordA = UNSAFE.getLong(cursorA);
                long currentWordB = UNSAFE.getLong(cursorB);
                long currentWordC = UNSAFE.getLong(cursorC);

                long startA = cursorA;
                long startB = cursorB;
                long startC = cursorC;

                long maskA = getDelimiterMask(currentWordA);
                long maskB = getDelimiterMask(currentWordB);
                long maskC = getDelimiterMask(currentWordC);

                long maskComplementA = -maskA;
                long maskComplementB = -maskB;
                long maskComplementC = -maskC;

                long maskWithDelimiterA = (maskA ^ (maskA - 1));
                long maskWithDelimiterB = (maskB ^ (maskB - 1));
                long maskWithDelimiterC = (maskC ^ (maskC - 1));

                long isMaskZeroA = (((maskA | maskComplementA) >>> 63) ^ 1);
                long isMaskZeroB = (((maskB | maskComplementB) >>> 63) ^ 1);
                long isMaskZeroC = (((maskC | maskComplementC) >>> 63) ^ 1);

                cursorA += isMaskZeroA << 3;
                cursorB += isMaskZeroB << 3;
                cursorC += isMaskZeroC << 3;

                long nextWordA = UNSAFE.getLong(cursorA);
                long nextWordB = UNSAFE.getLong(cursorB);
                long nextWordC = UNSAFE.getLong(cursorC);

                long firstWordMaskA = maskWithDelimiterA >>> 8;
                long firstWordMaskB = maskWithDelimiterB >>> 8;
                long firstWordMaskC = maskWithDelimiterC >>> 8;

                long nextMaskA = getDelimiterMask(nextWordA);
                long nextMaskB = getDelimiterMask(nextWordB);
                long nextMaskC = getDelimiterMask(nextWordC);

                boolean slowA = nextMaskA == 0;
                boolean slowB = nextMaskB == 0;
                boolean slowC = nextMaskC == 0;
                boolean slowSome = (slowA || slowB || slowC);

                long extA = -isMaskZeroA;
                long extB = -isMaskZeroB;
                long extC = -isMaskZeroC;

                long maskedFirstWordA = (extA | firstWordMaskA) & currentWordA;
                long maskedFirstWordB = (extB | firstWordMaskB) & currentWordB;
                long maskedFirstWordC = (extC | firstWordMaskC) & currentWordC;

                int hashA = hash(maskedFirstWordA);
                int hashB = hash(maskedFirstWordB);
                int hashC = hash(maskedFirstWordC);

                currentWordA = nextWordA;
                currentWordB = nextWordB;
                currentWordC = nextWordC;

                maskA = nextMaskA;
                maskB = nextMaskB;
                maskC = nextMaskC;
                if (slowSome) {
                    while (maskA == 0) {
                        cursorA += 8;
                        currentWordA = UNSAFE.getLong(cursorA);
                        maskA = getDelimiterMask(currentWordA);
                    }

                    while (maskB == 0) {
                        cursorB += 8;
                        currentWordB = UNSAFE.getLong(cursorB);
                        maskB = getDelimiterMask(currentWordB);
                    }
                    while (maskC == 0) {
                        cursorC += 8;
                        currentWordC = UNSAFE.getLong(cursorC);
                        maskC = getDelimiterMask(currentWordC);
                    }
                }

                final int delimiterByteA = Long.numberOfTrailingZeros(maskA);
                final int delimiterByteB = Long.numberOfTrailingZeros(maskB);
                final int delimiterByteC = Long.numberOfTrailingZeros(maskC);

                final long semicolonA = cursorA + (delimiterByteA >> 3);
                final long semicolonB = cursorB + (delimiterByteB >> 3);
                final long semicolonC = cursorC + (delimiterByteC >> 3);

                long digitStartA = semicolonA + 1;
                long digitStartB = semicolonB + 1;
                long digitStartC = semicolonC + 1;

                long temperatureWordA = UNSAFE.getLong(digitStartA);
                long temperatureWordB = UNSAFE.getLong(digitStartB);
                long temperatureWordC = UNSAFE.getLong(digitStartC);

                long lastWordMaskA = ((maskA - 1) ^ maskA) >>> 8;
                long lastWordMaskB = ((maskB - 1) ^ maskB) >>> 8;
                long lastWordMaskC = ((maskC - 1) ^ maskC) >>> 8;

                final long maskedLastWordA = currentWordA & lastWordMaskA;
                final long maskedLastWordB = currentWordB & lastWordMaskB;
                final long maskedLastWordC = currentWordC & lastWordMaskC;

                int lenA = (int) (semicolonA - startA);
                int lenB = (int) (semicolonB - startB);
                int lenC = (int) (semicolonC - startC);

                int mapIndexA = hashA & MAP_MASK;
                int mapIndexB = hashB & MAP_MASK;
                int mapIndexC = hashC & MAP_MASK;

                long baseEntryPtrA;
                long baseEntryPtrB;
                long baseEntryPtrC;

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

                    if (slowC) {
                        baseEntryPtrC = getOrCreateEntryBaseOffsetSlow(lenC, startC, hashC, maskedLastWordC);
                    }
                    else {
                        baseEntryPtrC = getOrCreateEntryBaseOffsetFast(mapIndexC, lenC, maskedLastWordC, maskedFirstWordC, fastMap);
                    }
                }
                else {
                    baseEntryPtrA = getOrCreateEntryBaseOffsetFast(mapIndexA, lenA, maskedLastWordA, maskedFirstWordA, fastMap);
                    baseEntryPtrB = getOrCreateEntryBaseOffsetFast(mapIndexB, lenB, maskedLastWordB, maskedFirstWordB, fastMap);
                    baseEntryPtrC = getOrCreateEntryBaseOffsetFast(mapIndexC, lenC, maskedLastWordC, maskedFirstWordC, fastMap);
                }

                cursorA = parseAndStoreTemperature(digitStartA, baseEntryPtrA, temperatureWordA);
                cursorB = parseAndStoreTemperature(digitStartB, baseEntryPtrB, temperatureWordB);
                cursorC = parseAndStoreTemperature(digitStartC, baseEntryPtrC, temperatureWordC);
            }
            doTail(fastMap);
            // System.out.println("Longest chain: " + longestChain);
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

}
