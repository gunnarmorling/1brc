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
        calculate();
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
        private static final long MAP_MASK = MAPS_SLOT_COUNT - 1;

        private long slowMap;
        private long slowMapNamesPtr;
        private long slowMapNamesLo;
        private long fastMap;
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
            // long word = UNSAFE.getLong(startCursor);
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

        private void doTail() {
            doOne(cursorA, endA);
            doOne(cursorB, endB);
            doOne(cursorC, endC);

            transferToHeap();
            UNSAFE.freeMemory(fastMap);
            UNSAFE.freeMemory(slowMap);
            UNSAFE.freeMemory(slowMapNamesLo);
        }

        private void transferToHeap() {
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
                long ext = -isMaskZeroA & 0xFF00_0000_0000_0000L;
                firstWordMask |= ext;

                long maskedFirstWord = currentWord & firstWordMask;
                long hash = hash(maskedFirstWord);
                while (mask == 0) {
                    cursor += 8;
                    currentWord = UNSAFE.getLong(cursor);
                    mask = getDelimiterMask(currentWord);
                }
                final int delimiterByte = Long.numberOfTrailingZeros(mask);
                final long semicolon = cursor + (delimiterByte >> 3);
                final long maskedWord = currentWord & ((mask - 1) ^ mask) >>> 8;

                long len = semicolon - start;
                long baseEntryPtr = getOrCreateEntryBaseOffsetSlow(len, start, (int) hash, maskedWord);
                long temperatureWord = UNSAFE.getLong(semicolon + 1);
                cursor = parseAndStoreTemperature(semicolon + 1, baseEntryPtr, temperatureWord);
            }
        }

        private static long hash(long word1) {
            // credit: mtopolnik
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 17;

            long hash = word1;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            return hash;
        }

        @Override
        public void run() {
            this.slowMap = UNSAFE.allocateMemory(SLOW_MAP_SIZE_BYTES);
            this.slowMapNamesPtr = UNSAFE.allocateMemory(SLOW_MAP_MAP_NAMES_BYTES);
            this.slowMapNamesLo = slowMapNamesPtr;
            this.fastMap = UNSAFE.allocateMemory(FAST_MAP_SIZE_BYTES);
            UNSAFE.setMemory(slowMap, SLOW_MAP_SIZE_BYTES, (byte) 0);
            UNSAFE.setMemory(fastMap, FAST_MAP_SIZE_BYTES, (byte) 0);
            UNSAFE.setMemory(slowMapNamesPtr, SLOW_MAP_MAP_NAMES_BYTES, (byte) 0);

            while (cursorA < endA && cursorB < endB && cursorC < endC) {
                long startA = cursorA;
                long startB = cursorB;
                long startC = cursorC;

                long currentWordA = UNSAFE.getLong(startA);
                long currentWordB = UNSAFE.getLong(startB);
                long currentWordC = UNSAFE.getLong(startC);

                long maskA = getDelimiterMask(currentWordA);
                long maskB = getDelimiterMask(currentWordB);
                long maskC = getDelimiterMask(currentWordC);

                long firstWordMaskA = (maskA ^ (maskA - 1)) >>> 8;
                long firstWordMaskB = (maskB ^ (maskB - 1)) >>> 8;
                long firstWordMaskC = (maskC ^ (maskC - 1)) >>> 8;

                final long isMaskZeroA = ((maskA | -maskA) >>> 63) ^ 1;
                final long isMaskZeroB = ((maskB | -maskB) >>> 63) ^ 1;
                final long isMaskZeroC = ((maskC | -maskC) >>> 63) ^ 1;

                long extA = -isMaskZeroA & 0xFF00_0000_0000_0000L;
                long extB = -isMaskZeroB & 0xFF00_0000_0000_0000L;
                long extC = -isMaskZeroC & 0xFF00_0000_0000_0000L;

                firstWordMaskA |= extA;
                firstWordMaskB |= extB;
                firstWordMaskC |= extC;

                long maskedFirstWordA = currentWordA & firstWordMaskA;
                long maskedFirstWordB = currentWordB & firstWordMaskB;
                long maskedFirstWordC = currentWordC & firstWordMaskC;

                // assertMasks(isMaskZeroA, maskA);

                long hashA = hash(maskedFirstWordA);
                long hashB = hash(maskedFirstWordB);
                long hashC = hash(maskedFirstWordC);

                cursorA += isMaskZeroA * 8;
                cursorB += isMaskZeroB * 8;
                cursorC += isMaskZeroC * 8;

                currentWordA = UNSAFE.getLong(cursorA);
                currentWordB = UNSAFE.getLong(cursorB);
                currentWordC = UNSAFE.getLong(cursorC);

                maskA = getDelimiterMask(currentWordA);
                while (maskA == 0) {
                    cursorA += 8;
                    currentWordA = UNSAFE.getLong(cursorA);
                    maskA = getDelimiterMask(currentWordA);
                }
                maskB = getDelimiterMask(currentWordB);
                while (maskB == 0) {
                    cursorB += 8;
                    currentWordB = UNSAFE.getLong(cursorB);
                    maskB = getDelimiterMask(currentWordB);
                }
                maskC = getDelimiterMask(currentWordC);
                while (maskC == 0) {
                    cursorC += 8;
                    currentWordC = UNSAFE.getLong(cursorC);
                    maskC = getDelimiterMask(currentWordC);
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

                final long maskedWordA = currentWordA & ((maskA - 1) ^ maskA) >>> 8;
                final long maskedWordB = currentWordB & ((maskB - 1) ^ maskB) >>> 8;
                final long maskedWordC = currentWordC & ((maskC - 1) ^ maskC) >>> 8;

                long lenA = semicolonA - startA;
                long lenB = semicolonB - startB;
                long lenC = semicolonC - startC;

                long baseEntryPtrA;
                if (lenA > 15) {
                    baseEntryPtrA = getOrCreateEntryBaseOffsetSlow(lenA, startA, (int) hashA, maskedWordA);
                }
                else {
                    baseEntryPtrA = getOrCreateEntryBaseOffsetFast(lenA, (int) hashA, maskedWordA, maskedFirstWordA);
                }

                long baseEntryPtrB;
                if (lenB > 15) {
                    baseEntryPtrB = getOrCreateEntryBaseOffsetSlow(lenB, startB, (int) hashB, maskedWordB);
                }
                else {
                    baseEntryPtrB = getOrCreateEntryBaseOffsetFast(lenB, (int) hashB, maskedWordB, maskedFirstWordB);
                }

                long baseEntryPtrC;
                if (lenC > 15) {
                    baseEntryPtrC = getOrCreateEntryBaseOffsetSlow(lenC, startC, (int) hashC, maskedWordC);
                }
                else {
                    baseEntryPtrC = getOrCreateEntryBaseOffsetFast(lenC, (int) hashC, maskedWordC, maskedFirstWordC);
                }

                cursorA = parseAndStoreTemperature(digitStartA, baseEntryPtrA, temperatureWordA);
                cursorB = parseAndStoreTemperature(digitStartB, baseEntryPtrB, temperatureWordB);
                cursorC = parseAndStoreTemperature(digitStartC, baseEntryPtrC, temperatureWordC);
            }
            doTail();
            // System.out.println("Longest chain: " + longestChain);
        }

        private long getOrCreateEntryBaseOffsetFast(long lenLong, int hash, long maskedLastWord, long maskedFirstWord) {
            int lenA = (int) lenLong;
            long mapIndexA = hash & MAP_MASK;
            for (;;) {
                long basePtr = mapIndexA * FAST_MAP_ENTRY_SIZE_BYTES + fastMap;
                long lenPtr = basePtr + MAP_LEN_OFFSET;
                int len = UNSAFE.getInt(lenPtr);
                if (len == lenA) {
                    long namePart1 = UNSAFE.getLong(basePtr + FAST_MAP_NAME_PART1);
                    long namePart2 = UNSAFE.getLong(basePtr + FAST_MAP_NAME_PART2);
                    if (namePart1 == maskedFirstWord && namePart2 == maskedLastWord) {
                        return basePtr;
                    }
                }
                else if (len == 0) {
                    UNSAFE.putInt(lenPtr, lenA);
                    // todo: this could be a single putLong()
                    UNSAFE.putInt(basePtr + MAP_MAX_OFFSET, Integer.MIN_VALUE);
                    UNSAFE.putInt(basePtr + MAP_MIN_OFFSET, Integer.MAX_VALUE);
                    UNSAFE.putLong(basePtr + FAST_MAP_NAME_PART1, maskedFirstWord);
                    UNSAFE.putLong(basePtr + FAST_MAP_NAME_PART2, maskedLastWord);
                    return basePtr;
                }
                mapIndexA = ++mapIndexA & MAP_MASK;
            }
        }

        private long getOrCreateEntryBaseOffsetSlow(long lenLong, long startPtr, int hash, long maskedLastWord) {
            long fullLen = lenLong & ~7L;
            int lenA = (int) lenLong;
            long mapIndexA = hash & MAP_MASK;
            for (;;) {
                long basePtr = mapIndexA * SLOW_MAP_ENTRY_SIZE_BYTES + slowMap;
                long lenPtr = basePtr + MAP_LEN_OFFSET;
                long namePtr = basePtr + SLOW_MAP_NAME_OFFSET;
                int len = UNSAFE.getInt(lenPtr);
                if (len == lenA) {
                    namePtr = UNSAFE.getLong(basePtr + SLOW_MAP_NAME_OFFSET);
                    if (nameMatch(startPtr, maskedLastWord, namePtr, fullLen)) {
                        return basePtr;
                    }
                }
                else if (len == 0) {
                    UNSAFE.putLong(namePtr, slowMapNamesPtr);
                    UNSAFE.putInt(lenPtr, lenA);
                    UNSAFE.putInt(basePtr + MAP_MAX_OFFSET, Integer.MIN_VALUE);
                    UNSAFE.putInt(basePtr + MAP_MIN_OFFSET, Integer.MAX_VALUE);
                    UNSAFE.copyMemory(startPtr, slowMapNamesPtr, lenA);
                    long alignedLen = (lenLong & ~7L) + 8;
                    slowMapNamesPtr += alignedLen;
                    return basePtr;
                }
                mapIndexA = ++mapIndexA & MAP_MASK;
            }
        }

        private static boolean nameMatch(long start, long maskedLastWord, long namePtr, long fullLen) {
            return nameMatchSlow(start, namePtr, fullLen, maskedLastWord);
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
