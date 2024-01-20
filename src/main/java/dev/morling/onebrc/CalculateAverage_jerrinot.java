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
    // private static final int THREAD_COUNT = 1;

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
        int chunkPerThread = 4;
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
                long startD = chunkStartOffsets[i * chunkPerThread + 3];
                long endD = chunkStartOffsets[i * chunkPerThread + 4];

                Processor processor = new Processor(startA, endA, startB, endB, startC, endC, startD, endD);
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
            long startD = chunkStartOffsets[ownIndex * chunkPerThread + 3];
            long endD = chunkStartOffsets[ownIndex * chunkPerThread + 4];
            Processor processor = new Processor(startA, endA, startB, endB, startC, endC, startD, endD);
            processor.run();

            var accumulator = new TreeMap<String, Processor.StationStats>();
            processor.accumulateStatus(accumulator);

            for (int i = 0; i < THREAD_COUNT - 1; i++) {
                Thread t = threads[i];
                t.join();
                processors[i].accumulateStatus(accumulator);
            }

            var sb = new StringBuilder();
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
            System.out.print(sb);
            System.out.println('}');
        }
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
        private static final int MAP_SLOT_COUNT = ceilPow2(MAX_UNIQUE_KEYS);
        private static final int STATION_MAX_NAME_BYTES = 104;

        private static final long COUNT_OFFSET = 0;
        private static final long MIN_OFFSET = 4;
        private static final long MAX_OFFSET = 8;
        private static final long SUM_OFFSET = 12;
        private static final long LEN_OFFSET = 20;
        private static final long NAME_OFFSET = 24;

        private static final int MAP_ENTRY_SIZE_BYTES = Integer.BYTES // count // 0
                + Integer.BYTES // min // +4
                + Integer.BYTES // max // +8
                + Long.BYTES // sum // +12
                + Integer.BYTES // station name len // +20
                + Long.BYTES; // station name ptr // 24

        private static final int MAP_SIZE_BYTES = MAP_SLOT_COUNT * MAP_ENTRY_SIZE_BYTES;
        private static final int MAP_NAMES_BYTES = MAX_UNIQUE_KEYS * STATION_MAX_NAME_BYTES;
        private static final long MAP_MASK = MAP_SLOT_COUNT - 1;

        private final long map;
        private long currentNamesPtr;
        private final long namesHi;
        private long cursorA;
        private long endA;
        private long cursorB;
        private long endB;
        private long cursorC;
        private long endC;
        private long cursorD;
        private long endD;

        // private long maxClusterLen;

        // credit: merykitty
        private long parseAndStoreTemperature(long startCursor, long baseEntryPtr, long word) {
            // long word = UNSAFE.getLong(startCursor);
            long countPtr = baseEntryPtr + COUNT_OFFSET;
            int cnt = UNSAFE.getInt(countPtr);
            UNSAFE.putInt(countPtr, cnt + 1);

            long minPtr = baseEntryPtr + MIN_OFFSET;
            long maxPtr = baseEntryPtr + MAX_OFFSET;
            long sumPtr = baseEntryPtr + SUM_OFFSET;

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
        }

        void accumulateStatus(TreeMap<String, StationStats> accumulator) {
            for (long baseAddress = map; baseAddress < map + MAP_SIZE_BYTES; baseAddress += MAP_ENTRY_SIZE_BYTES) {
                long len = UNSAFE.getInt(baseAddress + LEN_OFFSET);
                if (len == 0) {
                    continue;
                }
                byte[] nameArr = new byte[(int) len];
                long baseNameAddr = UNSAFE.getLong(baseAddress + NAME_OFFSET);
                for (int i = 0; i < len; i++) {
                    nameArr[i] = UNSAFE.getByte(baseNameAddr + i);
                }
                String name = new String(nameArr);
                int min = UNSAFE.getInt(baseAddress + MIN_OFFSET);
                int max = UNSAFE.getInt(baseAddress + MAX_OFFSET);
                int count = UNSAFE.getInt(baseAddress + COUNT_OFFSET);
                long sum = UNSAFE.getLong(baseAddress + SUM_OFFSET);

                var v = accumulator.get(name);
                if (v == null) {
                    accumulator.put(name, new StationStats(min, max, count, sum));
                }
                else {
                    accumulator.put(name, new StationStats(Math.min(v.min, min), Math.max(v.max, max), v.count + count, v.sum + sum));
                }
            }
        }

        Processor(long startA, long endA, long startB, long endB, long startC, long endC, long startD, long endD) {
            this.cursorA = startA;
            this.cursorB = startB;
            this.cursorC = startC;
            this.cursorD = startD;
            this.endA = endA;
            this.endB = endB;
            this.endC = endC;
            this.endD = endD;
            this.map = UNSAFE.allocateMemory(MAP_SIZE_BYTES);
            this.currentNamesPtr = UNSAFE.allocateMemory(MAP_NAMES_BYTES);
            this.namesHi = currentNamesPtr + MAP_NAMES_BYTES;

            int i;
            for (i = 0; i < MAP_SIZE_BYTES; i += 8) {
                UNSAFE.putLong(map + i, 0);
            }
            for (i = i - 8; i < MAP_SIZE_BYTES; i++) {
                UNSAFE.putByte(map + i, (byte) 0);
            }
            UNSAFE.setMemory(currentNamesPtr, MAP_NAMES_BYTES, (byte) 0);
        }

        private void doTail() {
            // todo: we would be probably better of without all that code dup. ("compilers hates him!")
            // System.out.println("done ILP");
            doOne(cursorA, endA);
            // System.out.println("done A");
            doOne(cursorB, endB);
            // System.out.println("done B");
            doOne(cursorC, endC);
            // System.out.println("done C");
            doOne(cursorD, endD);
            // System.out.println("done D");
        }

        private void doOne(long cursor, long endA) {
            while (cursor < endA) {
                long start = cursor;
                long currentWord = UNSAFE.getLong(cursor);
                long mask = getDelimiterMask(currentWord);
                long maskedFirstWord = currentWord & ((mask - 1) ^ mask) >>> 8;
                long hash = hash(maskedFirstWord);
                while (mask == 0) {
                    cursor += 8;
                    currentWord = UNSAFE.getLong(cursor);
                    mask = getDelimiterMask(currentWord);
                }
                final int delimiterByte = Long.numberOfTrailingZeros(mask);
                final long semicolon = cursor + (delimiterByte >> 3);
                final long maskedWord = currentWord & ((mask - 1) ^ mask) >>> 8;
                long baseEntryPtr = getOrCreateEntryBaseOffset(semicolon, start, (int) hash, maskedWord);
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
            while (cursorA < endA && cursorB < endB && cursorC < endC && cursorD < endD) {
                long startA = cursorA;
                long startB = cursorB;
                long startC = cursorC;
                long startD = cursorD;

                long currentWordA = UNSAFE.getLong(startA);
                long currentWordB = UNSAFE.getLong(startB);
                long currentWordC = UNSAFE.getLong(startC);
                long currentWordD = UNSAFE.getLong(startD);

                // credits for the hashing idea: mtopolnik
                long maskA = getDelimiterMask(currentWordA);
                long maskedFirstWordA = currentWordA & ((maskA - 1) ^ maskA) >>> 8;
                long hashA = hash(maskedFirstWordA);
                while (maskA == 0) {
                    cursorA += 8;
                    currentWordA = UNSAFE.getLong(cursorA);
                    maskA = getDelimiterMask(currentWordA);
                }
                final int delimiterByteA = Long.numberOfTrailingZeros(maskA);
                final long semicolonA = cursorA + (delimiterByteA >> 3);
                long temperatureWordA = UNSAFE.getLong(semicolonA + 1);
                final long maskedWordA = currentWordA & ((maskA - 1) ^ maskA) >>> 8;

                long maskB = getDelimiterMask(currentWordB);
                long maskedFirstWordB = currentWordB & ((maskB - 1) ^ maskB) >>> 8;
                long hashB = hash(maskedFirstWordB);
                while (maskB == 0) {
                    cursorB += 8;
                    currentWordB = UNSAFE.getLong(cursorB);
                    maskB = getDelimiterMask(currentWordB);
                }
                final int delimiterByteB = Long.numberOfTrailingZeros(maskB);
                final long semicolonB = cursorB + (delimiterByteB >> 3);
                long temperatureWordB = UNSAFE.getLong(semicolonB + 1);
                final long maskedWordB = currentWordB & ((maskB - 1) ^ maskB) >>> 8;

                long maskC = getDelimiterMask(currentWordC);
                long maskedFirstWordC = currentWordC & ((maskC - 1) ^ maskC) >>> 8;
                long hashC = hash(maskedFirstWordC);
                while (maskC == 0) {
                    cursorC += 8;
                    currentWordC = UNSAFE.getLong(cursorC);
                    maskC = getDelimiterMask(currentWordC);
                }
                final int delimiterByteC = Long.numberOfTrailingZeros(maskC);
                final long semicolonC = cursorC + (delimiterByteC >> 3);
                long temperatureWordC = UNSAFE.getLong(semicolonC + 1);
                final long maskedWordC = currentWordC & ((maskC - 1) ^ maskC) >>> 8;

                long maskD = getDelimiterMask(currentWordD);
                long maskedFirstWordD = currentWordD & ((maskD - 1) ^ maskD) >>> 8;
                long hashD = hash(maskedFirstWordD);
                while (maskD == 0) {
                    cursorD += 8;
                    currentWordD = UNSAFE.getLong(cursorD);
                    maskD = getDelimiterMask(currentWordD);
                }
                final int delimiterByteD = Long.numberOfTrailingZeros(maskD);
                final long semicolonD = cursorD + (delimiterByteD >> 3);
                long temperatureWordD = UNSAFE.getLong(semicolonD + 1);
                final long maskedWordD = currentWordD & ((maskD - 1) ^ maskD) >>> 8;

                long baseEntryPtrA = getOrCreateEntryBaseOffset(semicolonA, startA, (int) hashA, maskedWordA);
                long baseEntryPtrB = getOrCreateEntryBaseOffset(semicolonB, startB, (int) hashB, maskedWordB);
                long baseEntryPtrC = getOrCreateEntryBaseOffset(semicolonC, startC, (int) hashC, maskedWordC);
                long baseEntryPtrD = getOrCreateEntryBaseOffset(semicolonD, startD, (int) hashD, maskedWordD);

                cursorA = parseAndStoreTemperature(semicolonA + 1, baseEntryPtrA, temperatureWordA);
                cursorB = parseAndStoreTemperature(semicolonB + 1, baseEntryPtrB, temperatureWordB);
                cursorC = parseAndStoreTemperature(semicolonC + 1, baseEntryPtrC, temperatureWordC);
                cursorD = parseAndStoreTemperature(semicolonD + 1, baseEntryPtrD, temperatureWordD);
            }
            doTail();
        }

        private long getOrCreateEntryBaseOffset(long semicolonPtr, long startPtr, int hash, long maskedWord) {
            long lenLong = semicolonPtr - startPtr;
            int lenA = (int) lenLong;

            long mapIndexA = hash & MAP_MASK;
            for (;;) {
                long basePtr = mapIndexA * MAP_ENTRY_SIZE_BYTES + map;
                long lenPtr = basePtr + LEN_OFFSET;
                long namePtr = basePtr + NAME_OFFSET;
                int len = UNSAFE.getInt(lenPtr);
                if (len == lenA) {
                    namePtr = UNSAFE.getLong(basePtr + NAME_OFFSET);
                    if (nameMatch(startPtr, maskedWord, namePtr, lenLong)) {
                        return basePtr;
                    }
                }
                else if (len == 0) {
                    // todo: uncommon branch maybe?
                    // empty slot
                    UNSAFE.putLong(namePtr, currentNamesPtr);
                    UNSAFE.putInt(lenPtr, lenA);
                    // todo: this could be a single putLong()
                    UNSAFE.putInt(basePtr + MAX_OFFSET, Integer.MIN_VALUE);
                    UNSAFE.putInt(basePtr + MIN_OFFSET, Integer.MAX_VALUE);
                    UNSAFE.copyMemory(startPtr, currentNamesPtr, lenA);
                    long consume = (lenLong & ~7L) + 8;
                    currentNamesPtr += consume;
                    assert currentNamesPtr <= namesHi;
                    return basePtr;
                }
                mapIndexA = ++mapIndexA & MAP_MASK;
            }
        }

        private static boolean nameMatch(long startA, long maskedWordA, long namePtr, long len) {
            // long namePtr = basePtr + NAME_OFFSET;
            long fullLen = len & ~7L;
            long offset;

            // todo: this is worth exploring further.
            // @mtopolnik has an interesting algo with 2 unconditioned long loads: this is sufficient
            // for majority of names. so we would be left with just a single branch which is almost never taken?
            for (offset = 0; offset < fullLen; offset += 8) {
                if (UNSAFE.getLong(startA + offset) != UNSAFE.getLong(namePtr + offset)) {
                    return false;
                }
            }

            long maskedWordInMap = UNSAFE.getLong(namePtr + fullLen);
            return (maskedWordInMap == maskedWordA);
        }
    }

}
