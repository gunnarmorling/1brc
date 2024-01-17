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
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_jerrinot {
    private static final Unsafe UNSAFE = unsafe();
    private static final String MEASUREMENTS_TXT = "measurements.txt";
    // todo: with hyper-threading enable we would be better of with availableProcessors / 2;
    // todo: validate the testing env. params.
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
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

            for (int i = 0; i < THREAD_COUNT; i++) {
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

            var accumulator = new TreeMap<String, Processor.StationStats>();
            for (int i = 0; i < THREAD_COUNT; i++) {
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
        private static final int MAP_SLOT_COUNT = ceilPow2(10000);
        private static final int STATION_MAX_NAME_BYTES = 104;

        private static final long COUNT_OFFSET = 0;
        private static final long MIN_OFFSET = 4;
        private static final long MAX_OFFSET = 8;
        private static final long SUM_OFFSET = 12;
        private static final long LEN_OFFSET = 20;
        private static final long NAME_OFFSET = 24;

        private static final int MAP_ENTRY_SIZE_BYTES = +Integer.BYTES // count // 0
                + Integer.BYTES // min // +4
                + Integer.BYTES // max // +8
                + Long.BYTES // sum // +12
                + Integer.BYTES // station name len // +20
                + STATION_MAX_NAME_BYTES; // +24

        private static final int MAP_SIZE_BYTES = MAP_SLOT_COUNT * MAP_ENTRY_SIZE_BYTES;
        private static final long MAP_MASK = MAP_SLOT_COUNT - 1;

        // todo: some fields could probably be converted to locals

        private final long map;

        private long cursorA;
        private long endA;
        private long cursorB;
        private long endB;
        private long cursorC;
        private long endC;
        private long cursorD;
        private long endD;
        private long maskA;
        private long maskB;
        private long maskC;
        private long maskD;

        // credit: merykitty
        private long parseAndStoreTemperature(long startCursor, long baseEntryPtr) {
            long word = UNSAFE.getLong(startCursor);
            final long negateda = ~word;
            final int dotPos = Long.numberOfTrailingZeros(negateda & 0x10101000);
            final long signed = (negateda << 59) >> 63;
            final long removeSignMask = ~(signed & 0xFF);
            final long digits = ((word & removeSignMask) << (28 - dotPos)) & 0x0F000F0F00L;
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            final int temperature = (int) ((absValue ^ signed) - signed);

            long countPtr = baseEntryPtr + COUNT_OFFSET;
            long minPtr = baseEntryPtr + MIN_OFFSET;
            long maxPtr = baseEntryPtr + MAX_OFFSET;
            long sumPtr = baseEntryPtr + SUM_OFFSET;

            int min = UNSAFE.getInt(minPtr);
            int max = UNSAFE.getInt(maxPtr);
            long sum = UNSAFE.getLong(sumPtr);
            // try if min/max intrinsics are paying off
            // maybe braching is better? the branch is becoming more predictable with
            // each new sample.
            max = Math.max(max, temperature);
            min = Math.min(min, temperature);
            sum += temperature;
            UNSAFE.putInt(countPtr, UNSAFE.getInt(countPtr) + 1);
            UNSAFE.putInt(minPtr, min);
            UNSAFE.putInt(maxPtr, max);
            UNSAFE.putLong(sumPtr, sum);
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
                long baseNameAddr = baseAddress + NAME_OFFSET;
                for (int i = 0; i < len; i++) {
                    nameArr[i] = UNSAFE.getByte(baseNameAddr + i);
                }
                String name = new String(nameArr);
                int min = UNSAFE.getInt(baseAddress + MIN_OFFSET);
                int max = UNSAFE.getInt(baseAddress + MAX_OFFSET);
                int count = UNSAFE.getInt(baseAddress + COUNT_OFFSET);
                long sum = UNSAFE.getLong(baseAddress + SUM_OFFSET);

                // todo: lambdas bootstrap probably cost us
                accumulator.compute(name, (_, v) -> {
                    if (v == null) {
                        return new StationStats(min, max, count, sum);
                    }
                    return new StationStats(Math.min(v.min, min), Math.max(v.max, max), v.count + count, v.sum + sum);
                });
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

            int i;
            for (i = 0; i < MAP_SIZE_BYTES; i += 8) {
                UNSAFE.putLong(map + i, 0);
            }
            for (i = i - 8; i < MAP_SIZE_BYTES; i++) {
                UNSAFE.putByte(map + i, (byte) 0);
            }
        }

        private void doTail() {
            // todo: we would be probably better of without all that code dup. ("compilers hates him!")
            // System.out.println("done ILP");
            while (cursorA < endA) {
                long startA = cursorA;
                long delimiterWordA = UNSAFE.getLong(cursorA);
                long hashA = 0;
                maskA = getDelimiterMask(delimiterWordA);
                while (maskA == 0) {
                    hashA ^= delimiterWordA;
                    cursorA += 8;
                    delimiterWordA = UNSAFE.getLong(cursorA);
                    maskA = getDelimiterMask(delimiterWordA);
                }
                final int delimiterByteA = Long.numberOfTrailingZeros(maskA);
                final long semicolonA = cursorA + (delimiterByteA >> 3);
                final long maskedWordA = delimiterWordA & ((maskA >>> 7) - 1);
                hashA ^= maskedWordA;
                int intHashA = (int) (hashA ^ (hashA >> 32));
                intHashA = intHashA ^ (intHashA >> 17);

                long baseEntryPtrA = getOrCreateEntryBaseOffset(semicolonA, startA, intHashA, maskedWordA);
                cursorA = parseAndStoreTemperature(semicolonA + 1, baseEntryPtrA);
            }
            // System.out.println("done A");
            while (cursorB < endB) {
                long startB = cursorB;
                long delimiterWordB = UNSAFE.getLong(cursorB);
                long hashB = 0;
                maskB = getDelimiterMask(delimiterWordB);
                while (maskB == 0) {
                    hashB ^= delimiterWordB;
                    cursorB += 8;
                    delimiterWordB = UNSAFE.getLong(cursorB);
                    maskB = getDelimiterMask(delimiterWordB);
                }
                final int delimiterByteB = Long.numberOfTrailingZeros(maskB);
                final long semicolonB = cursorB + (delimiterByteB >> 3);
                final long maskedWordB = delimiterWordB & ((maskB >>> 7) - 1);
                hashB ^= maskedWordB;
                int intHashB = (int) (hashB ^ (hashB >> 32));
                intHashB = intHashB ^ (intHashB >> 17);

                long baseEntryPtrB = getOrCreateEntryBaseOffset(semicolonB, startB, intHashB, maskedWordB);
                cursorB = parseAndStoreTemperature(semicolonB + 1, baseEntryPtrB);
            }
            // System.out.println("done B");
            while (cursorC < endC) {
                long startC = cursorC;
                long delimiterWordC = UNSAFE.getLong(cursorC);
                long hashC = 0;
                maskC = getDelimiterMask(delimiterWordC);
                while (maskC == 0) {
                    hashC ^= delimiterWordC;
                    cursorC += 8;
                    delimiterWordC = UNSAFE.getLong(cursorC);
                    maskC = getDelimiterMask(delimiterWordC);
                }
                final int delimiterByteC = Long.numberOfTrailingZeros(maskC);
                final long semicolonC = cursorC + (delimiterByteC >> 3);
                final long maskedWordC = delimiterWordC & ((maskC >>> 7) - 1);
                hashC ^= maskedWordC;
                int intHashC = (int) (hashC ^ (hashC >> 32));
                intHashC = intHashC ^ (intHashC >> 17);

                long baseEntryPtrC = getOrCreateEntryBaseOffset(semicolonC, startC, intHashC, maskedWordC);
                cursorC = parseAndStoreTemperature(semicolonC + 1, baseEntryPtrC);
            }
            // System.out.println("done C");
            while (cursorD < endD) {
                long startD = cursorD;
                long delimiterWordD = UNSAFE.getLong(cursorD);
                long hashD = 0;
                maskD = getDelimiterMask(delimiterWordD);
                while (maskD == 0) {
                    hashD ^= delimiterWordD;
                    cursorD += 8;
                    delimiterWordD = UNSAFE.getLong(cursorD);
                    maskD = getDelimiterMask(delimiterWordD);
                }
                final int delimiterByteD = Long.numberOfTrailingZeros(maskD);
                final long semicolonD = cursorD + (delimiterByteD >> 3);
                final long maskedWordD = delimiterWordD & ((maskD >>> 7) - 1);
                hashD ^= maskedWordD;
                int intHashD = (int) (hashD ^ (hashD >> 32));
                intHashD = intHashD ^ (intHashD >> 17);

                long baseEntryPtrD = getOrCreateEntryBaseOffset(semicolonD, startD, intHashD, maskedWordD);
                cursorD = parseAndStoreTemperature(semicolonD + 1, baseEntryPtrD);
            }
            // System.out.println("done D");
        }

        @Override
        public void run() {
            while (cursorA < endA && cursorB < endB && cursorC < endC && cursorD < endD) {
                // todo: experiment with different inter-leaving
                long startA = cursorA;
                long startB = cursorB;
                long startC = cursorC;
                long startD = cursorD;

                long delimiterWordA = UNSAFE.getLong(cursorA);
                long delimiterWordB = UNSAFE.getLong(cursorB);
                long delimiterWordC = UNSAFE.getLong(cursorC);
                long delimiterWordD = UNSAFE.getLong(cursorD);

                long hashA = 0;
                long hashB = 0;
                long hashC = 0;
                long hashD = 0;

                // credits for the hashing idea: royvanrijn
                maskA = getDelimiterMask(delimiterWordA);
                while (maskA == 0) {
                    hashA ^= delimiterWordA;
                    cursorA += 8;
                    delimiterWordA = UNSAFE.getLong(cursorA);
                    maskA = getDelimiterMask(delimiterWordA);
                }
                final int delimiterByteA = Long.numberOfTrailingZeros(maskA);
                final long semicolonA = cursorA + (delimiterByteA >> 3);
                final long maskedWordA = delimiterWordA & ((maskA >>> 7) - 1);
                hashA ^= maskedWordA;
                int intHashA = (int) (hashA ^ (hashA >> 32));
                intHashA = intHashA ^ (intHashA >> 17);

                maskB = getDelimiterMask(delimiterWordB);
                while (maskB == 0) {
                    hashB ^= delimiterWordB;
                    cursorB += 8;
                    delimiterWordB = UNSAFE.getLong(cursorB);
                    maskB = getDelimiterMask(delimiterWordB);
                }
                final int delimiterByteB = Long.numberOfTrailingZeros(maskB);
                final long semicolonB = cursorB + (delimiterByteB >> 3);
                final long maskedWordB = delimiterWordB & ((maskB >>> 7) - 1);
                hashB ^= maskedWordB;
                int intHashB = (int) (hashB ^ (hashB >> 32));
                intHashB = intHashB ^ (intHashB >> 17);

                maskC = getDelimiterMask(delimiterWordC);
                while (maskC == 0) {
                    hashC ^= delimiterWordC;
                    cursorC += 8;
                    delimiterWordC = UNSAFE.getLong(cursorC);
                    maskC = getDelimiterMask(delimiterWordC);
                }
                final int delimiterByteC = Long.numberOfTrailingZeros(maskC);
                final long semicolonC = cursorC + (delimiterByteC >> 3);
                final long maskedWordC = delimiterWordC & ((maskC >>> 7) - 1);
                hashC ^= maskedWordC;
                int intHashC = (int) (hashC ^ (hashC >> 32));
                intHashC = intHashC ^ (intHashC >> 17);

                maskD = getDelimiterMask(delimiterWordD);
                while (maskD == 0) {
                    hashD ^= delimiterWordD;
                    cursorD += 8;
                    delimiterWordD = UNSAFE.getLong(cursorD);
                    maskD = getDelimiterMask(delimiterWordD);
                }
                final int delimiterByteD = Long.numberOfTrailingZeros(maskD);
                final long semicolonD = cursorD + (delimiterByteD >> 3);
                final long maskedWordD = delimiterWordD & ((maskD >>> 7) - 1);
                hashD ^= maskedWordD;
                int intHashD = (int) (hashD ^ (hashD >> 32));
                intHashD = intHashD ^ (intHashD >> 17);

                long baseEntryPtrA = getOrCreateEntryBaseOffset(semicolonA, startA, intHashA, maskedWordA);
                long baseEntryPtrB = getOrCreateEntryBaseOffset(semicolonB, startB, intHashB, maskedWordB);
                long baseEntryPtrC = getOrCreateEntryBaseOffset(semicolonC, startC, intHashC, maskedWordC);
                long baseEntryPtrD = getOrCreateEntryBaseOffset(semicolonD, startD, intHashD, maskedWordD);

                cursorA = parseAndStoreTemperature(semicolonA + 1, baseEntryPtrA);
                cursorB = parseAndStoreTemperature(semicolonB + 1, baseEntryPtrB);
                cursorC = parseAndStoreTemperature(semicolonC + 1, baseEntryPtrC);
                cursorD = parseAndStoreTemperature(semicolonD + 1, baseEntryPtrD);
            }
            doTail();
        }

        private long getOrCreateEntryBaseOffset(long semicolonA, long startA, int intHashA, long maskedWordA) {
            int lenA = (int) (semicolonA - startA);
            long mapIndexA = intHashA & MAP_MASK;
            for (;;) {
                long basePtr = mapIndexA * MAP_ENTRY_SIZE_BYTES + map;
                long lenPtr = basePtr + LEN_OFFSET;
                int len = UNSAFE.getInt(lenPtr);
                if (len == 0) {
                    // todo: uncommon branch maybe?
                    // empty slot
                    UNSAFE.copyMemory(semicolonA - lenA, basePtr + NAME_OFFSET, lenA);
                    UNSAFE.putInt(lenPtr, lenA);
                    UNSAFE.putInt(basePtr + MAX_OFFSET, Integer.MIN_VALUE);
                    UNSAFE.putInt(basePtr + MIN_OFFSET, Integer.MAX_VALUE);
                    return basePtr;
                }
                if (len == lenA) {
                    boolean match = true;
                    long namePtr = basePtr + NAME_OFFSET;
                    int fullLen = (len >> 3) << 3;
                    long offset;
                    // todo: this is worth exploring further.
                    // @mtopolnik has an interesting algo with 2 unconditioned long loads: this is sufficient
                    // for majority of names. so we would be left with just a single branch which is almost never taken?
                    for (offset = 0; offset < fullLen; offset += 8) {
                        match &= (UNSAFE.getLong(startA + offset) == UNSAFE.getLong(namePtr + offset));
                    }

                    long maskedWordInMap = UNSAFE.getLong(namePtr + offset);
                    match &= (maskedWordInMap == maskedWordA);

                    if (match) {
                        return basePtr;
                    }
                }
                mapIndexA = ++mapIndexA & MAP_MASK;
            }
        }
    }

}
