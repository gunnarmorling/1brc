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
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class CalculateAverage_mtopolnik {
    private static final Unsafe UNSAFE = unsafe();
    private static final int MAX_NAME_LEN = 100;
    private static final int STATS_TABLE_SIZE = 1 << 16;
    private static final int TABLE_INDEX_MASK = STATS_TABLE_SIZE - 1;
    private static final String MEASUREMENTS_TXT = "measurements.txt";
    private static final byte SEMICOLON = ';';
    private static final long BROADCAST_SEMICOLON = broadcastByte(SEMICOLON);

    // These two are just informative, I let the IDE calculate them for me
    private static final long NATIVE_MEM_PER_THREAD = StatsAccessor.SIZEOF * STATS_TABLE_SIZE;
    private static final long NATIVE_MEM_ON_8_THREADS = 8 * NATIVE_MEM_PER_THREAD;

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

    static class StationStats implements Comparable<StationStats> {
        String name;
        long sum;
        int count;
        int min;
        int max;

        @Override
        public String toString() {
            return String.format("%s=%.1f/%.1f/%.1f", name, min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }

    public static void main(String[] args) throws Exception {
        calculate();
    }

    static void calculate() throws Exception {
        final File file = new File(MEASUREMENTS_TXT);
        final long length = file.length();
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final var results = new StationStats[chunkCount][];
        final var chunkStartOffsets = new long[chunkCount];
        try (var raf = new RandomAccessFile(file, "r")) {
            final var inputBase = raf.getChannel().map(MapMode.READ_ONLY, 0, length, Arena.global()).address();
            for (int i = 1; i < chunkStartOffsets.length; i++) {
                var start = length * i / chunkStartOffsets.length;
                raf.seek(start);
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }
            var threads = new Thread[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(inputBase + chunkStart, inputBase + chunkLimit, results, i));
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        }
        mergeSortAndPrint(results);
    }

    private static class ChunkProcessor implements Runnable {
        private static final long NAMEBUF_SIZE = 2 * Long.BYTES;
        private static final int CACHELINE_SIZE = 64;

        private final long inputBase;
        private final long inputSize;
        private final StationStats[][] results;
        private final int myIndex;

        private StatsAccessor stats;
        private long nameBufBase;
        private long cursor;

        ChunkProcessor(long chunkStart, long chunkLimit, StationStats[][] results, int myIndex) {
            this.inputBase = chunkStart;
            this.inputSize = chunkLimit - chunkStart;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            try (Arena confinedArena = Arena.ofConfined()) {
                long totalAllocated = 0;
                String threadName = Thread.currentThread().getName();
                long statsByteSize = STATS_TABLE_SIZE * StatsAccessor.SIZEOF;
                var diagnosticString = String.format("Thread %s needs %,d bytes, managed to allocate before OOM: ",
                        threadName, statsByteSize + NAMEBUF_SIZE);
                try {
                    stats = new StatsAccessor(confinedArena.allocate(statsByteSize, CACHELINE_SIZE));
                    totalAllocated = statsByteSize;
                    nameBufBase = confinedArena.allocate(NAMEBUF_SIZE).address();
                }
                catch (OutOfMemoryError e) {
                    System.err.print(diagnosticString);
                    System.err.println(totalAllocated);
                    throw e;
                }
                processChunk();
                exportResults();
            }
        }

        private void processChunk() {
            while (cursor < inputSize) {
                long word1;
                long word2;
                if (cursor + 2 * Long.BYTES <= inputSize) {
                    word1 = UNSAFE.getLong(inputBase + cursor);
                    word2 = UNSAFE.getLong(inputBase + cursor + Long.BYTES);
                }
                else {
                    UNSAFE.putLong(nameBufBase, 0);
                    UNSAFE.putLong(nameBufBase + Long.BYTES, 0);
                    UNSAFE.copyMemory(inputBase + cursor, nameBufBase, Long.min(NAMEBUF_SIZE, inputSize - cursor));
                    word1 = UNSAFE.getLong(nameBufBase);
                    word2 = UNSAFE.getLong(nameBufBase + Long.BYTES);
                }
                long posOfSemicolon = posOfSemicolon(word1, word2);
                word1 = maskWord(word1, posOfSemicolon - cursor);
                word2 = maskWord(word2, posOfSemicolon - cursor - Long.BYTES);
                long hash = hash(word1);
                long namePos = cursor;
                long nameLen = posOfSemicolon - cursor;
                assert nameLen <= 100 : "nameLen > 100";
                int temperature = parseTemperatureAndAdvanceCursor(posOfSemicolon);
                updateStats(hash, namePos, nameLen, word1, word2, temperature);
            }
        }

        private void updateStats(long hash, long namePos, long nameLen, long nameWord1, long nameWord2, int temperature) {
            int tableIndex = (int) (hash & TABLE_INDEX_MASK);
            while (true) {
                stats.gotoIndex(tableIndex);
                if (stats.hash() == hash && stats.nameLen() == nameLen
                        && nameEquals(stats.nameAddress(), inputBase + namePos, nameLen, nameWord1, nameWord2)) {
                    stats.setSum(stats.sum() + temperature);
                    stats.setCount(stats.count() + 1);
                    stats.setMin((short) Integer.min(stats.min(), temperature));
                    stats.setMax((short) Integer.max(stats.max(), temperature));
                    return;
                }
                if (stats.nameLen() != 0) {
                    tableIndex = (tableIndex + 1) & TABLE_INDEX_MASK;
                    continue;
                }
                stats.setHash(hash);
                stats.setNameLen((int) nameLen);
                stats.setSum(temperature);
                stats.setCount(1);
                stats.setMin((short) temperature);
                stats.setMax((short) temperature);
                UNSAFE.copyMemory(inputBase + namePos, stats.nameAddress(), nameLen);
                return;
            }
        }

        private int parseTemperatureAndAdvanceCursor(long semicolonPos) {
            long startOffset = semicolonPos + 1;
            if (startOffset <= inputSize - Long.BYTES) {
                return parseTemperatureSwarAndAdvanceCursor(startOffset);
            }
            return parseTemperatureSimpleAndAdvanceCursor(startOffset);
        }

        // Credit: merykitty
        private int parseTemperatureSwarAndAdvanceCursor(long startOffset) {
            long word = UNSAFE.getLong(inputBase + startOffset);
            final long negated = ~word;
            final int dotPos = Long.numberOfTrailingZeros(negated & 0x10101000);
            final long signed = (negated << 59) >> 63;
            final long removeSignMask = ~(signed & 0xFF);
            final long digits = ((word & removeSignMask) << (28 - dotPos)) & 0x0F000F0F00L;
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            final int temperature = (int) ((absValue ^ signed) - signed);
            cursor = startOffset + (dotPos / 8) + 3;
            return temperature;
        }

        private int parseTemperatureSimpleAndAdvanceCursor(long startOffset) {
            final byte minus = (byte) '-';
            final byte zero = (byte) '0';
            final byte dot = (byte) '.';

            // Temperature plus the following newline is at least 4 chars, so this is always safe:
            int fourCh = UNSAFE.getInt(inputBase + startOffset);
            final int mask = 0xFF;
            byte ch = (byte) (fourCh & mask);
            int shift = 0;
            int temperature;
            int sign;
            if (ch == minus) {
                sign = -1;
                shift += 8;
                ch = (byte) ((fourCh & (mask << shift)) >>> shift);
            }
            else {
                sign = 1;
            }
            temperature = ch - zero;
            shift += 8;
            ch = (byte) ((fourCh & (mask << shift)) >>> shift);
            if (ch == dot) {
                shift += 8;
                ch = (byte) ((fourCh & (mask << shift)) >>> shift);
            }
            else {
                temperature = 10 * temperature + (ch - zero);
                shift += 16;
                // The last character may be past the four loaded bytes, load it from memory.
                // Checking that with another `if` is self-defeating for performance.
                ch = UNSAFE.getByte(inputBase + startOffset + (shift / 8));
            }
            temperature = 10 * temperature + (ch - zero);
            // `shift` holds the number of bits in the temperature field.
            // A newline character follows the temperature, and so we advance
            // the cursor past the newline to the start of the next line.
            cursor = startOffset + (shift / 8) + 2;
            return sign * temperature;
        }

        private static long hash(long word1) {
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 17;

            long hash = word1;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            // hash ^= word2;
            // hash *= seed;
            // hash = Long.rotateLeft(hash, rotDist);
            return hash;
        }

        private static boolean nameEquals(long statsAddr, long inputAddr, long len, long inputWord1, long inputWord2) {
            boolean mismatch1 = maskWord(inputWord1, len) != UNSAFE.getLong(statsAddr);
            boolean mismatch2 = maskWord(inputWord2, len - Long.BYTES) != UNSAFE.getLong(statsAddr + Long.BYTES);
            if (mismatch1 | mismatch2) {
                return false;
            }
            for (int i = 2 * Long.BYTES; i < len; i++) {
                if (UNSAFE.getByte(inputAddr + i) != UNSAFE.getByte(statsAddr + i)) {
                    return false;
                }
            }
            return true;
        }

        private static long maskWord(long word, long len) {
            long halfShiftDistance = Long.max(0, Long.BYTES - len) << 2;
            long mask = (~0L >>> halfShiftDistance) >>> halfShiftDistance; // avoid Java trap of shiftDist % 64
            return word & mask;
        }

        private static final long BROADCAST_0x01 = broadcastByte(0x01);
        private static final long BROADCAST_0x80 = broadcastByte(0x80);

        // Adapted from https://jameshfisher.com/2017/01/24/bitwise-check-for-zero-byte/
        // and https://github.com/ashvardanian/StringZilla/blob/14e7a78edcc16b031c06b375aac1f66d8f19d45a/stringzilla/stringzilla.h#L139-L169
        long posOfSemicolon(long word1, long word2) {
            long diff = word1 ^ BROADCAST_SEMICOLON;
            long matchBits1 = (diff - BROADCAST_0x01) & ~diff & BROADCAST_0x80;
            diff = word2 ^ BROADCAST_SEMICOLON;
            long matchBits2 = (diff - BROADCAST_0x01) & ~diff & BROADCAST_0x80;
            if ((matchBits1 | matchBits2) != 0) {
                int trailing1 = Long.numberOfTrailingZeros(matchBits1);
                int match1IsNonZero = trailing1 & 63;
                match1IsNonZero |= match1IsNonZero >>> 3;
                match1IsNonZero |= match1IsNonZero >>> 1;
                match1IsNonZero |= match1IsNonZero >>> 1;
                // Now match1IsNonZero is 1 if it's non-zero, else 0. Use it to
                // raise the lowest bit in traling2 if trailing1 is nonzero. This forces
                // trailing2 to be zero if trailing1 is non-zero.
                int trailing2 = Long.numberOfTrailingZeros(matchBits2 | match1IsNonZero) & 63;
                return cursor + ((trailing1 | trailing2) >> 3);
            }
            long offset = cursor + 2 * Long.BYTES;
            for (; offset <= inputSize - Long.BYTES; offset += Long.BYTES) {
                var block = UNSAFE.getLong(inputBase + offset);
                diff = block ^ BROADCAST_SEMICOLON;
                long matchBits = (diff - BROADCAST_0x01) & ~diff & BROADCAST_0x80;
                if (matchBits != 0) {
                    return offset + Long.numberOfTrailingZeros(matchBits) / 8;
                }
            }
            return posOfSemicolonSimple(offset);
        }

        private long posOfSemicolonSimple(long offset) {
            for (; offset < inputSize; offset++) {
                if (UNSAFE.getByte(inputBase + offset) == SEMICOLON) {
                    return offset;
                }
            }
            throw new RuntimeException("Semicolon not found");
        }

        // Copies the results from native memory to Java heap and puts them into the results array.
        private void exportResults() {
            var exportedStats = new ArrayList<StationStats>(10_000);
            for (int i = 0; i < STATS_TABLE_SIZE; i++) {
                stats.gotoIndex(i);
                if (stats.nameLen() == 0) {
                    continue;
                }
                var sum = stats.sum();
                var count = stats.count();
                var min = stats.min();
                var max = stats.max();
                var name = stats.exportNameString();
                var stationStats = new StationStats();
                stationStats.name = name;
                stationStats.sum = sum;
                stationStats.count = count;
                stationStats.min = min;
                stationStats.max = max;
                exportedStats.add(stationStats);
            }
            StationStats[] exported = exportedStats.toArray(new StationStats[0]);
            Arrays.sort(exported);
            results[myIndex] = exported;
        }

        private final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());

        private String longToString(long word) {
            buf.clear();
            buf.putLong(word);
            return new String(buf.array(), StandardCharsets.UTF_8); // + "|" + Arrays.toString(buf.array());
        }
    }

    private static long broadcastByte(int b) {
        long nnnnnnnn = b;
        nnnnnnnn |= nnnnnnnn << 8;
        nnnnnnnn |= nnnnnnnn << 16;
        nnnnnnnn |= nnnnnnnn << 32;
        return nnnnnnnn;
    }

    static class StatsAccessor {
        static final int NAME_SLOT_SIZE = 104;
        static final long HASH_OFFSET = 0;
        static final long NAMELEN_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long SUM_OFFSET = NAMELEN_OFFSET + Integer.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Short.BYTES;
        static final long NAME_OFFSET = MAX_OFFSET + Short.BYTES;
        static final long SIZEOF = (NAME_OFFSET + NAME_SLOT_SIZE - 1) / 8 * 8 + 8;

        static final int ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

        private final long address;
        private long slotBase;

        StatsAccessor(MemorySegment memSeg) {
            memSeg.fill((byte) 0);
            this.address = memSeg.address();
        }

        void gotoIndex(int index) {
            slotBase = address + index * SIZEOF;
        }

        long hash() {
            return UNSAFE.getLong(slotBase + HASH_OFFSET);
        }

        int nameLen() {
            return UNSAFE.getInt(slotBase + NAMELEN_OFFSET);
        }

        int sum() {
            return UNSAFE.getInt(slotBase + SUM_OFFSET);
        }

        int count() {
            return UNSAFE.getInt(slotBase + COUNT_OFFSET);
        }

        short min() {
            return UNSAFE.getShort(slotBase + MIN_OFFSET);
        }

        short max() {
            return UNSAFE.getShort(slotBase + MAX_OFFSET);
        }

        long nameAddress() {
            return slotBase + NAME_OFFSET;
        }

        String exportNameString() {
            final var bytes = new byte[nameLen()];
            UNSAFE.copyMemory(null, nameAddress(), bytes, ARRAY_BASE_OFFSET, nameLen());
            return new String(bytes, StandardCharsets.UTF_8);
        }

        void setHash(long hash) {
            UNSAFE.putLong(slotBase + HASH_OFFSET, hash);
        }

        void setNameLen(int nameLen) {
            UNSAFE.putInt(slotBase + NAMELEN_OFFSET, nameLen);
        }

        void setSum(int sum) {
            UNSAFE.putInt(slotBase + SUM_OFFSET, sum);
        }

        void setCount(int count) {
            UNSAFE.putInt(slotBase + COUNT_OFFSET, count);
        }

        void setMin(short min) {
            UNSAFE.putShort(slotBase + MIN_OFFSET, min);
        }

        void setMax(short max) {
            UNSAFE.putShort(slotBase + MAX_OFFSET, max);
        }
    }

    private static void mergeSortAndPrint(StationStats[][] results) {
        var onFirst = true;
        System.out.print('{');
        var cursors = new int[results.length];
        var indexOfMin = 0;
        StationStats curr = null;
        int exhaustedCount;
        while (true) {
            exhaustedCount = 0;
            StationStats min = null;
            for (int i = 0; i < cursors.length; i++) {
                if (cursors[i] == results[i].length) {
                    exhaustedCount++;
                    continue;
                }
                StationStats candidate = results[i][cursors[i]];
                if (min == null || min.compareTo(candidate) > 0) {
                    indexOfMin = i;
                    min = candidate;
                }
            }
            if (exhaustedCount == cursors.length) {
                if (!onFirst) {
                    System.out.print(", ");
                }
                System.out.print(curr);
                break;
            }
            cursors[indexOfMin]++;
            if (curr == null) {
                curr = min;
            }
            else if (min.equals(curr)) {
                curr.sum += min.sum;
                curr.count += min.count;
                curr.min = Integer.min(curr.min, min.min);
                curr.max = Integer.max(curr.max, min.max);
            }
            else {
                if (onFirst) {
                    onFirst = false;
                }
                else {
                    System.out.print(", ");
                }
                System.out.print(curr);
                curr = min;
            }
        }
        System.out.println('}');
    }
}
