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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

// create_measurements3.sh 500_000_000
// initial:    real	0m11.640s user	0m39.766s sys	0m9.852s
// short hash: real	0m11.241s user	0m36.534s sys	0m9.700s

public class CalculateAverage_mtopolnik {
    private static final Unsafe UNSAFE = unsafe();
    private static final boolean ORDER_IS_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    private static final int MAX_NAME_LEN = 100;
    private static final int NAME_SLOT_SIZE = 104;
    private static final int STATS_TABLE_SIZE = 1 << 16;
    private static final long NATIVE_MEM_PER_THREAD = (NAME_SLOT_SIZE + StatsAccessor.SIZEOF) * STATS_TABLE_SIZE;
    private static final long NATIVE_MEM_ON_8_THREADS = 8 * NATIVE_MEM_PER_THREAD;
    private static final String MEASUREMENTS_TXT = "measurements.txt";
    private static final byte SEMICOLON = (byte) ';';
    private static final long BROADCAST_SEMICOLON = broadcastSemicolon();

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

    static class StationStats {
        String name;
        long sum;
        int count;
        int min;
        int max;

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }
    }

    public static void main(String[] args) throws Exception {
        calculate();
    }

    static void calculate() throws Exception {
        final File file = new File(MEASUREMENTS_TXT);
        final long length = file.length();
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final var results = new StationStats[(int) chunkCount][];
        final var chunkStartOffsets = new long[chunkCount];
        try (var raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < chunkStartOffsets.length; i++) {
                var start = length * i / chunkStartOffsets.length;
                raf.seek(start);
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }
            var threads = new Thread[(int) chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(raf, chunkStart, chunkLimit, results, i));
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        }
        var totals = new TreeMap<String, StationStats>();
        for (var chunkResults : results) {
            for (var stats : chunkResults) {
                var prev = totals.putIfAbsent(stats.name, stats);
                if (prev != null) {
                    prev.sum += stats.sum;
                    prev.count += stats.count;
                    prev.min = Integer.min(prev.min, stats.min);
                    prev.max = Integer.max(prev.max, stats.max);
                }
            }
        }
        System.out.println(totals);
    }

    private static class ChunkProcessor implements Runnable {
        private static final long HASHBUF_SIZE = 8;

        private final long chunkStart;
        private final long chunkLimit;
        private final RandomAccessFile raf;
        private final StationStats[][] results;
        private final int myIndex;

        private StatsAccessor stats;
        private long inputBase;
        private long inputSize;
        private long hashBufBase;
        private long cursor;

        ChunkProcessor(RandomAccessFile raf, long chunkStart, long chunkLimit, StationStats[][] results, int myIndex) {
            this.raf = raf;
            this.chunkStart = chunkStart;
            this.chunkLimit = chunkLimit;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            try (Arena confinedArena = Arena.ofConfined()) {
                final var inputMem = raf.getChannel().map(MapMode.READ_ONLY, chunkStart, chunkLimit - chunkStart, confinedArena);
                inputBase = inputMem.address();
                inputSize = inputMem.byteSize();
                stats = new StatsAccessor(confinedArena.allocate(STATS_TABLE_SIZE * StatsAccessor.SIZEOF, Long.BYTES));
                hashBufBase = confinedArena.allocate(HASHBUF_SIZE).address();
                processChunk();
                exportResults();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void processChunk() {
            while (cursor < inputSize) {
                long semicolonPos = bytePosOfSemicolon(cursor);
                final long hash = hash(semicolonPos);
                long nameLen = semicolonPos - cursor;
                assert nameLen <= 100 : "nameLen > 100";
                final long namePos = cursor;
                int temperature = parseTemperatureAndAdvanceCursor(semicolonPos);
                updateStats(hash, nameLen, temperature, namePos);
            }
        }

        private void updateStats(long hash, long nameLen, int temperature, long namePos) {
            int tableIndex = (int) (hash % STATS_TABLE_SIZE);
            while (true) {
                stats.gotoIndex(tableIndex);
                long foundHash = stats.hash();
                if (foundHash == hash && stats.nameLen() == nameLen
                        && strcmp(stats.nameAddress(), inputBase + namePos, nameLen)) {
                    stats.setSum(stats.sum() + temperature);
                    stats.setCount(stats.count() + 1);
                    stats.setMin((short) Integer.min(stats.min(), temperature));
                    stats.setMax((short) Integer.max(stats.max(), temperature));
                    return;
                }
                if (foundHash != 0) {
                    tableIndex = (tableIndex + 1) % STATS_TABLE_SIZE;
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
            if (ORDER_IS_BIG_ENDIAN) {
                word = Long.reverseBytes(word);
            }
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
            if (ORDER_IS_BIG_ENDIAN) {
                fourCh = Integer.reverseBytes(fourCh);
            }
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

        private long hash(long limit) {
            long n1;
            if (cursor <= inputSize - Long.BYTES) {
                n1 = UNSAFE.getLong(inputBase + cursor);
                long nameSize = limit - cursor;
                long shiftDistance = 8 * Long.max(0, Long.BYTES - nameSize);
                long mask = ~0L;
                if (!ORDER_IS_BIG_ENDIAN) {
                    mask >>>= shiftDistance;
                }
                else {
                    mask <<= shiftDistance;
                }
                n1 &= mask;
            }
            else {
                UNSAFE.putLong(hashBufBase, 0);
                // UNSAFE.putLong(hashBufBase + Long.BYTES, 0);
                UNSAFE.copyMemory(inputBase + cursor, hashBufBase, Long.min(HASHBUF_SIZE, limit - cursor));
                n1 = UNSAFE.getLong(hashBufBase);
                // long n2 = UNSAFE.getLong(hashBufBase + Long.BYTES);
            }
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 19;
            long hash = n1;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            // hash ^= n2;
            // hash *= seed;
            // hash = Long.rotateLeft(hash, rotDist);
            return hash != 0 ? hash & (~Long.MIN_VALUE) : 1;
        }

        // Copies the results from native memory to Java heap and puts them into the results array.
        private void exportResults() {
            var exportedStats = new ArrayList<StationStats>(10_000);
            for (int i = 0; i < STATS_TABLE_SIZE; i++) {
                stats.gotoIndex(i);
                if (stats.hash() == 0) {
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
            results[myIndex] = exportedStats.toArray(new StationStats[0]);
        }

        long bytePosOfSemicolon(long offset) {
            return !ORDER_IS_BIG_ENDIAN
                    ? bytePosLittleEndian(offset)
                    : bytePosBigEndian(offset);
        }

        // Adapted from https://jameshfisher.com/2017/01/24/bitwise-check-for-zero-byte/
        // and https://github.com/ashvardanian/StringZilla/blob/14e7a78edcc16b031c06b375aac1f66d8f19d45a/stringzilla/stringzilla.h#L139-L169
        long bytePosLittleEndian(long offset) {
            final long limit = inputSize - Long.BYTES + 1;
            for (; offset < limit; offset += Long.BYTES) {
                var block = UNSAFE.getLong(inputBase + offset);
                final long diff = block ^ BROADCAST_SEMICOLON;
                long matchIndicators = (diff - 0x0101010101010101L) & ~diff & 0x8080808080808080L;
                if (matchIndicators != 0) {
                    return offset + Long.numberOfTrailingZeros(matchIndicators) / 8;
                }
            }
            return simpleSearch(offset);
        }

        // Adapted from https://richardstartin.github.io/posts/finding-bytes
        long bytePosBigEndian(long offset) {
            final long limit = inputSize - Long.BYTES + 1;
            for (; offset < limit; offset += Long.BYTES) {
                var block = UNSAFE.getLong(inputBase + offset);
                final long diff = block ^ BROADCAST_SEMICOLON;
                long matchIndicators = (diff & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
                matchIndicators = ~(matchIndicators | diff | 0x7F7F7F7F7F7F7F7FL);
                if (matchIndicators != 0) {
                    return offset + Long.numberOfLeadingZeros(matchIndicators) / 8;
                }
            }
            return simpleSearch(offset);
        }

        private long simpleSearch(long offset) {
            for (; offset < inputSize; offset++) {
                if (UNSAFE.getByte(inputBase + offset) == SEMICOLON) {
                    return offset;
                }
            }
            throw new RuntimeException("Semicolon not found");
        }
    }

    private static boolean strcmp(long addr1, long addr2, long len) {
        int i = 0;
        for (; i <= len - Long.BYTES; i += Long.BYTES) {
            if (UNSAFE.getLong(addr1 + i) != UNSAFE.getLong(addr2 + i)) {
                return false;
            }
        }
        for (; i <= len - Integer.BYTES; i += Integer.BYTES) {
            if (UNSAFE.getInt(addr1 + i) != UNSAFE.getInt(addr2 + i)) {
                return false;
            }
        }
        for (; i < len; i++) {
            if (UNSAFE.getByte(addr1 + i) != UNSAFE.getByte(addr2 + i)) {
                return false;
            }
        }
        return true;
    }

    private static long broadcastSemicolon() {
        long nnnnnnnn = SEMICOLON;
        nnnnnnnn |= nnnnnnnn << 8;
        nnnnnnnn |= nnnnnnnn << 16;
        nnnnnnnn |= nnnnnnnn << 32;
        return nnnnnnnn;
    }

    static class StatsAccessor {
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
}
