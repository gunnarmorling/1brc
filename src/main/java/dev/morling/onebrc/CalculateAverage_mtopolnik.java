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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.TreeMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

// create_measurements3.sh 500_000_000
// initial:    real	0m11.640s user	0m39.766s sys	0m9.852s
// short hash: real	0m11.241s user	0m36.534s sys	0m9.700s

public class CalculateAverage_mtopolnik {
    public static final int MAX_NAME_LEN = 100;
    public static final int NAME_SLOT_SIZE = 104;
    private static final int STATS_TABLE_SIZE = 1 << 16;
    private static final long NATIVE_MEM_PER_THREAD = (NAME_SLOT_SIZE + StatsAccessor.SIZEOF) * STATS_TABLE_SIZE;
    private static final long NATIVE_MEM_ON_8_THREADS = 8 * NATIVE_MEM_PER_THREAD;
    public static final String MEASUREMENTS_TXT = "measurements.txt";
    private static final byte SEMICOLON = (byte) ';';
    private static final long BROADCAST_SEMICOLON = broadcastSemicolon();

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

        private MemorySegment inputMem;
        private MemorySegment namesMem;
        private MemorySegment hashBuf;
        private StatsAccessor stats;
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
                inputMem = raf.getChannel().map(MapMode.READ_ONLY, chunkStart, chunkLimit - chunkStart, confinedArena);
                MemorySegment statsMem = confinedArena.allocate(STATS_TABLE_SIZE * StatsAccessor.SIZEOF, Long.BYTES);
                stats = new StatsAccessor(statsMem);
                namesMem = confinedArena.allocate(STATS_TABLE_SIZE * NAME_SLOT_SIZE, Long.BYTES);
                namesMem.fill((byte) 0);
                hashBuf = confinedArena.allocate(HASHBUF_SIZE);
                while (cursor < inputMem.byteSize()) {
                    recordMeasurementAndAdvanceCursor(bytePosOfSemicolon(inputMem, cursor));
                }
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
                    var name = namesMem.getUtf8String(i * NAME_SLOT_SIZE);
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
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void recordMeasurementAndAdvanceCursor(long semicolonPos) {
            final long hash = hash(inputMem, cursor, semicolonPos);
            int tableIndex = (int) (hash % STATS_TABLE_SIZE);
            long nameLen = semicolonPos - cursor;
            assert nameLen <= 100 : "nameLen > 100";
            MemorySegment nameSlice = inputMem.asSlice(cursor, nameLen);
            int temperature = parseTemperatureAndAdvanceCursor(semicolonPos);
            while (true) {
                stats.gotoIndex(tableIndex);
                long foundHash = stats.hash();
                if (foundHash == 0) {
                    stats.setHash(hash);
                    stats.setNameLen((int) nameLen);
                    stats.setSum(temperature);
                    stats.setCount(1);
                    stats.setMin((short) temperature);
                    stats.setMax((short) temperature);
                    var nameBlock = namesMem.asSlice(tableIndex * NAME_SLOT_SIZE, NAME_SLOT_SIZE);
                    nameBlock.copyFrom(nameSlice);
                    break;
                }
                if (foundHash != hash
                        || stats.nameLen() != nameLen
                        || namesMem.asSlice(tableIndex * NAME_SLOT_SIZE, nameLen).mismatch(nameSlice) != -1) {
                    tableIndex = (tableIndex + 1) % STATS_TABLE_SIZE;
                    continue;
                }
                stats.setSum(stats.sum() + temperature);
                stats.setCount(stats.count() + 1);
                stats.setMin((short) Integer.min(stats.min(), temperature));
                stats.setMax((short) Integer.max(stats.max(), temperature));
                break;
            }
        }

        private int parseTemperatureAndAdvanceCursor(long semicolonPos) {
            final byte minus = (byte) '-';
            final byte zero = (byte) '0';
            final byte dot = (byte) '.';

            long start = semicolonPos + 1;
            // Temperature plus the following newline is at least 4 chars, so this is always safe:
            int fourCh = inputMem.get(JAVA_INT_UNALIGNED, start);
            if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
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
                ch = inputMem.get(JAVA_BYTE, start + (shift / 8));
            }
            temperature = 10 * temperature + (ch - zero);
            // `shift` holds the number of bits in the temperature field.
            // A newline character follows the temperature, and so we advance
            // the cursor past the newline to the start of the next line.
            cursor = start + (shift / 8) + 2;
            return sign * temperature;
        }

        private long hash(MemorySegment inputMem, long start, long limit) {
            hashBuf.set(JAVA_LONG, 0, 0);
            // hashBuf.set(JAVA_LONG, 8, 0);
            hashBuf.copyFrom(inputMem.asSlice(start, Long.min(HASHBUF_SIZE, limit - start)));
            long n1 = hashBuf.get(JAVA_LONG, 0);
            // long n2 = hashBuf.get(JAVA_LONG, 8);
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
    }

    static long bytePosOfSemicolon(MemorySegment haystack, long start) {
        return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
                ? bytePosLittleEndian(haystack, start)
                : bytePosBigEndian(haystack, start);
    }

    // Adapted from https://jameshfisher.com/2017/01/24/bitwise-check-for-zero-byte/
    // and https://github.com/ashvardanian/StringZilla/blob/14e7a78edcc16b031c06b375aac1f66d8f19d45a/stringzilla/stringzilla.h#L139-L169
    static long bytePosLittleEndian(MemorySegment haystack, long start) {
        long limit = haystack.byteSize() - Long.BYTES + 1;
        long offset = start;
        for (; offset < limit; offset += Long.BYTES) {
            var block = haystack.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long diff = block ^ BROADCAST_SEMICOLON;
            long matchIndicators = (diff - 0x0101010101010101L) & ~diff & 0x8080808080808080L;
            if (matchIndicators != 0) {
                return offset + Long.numberOfTrailingZeros(matchIndicators) / 8;
            }
        }
        return simpleSearch(haystack, offset);
    }

    // Adapted from https://richardstartin.github.io/posts/finding-bytes
    static long bytePosBigEndian(MemorySegment haystack, long start) {
        long limit = haystack.byteSize() - Long.BYTES + 1;
        long offset = start;
        for (; offset < limit; offset += Long.BYTES) {
            var block = haystack.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long diff = block ^ BROADCAST_SEMICOLON;
            long matchIndicators = (diff & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
            matchIndicators = ~(matchIndicators | diff | 0x7F7F7F7F7F7F7F7FL);
            if (matchIndicators != 0) {
                return offset + Long.numberOfLeadingZeros(matchIndicators) / 8;
            }
        }
        return simpleSearch(haystack, offset);
    }

    private static long broadcastSemicolon() {
        long nnnnnnnn = SEMICOLON;
        nnnnnnnn |= nnnnnnnn << 8;
        nnnnnnnn |= nnnnnnnn << 16;
        nnnnnnnn |= nnnnnnnn << 32;
        return nnnnnnnn;
    }

    private static long simpleSearch(MemorySegment haystack, long offset) {
        for (; offset < haystack.byteSize(); offset++) {
            if (haystack.get(JAVA_BYTE, offset) == SEMICOLON) {
                return offset;
            }
        }
        return -1;
    }

    static class StatsAccessor {
        static final long HASH_OFFSET = 0;
        static final long NAMELEN_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long SUM_OFFSET = NAMELEN_OFFSET + Integer.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Short.BYTES;
        static final long SIZEOF = (MAX_OFFSET + Short.BYTES - 1) / 8 * 8 + 8;

        private final MemorySegment memSeg;
        private long base;

        StatsAccessor(MemorySegment memSeg) {
            this.memSeg = memSeg;
        }

        void gotoIndex(int index) {
            base = index * SIZEOF;
        }

        long hash() {
            return memSeg.get(JAVA_LONG, base + HASH_OFFSET);
        }

        int nameLen() {
            return memSeg.get(JAVA_INT, base + NAMELEN_OFFSET);
        }

        int sum() {
            return memSeg.get(JAVA_INT, base + SUM_OFFSET);
        }

        int count() {
            return memSeg.get(JAVA_INT, base + COUNT_OFFSET);
        }

        short min() {
            return memSeg.get(JAVA_SHORT, base + MIN_OFFSET);
        }

        short max() {
            return memSeg.get(JAVA_SHORT, base + MAX_OFFSET);
        }

        void setHash(long hash) {
            memSeg.set(JAVA_LONG, base + HASH_OFFSET, hash);
        }

        void setNameLen(int nameLen) {
            memSeg.set(JAVA_INT, base + NAMELEN_OFFSET, nameLen);
        }

        void setSum(int sum) {
            memSeg.set(JAVA_INT, base + SUM_OFFSET, sum);
        }

        void setCount(int count) {
            memSeg.set(JAVA_INT, base + COUNT_OFFSET, count);
        }

        void setMin(short min) {
            memSeg.set(JAVA_SHORT, base + MIN_OFFSET, min);
        }

        void setMax(short max) {
            memSeg.set(JAVA_SHORT, base + MAX_OFFSET, max);
        }
    }
}
