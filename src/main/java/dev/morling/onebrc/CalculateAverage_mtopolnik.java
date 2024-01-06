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
import static java.lang.foreign.ValueLayout.JAVA_LONG;

// create_measurements3.sh 500_000_000
// initial:    real	0m11.640s user	0m39.766s sys	0m9.852s
// short hash: real	0m11.241s user	0m36.534s sys	0m9.700s

public class CalculateAverage_mtopolnik {
    public static final int MAX_NAME_LEN = 100;
    public static final int NAME_SLOT_SIZE = 128;
    private static final int STATS_TABLE_SIZE = 1 << 15;
    public static final String MEASUREMENTS_TXT = "measurements.txt";

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
        // while (true)
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
        private static final long HASHBUF_SIZE = 16;

        private final long chunkStart;
        private final long chunkLimit;
        private final RandomAccessFile raf;
        private final StationStats[][] results;
        private final int myIndex;

        private MemorySegment inputMem;
        private MemorySegment namesMem;
        private MemorySegment hashBuf;
        private StatsAccessor stats;

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
                hashBuf = confinedArena.allocate(HASHBUF_SIZE);
                long offset = 0;
                byte semicolon = (byte) ';';
                byte newline = (byte) '\n';
                long broadcastSemicolon = broadcastByte(semicolon);
                long broadcastNewline = broadcastByte(newline);
                while (offset < inputMem.byteSize()) {
                    final long semicolonPos = bytePos(inputMem, semicolon, broadcastSemicolon, offset);
                    if (semicolonPos == -1) {
                        break;
                    }
                    final long newlinePos = bytePos(inputMem, newline, broadcastNewline, semicolonPos + 1);
                    if (newlinePos == -1) {
                        throw new RuntimeException("No newline after a semicolon!");
                    }
                    recordMeasurement(offset, semicolonPos, newlinePos);
                    offset = newlinePos + 1;
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

        private void recordMeasurement(long startPos, long semicolonPos, long newlinePos) {
            int temperature = parseTemperature(semicolonPos, newlinePos);
            final long hash = hash(inputMem, startPos, semicolonPos);
            int tableIndex = (int) (hash % STATS_TABLE_SIZE);
            short nameLen = (short) (semicolonPos - startPos);
            assert nameLen <= 100 : "nameLen > 100";
            MemorySegment nameSlice = inputMem.asSlice(startPos, nameLen);
            while (true) {
                stats.gotoIndex(tableIndex);
                long foundHash = stats.hash();
                if (foundHash == 0) {
                    stats.hash(hash);
                    stats.sum(temperature);
                    stats.count(1);
                    stats.min(temperature);
                    stats.max(temperature);
                    var nameBlock = namesMem.asSlice(tableIndex * NAME_SLOT_SIZE, NAME_SLOT_SIZE);
                    nameBlock.copyFrom(nameSlice);
                    nameBlock.set(JAVA_BYTE, nameLen, (byte) 0);
                    break;
                }
                if (foundHash != hash || namesMem.asSlice(tableIndex * NAME_SLOT_SIZE, nameLen).mismatch(nameSlice) != -1) {
                    tableIndex = (tableIndex + 1) % STATS_TABLE_SIZE;
                    continue;
                }
                stats.sum(stats.sum() + temperature);
                stats.count(stats.count() + 1);
                stats.min(Integer.min(stats.min(), temperature));
                stats.max(Integer.max(stats.max(), temperature));
                break;
            }
        }

        private int parseTemperature(long start, long limit) {
            final byte zeroChar = (byte) '0';
            int temperature = inputMem.get(JAVA_BYTE, limit - 1) - zeroChar;
            temperature += 10 * (inputMem.get(JAVA_BYTE, limit - 3) - zeroChar);
            if (limit - 4 > start) {
                final byte b = inputMem.get(JAVA_BYTE, limit - 4);
                if (b != (byte) '-') {
                    temperature += 100 * (b - zeroChar);
                    if (limit - 5 > start) {
                        temperature = -temperature;
                    }
                }
                else {
                    temperature = -temperature;
                }
            }
            return temperature;
        }

        private long hash(MemorySegment inputMem, long start, long limit) {
            hashBuf.set(JAVA_LONG, 0, 0);
            hashBuf.set(JAVA_LONG, 8, 0);
            hashBuf.copyFrom(inputMem.asSlice(start, Long.min(HASHBUF_SIZE, limit - start)));
            long n1 = hashBuf.get(JAVA_LONG, 0);
            long n2 = hashBuf.get(JAVA_LONG, 8);
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 19;
            long hash = n1;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            hash ^= n2;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            return hash != 0 ? hash & (~Long.MIN_VALUE) : 1;
        }
    }

    static long bytePos(MemorySegment haystack, byte needle, long broadcastNeedle, long start) {
        // return simpleSearch(haystack, needle, start);
        return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
                ? bytePosLittleEndian(haystack, start, needle, broadcastNeedle)
                : bytePosBigEndian(haystack, start, needle, broadcastNeedle);
    }

    // Adapted from https://jameshfisher.com/2017/01/24/bitwise-check-for-zero-byte/
    // and https://github.com/ashvardanian/StringZilla/blob/14e7a78edcc16b031c06b375aac1f66d8f19d45a/stringzilla/stringzilla.h#L139-L169
    static long bytePosLittleEndian(MemorySegment haystack, long start, byte needle, long broadcastNeedle) {
        long limit = haystack.byteSize() - Long.BYTES + 1;
        long offset = start;
        for (; offset < limit; offset += Long.BYTES) {
            var block = haystack.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long diff = block ^ broadcastNeedle;
            long matchIndicators = (diff - 0x0101010101010101L) & ~diff & 0x8080808080808080L;
            if (matchIndicators != 0) {
                return offset + Long.numberOfTrailingZeros(matchIndicators) / 8;
            }
        }
        return simpleSearch(haystack, needle, offset);
    }

    // Adapted from https://richardstartin.github.io/posts/finding-bytes
    static long bytePosBigEndian(MemorySegment haystack, long start, byte needle, long broadcastNeedle) {
        long limit = haystack.byteSize() - Long.BYTES + 1;
        long offset = start;
        for (; offset < limit; offset += Long.BYTES) {
            var block = haystack.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final long diff = block ^ broadcastNeedle;
            long matchIndicators = (diff & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
            matchIndicators = ~(matchIndicators | diff | 0x7F7F7F7F7F7F7F7FL);
            if (matchIndicators != 0) {
                return offset + Long.numberOfLeadingZeros(matchIndicators) / 8;
            }
        }
        return simpleSearch(haystack, needle, offset);
    }

    private static long broadcastByte(byte val) {
        long broadcast = val;
        broadcast |= broadcast << 8;
        broadcast |= broadcast << 16;
        broadcast |= broadcast << 32;
        return broadcast;
    }

    private static long simpleSearch(MemorySegment haystack, byte needle, long offset) {
        for (; offset < haystack.byteSize(); offset++) {
            if (haystack.get(JAVA_BYTE, offset) == needle) {
                return offset;
            }
        }
        return -1;
    }

    static class StatsAccessor {
        static final long HASH_OFFSET = 0;
        static final long SUM_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Integer.BYTES;
        static final long SIZEOF = (MAX_OFFSET + Integer.BYTES - 1) / 8 * 8 + 8;

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

        int sum() {
            return memSeg.get(JAVA_INT, base + SUM_OFFSET);
        }

        int count() {
            return memSeg.get(JAVA_INT, base + COUNT_OFFSET);
        }

        int min() {
            return memSeg.get(JAVA_INT, base + MIN_OFFSET);
        }

        int max() {
            return memSeg.get(JAVA_INT, base + MAX_OFFSET);
        }

        void hash(long hash) {
            memSeg.set(JAVA_LONG, base + HASH_OFFSET, hash);
        }

        void sum(int sum) {
            memSeg.set(JAVA_INT, base + SUM_OFFSET, sum);
        }

        void count(int count) {
            memSeg.set(JAVA_INT, base + COUNT_OFFSET, count);
        }

        void min(int min) {
            memSeg.set(JAVA_INT, base + MIN_OFFSET, min);
        }

        void max(int max) {
            memSeg.set(JAVA_INT, base + MAX_OFFSET, max);
        }
    }
}
