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
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import sun.misc.Unsafe;

public class CalculateAverage_gonixunsafe {

    private static final String FILE = "./measurements.txt";
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws Exception {

        var file = new RandomAccessFile(FILE, "r");

        var chunks = Aggregator.buildChunks(file, MAX_THREADS);
        var chunksCount = chunks.size();
        var threads = new Thread[chunksCount];
        var result = new AtomicReference<Aggregator>();
        for (int i = 0; i < chunksCount; ++i) {
            var agg = new Aggregator();
            var chunk = chunks.get(i);
            var thread = new Thread(() -> {
                agg.processChunk(chunk);
                while (!result.compareAndSet(null, agg)) {
                    Aggregator other = result.getAndSet(null);
                    if (other != null) {
                        agg.merge(other);
                    }
                }
            });
            thread.start();
            threads[i] = thread;
        }
        for (int i = 0; i < chunksCount; ++i) {
            threads[i].join();
        }
        System.out.println(result.get().toString());
        System.out.close();
    }

    private static class Aggregator {
        private static final int MAX_STATIONS = 10_000;
        private static final int INDEX_SIZE = 256 * 1024 * 8;
        private static final int INDEX_MASK = (INDEX_SIZE - 1) & ~7;

        private static final int HEADER_SIZE = 8;
        private static final int MAX_KEY_SIZE = 100;
        private static final int FLD_COUNT = 0; // long
        private static final int FLD_SUM = 8; // long
        private static final int FLD_MIN = 16; // int
        private static final int FLD_MAX = 20; // int
        private static final int FLD_HASH = 24; // int
        private static final int FIELDS_SIZE = 28 + 4; // +padding to align to 8 bytes
        private static final int MAX_STATION_SIZE = HEADER_SIZE + MAX_KEY_SIZE + FIELDS_SIZE;

        private static final Unsafe UNSAFE;

        static {
            try {
                Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
                unsafe.setAccessible(true);
                UNSAFE = (Unsafe) unsafe.get(Unsafe.class);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        private static long alloc(long size) {
            long addr = UNSAFE.allocateMemory(size);
            UNSAFE.setMemory(addr, size, (byte) 0);
            return addr;
        }

        // Poor man's hash map: hash code to offset in `mem`.
        private final long indexAddr = alloc(INDEX_SIZE);

        // Contiguous storage of key (station name) and stats fields of all
        // unique stations.
        // The idea here is to improve locality so that stats fields would
        // possibly be already in the CPU cache after we are done comparing
        // the key.
        private final long memAddr = alloc(MAX_STATIONS * MAX_STATION_SIZE);
        private long memUsed = memAddr;
        private int count = 0;

        static List<Chunk> buildChunks(RandomAccessFile file, int count) throws IOException {
            var fileSize = file.length();
            var chunkSize = Math.min(Integer.MAX_VALUE - 512, fileSize / count);
            if (chunkSize <= 0) {
                chunkSize = fileSize;
            }
            var chunks = new ArrayList<Chunk>((int) (fileSize / chunkSize) + 1);
            var mmap = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            var fileStartAddr = mmap.address();
            var fileEndAddr = mmap.address() + mmap.byteSize();
            var chunkStartAddr = fileStartAddr;
            while (chunkStartAddr < fileEndAddr) {
                var pos = chunkStartAddr + chunkSize;
                if (pos < fileEndAddr) {
                    while (UNSAFE.getByte(pos) != '\n') {
                        pos += 1;
                    }
                    pos += 1;
                }
                else {
                    pos = fileEndAddr;
                }
                chunks.add(new Chunk(mmap, chunkStartAddr, pos, fileStartAddr, fileEndAddr));
                chunkStartAddr = pos;
            }
            return chunks;
        }

        Aggregator processChunk(Chunk chunk) {
            // As an optimization, we assume that we can read past the end
            // of file size if as we don't cross page boundary.
            final int WANT_PADDING = 8;
            final int PAGE_SIZE = UNSAFE.pageSize();
            if (((chunk.chunkEndAddr + WANT_PADDING) / PAGE_SIZE) <= (chunk.fileEndAddr / PAGE_SIZE)) {
                return processChunk(chunk.chunkStartAddr, chunk.chunkEndAddr);
            }

            // Otherwise, to avoid checking if it is safe to read a whole long
            // near the end of a chunk, we copy the last couple of lines to a
            // padded buffer and process that part separately.
            long pos = Math.max(-1, chunk.chunkEndAddr - WANT_PADDING - 1);
            while (pos >= 0 && UNSAFE.getByte(pos) != '\n') {
                pos--;
            }
            pos++;
            if (pos > 0) {
                processChunk(chunk.chunkStartAddr, pos);
            }
            long tailLen = chunk.chunkEndAddr - pos;
            var tailAddr = alloc(tailLen + WANT_PADDING);
            UNSAFE.copyMemory(pos, tailAddr, tailLen);
            processChunk(tailAddr, tailAddr + tailLen);
            return this;
        }

        private Aggregator processChunk(long startAddr, long endAddr) {
            long pos = startAddr;
            while (pos < endAddr) {

                long start = pos;
                long keyLong = UNSAFE.getLong(pos);
                long valueSepMark = valueSepMark(keyLong);
                if (valueSepMark != 0) {
                    int tailBits = tailBits(valueSepMark);
                    pos += valueOffset(tailBits);
                    // assert (UNSAFE.getByte(pos - 1) == ';') : "Expected ';' (1), pos=" + (pos - startAddr);
                    long tailAndLen = tailAndLen(tailBits, keyLong, pos - start - 1);

                    long valueLong = UNSAFE.getLong(pos);
                    int decimalSepMark = decimalSepMark(valueLong);
                    pos += nextKeyOffset(decimalSepMark);
                    // assert (UNSAFE.getByte(pos - 1) == '\n') : "Expected '\\n' (1), pos=" + (pos - startAddr);
                    int measurement = decimalValue(decimalSepMark, valueLong);

                    add1(start, tailAndLen, hash(hash1(tailAndLen)), measurement);
                    continue;
                }

                pos += 8;
                long keyLong1 = keyLong;
                keyLong = UNSAFE.getLong(pos);
                valueSepMark = valueSepMark(keyLong);
                if (valueSepMark != 0) {
                    int tailBits = tailBits(valueSepMark);
                    pos += valueOffset(tailBits);
                    // assert (UNSAFE.getByte(pos - 1) == ';') : "Expected ';' (2), pos=" + (pos - startAddr);
                    long tailAndLen = tailAndLen(tailBits, keyLong, pos - start - 1);

                    long valueLong = UNSAFE.getLong(pos);
                    int decimalSepMark = decimalSepMark(valueLong);
                    pos += nextKeyOffset(decimalSepMark);
                    // assert (UNSAFE.getByte(pos - 1) == '\n') : "Expected '\\n' (2), pos=" + (pos - startAddr);
                    int measurement = decimalValue(decimalSepMark, valueLong);

                    add2(start, keyLong1, tailAndLen, hash(hash(hash1(keyLong1), tailAndLen)), measurement);
                    continue;
                }

                long hash = hash1(keyLong1);
                do {
                    pos += 8;
                    hash = hash(hash, keyLong);
                    keyLong = UNSAFE.getLong(pos);
                    valueSepMark = valueSepMark(keyLong);
                } while (valueSepMark == 0);
                int tailBits = tailBits(valueSepMark);
                pos += valueOffset(tailBits);
                // assert (UNSAFE.getByte(pos - 1) == ';') : "Expected ';' (N), pos=" + (pos - startAddr);
                long tailAndLen = tailAndLen(tailBits, keyLong, pos - start - 1);
                hash = hash(hash, tailAndLen);

                long valueLong = UNSAFE.getLong(pos);
                int decimalSepMark = decimalSepMark(valueLong);
                pos += nextKeyOffset(decimalSepMark);
                // assert (UNSAFE.getByte(pos - 1) == '\n') : "Expected '\\n' (N), pos=" + (pos - startAddr);
                int measurement = decimalValue(decimalSepMark, valueLong);

                addN(start, tailAndLen, hash(hash), measurement);
            }

            return this;
        }

        private static long hash1(long value) {
            return value;
        }

        private static long hash(long hash, long value) {
            return hash ^ value;
        }

        private static int hash(long hash) {
            hash *= 0x9E3779B97F4A7C15L; // Fibonacci hashing multiplier
            return (int) (hash >>> 39);
        }

        private static long valueSepMark(long keyLong) {
            // Seen this trick used in multiple other solutions.
            // Nice breakdown here: https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
            long match = keyLong ^ 0x3B3B3B3B_3B3B3B3BL; // 3B == ';'
            match = (match - 0x01010101_01010101L) & (~match & 0x80808080_80808080L);
            return match;
        }

        private static int tailBits(long valueSepMark) {
            return Long.numberOfTrailingZeros(valueSepMark >>> 7);
        }

        private static int valueOffset(int tailBits) {
            return (int) (tailBits >>> 3) + 1;
        }

        private static long tailAndLen(int tailBits, long keyLong, long keyLen) {
            long tailMask = ~(-1L << tailBits);
            long tail = keyLong & tailMask;
            return (tail << 8) | (keyLen & 0xFF);
        }

        private static int decimalSepMark(long value) {
            // Seen this trick used in multiple other solutions.
            // Looks like the original author is @merykitty.

            // The 4th binary digit of the ascii of a digit is 1 while
            // that of the '.' is 0. This finds the decimal separator
            // The value can be 12, 20, 28
            return Long.numberOfTrailingZeros(~value & 0x10101000);
        }

        private static int decimalValue(int decimalSepMark, long value) {
            // Seen this trick used in multiple other solutions.
            // Looks like the original author is @merykitty.

            int shift = 28 - decimalSepMark;
            // signed is -1 if negative, 0 otherwise
            long signed = (~value << 59) >> 63;
            long designMask = ~(signed & 0xFF);
            // Align the number to a specific position and transform the ascii code
            // to actual digit value in each byte
            long digits = ((value & designMask) << shift) & 0x0F000F0F00L;

            // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
            // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
            // 0x000000UU00TTHH00 +
            // 0x00UU00TTHH000000 * 10 +
            // 0xUU00TTHH00000000 * 100
            // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
            // This results in our value lies in the bit 32 to 41 of this product
            // That was close :)
            long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            return (int) ((absValue ^ signed) - signed);
        }

        private static int nextKeyOffset(int decimalSepMark) {
            return (decimalSepMark >>> 3) + 3;
        }

        private void add1(long keyStartAddr, long tailAndLen, int hash, int measurement) {
            int idx = hash & INDEX_MASK;
            for (long entryAddr; (entryAddr = UNSAFE.getLong(indexAddr + idx)) != 0; idx = (idx + 8) & INDEX_MASK) {
                if (update1(entryAddr, tailAndLen, measurement)) {
                    return;
                }
            }
            UNSAFE.putLong(indexAddr + idx, create(keyStartAddr, tailAndLen, hash, measurement, '1'));
        }

        private void add2(long keyStartAddr, long keyLong, long tailAndLen, int hash, int measurement) {
            int idx = hash & INDEX_MASK;
            for (long entryAddr; (entryAddr = UNSAFE.getLong(indexAddr + idx)) != 0; idx = (idx + 8) & INDEX_MASK) {
                if (update2(entryAddr, keyLong, tailAndLen, measurement)) {
                    return;
                }
            }
            UNSAFE.putLong(indexAddr + idx, create(keyStartAddr, tailAndLen, hash, measurement, '2'));
        }

        private void addN(long keyStartAddr, long tailAndLen, int hash, int measurement) {
            int idx = hash & INDEX_MASK;
            for (long entryAddr; (entryAddr = UNSAFE.getLong(indexAddr + idx)) != 0; idx = (idx + 8) & INDEX_MASK) {
                if (updateN(entryAddr, keyStartAddr, tailAndLen, measurement)) {
                    return;
                }
            }
            UNSAFE.putLong(indexAddr + idx, create(keyStartAddr, tailAndLen, hash, measurement, 'N'));
        }

        private long create(long keyStartAddr, long tailAndLen, int hash, int measurement, char _origin) {
            // assert (memUsed + MAX_STATION_SIZE < memAddr + MAX_STATION_SIZE * MAX_STATIONS) : "Too many stations";

            final long entryAddr = memUsed;

            int keySize = (int) (tailAndLen & 0xF8);
            long fieldsAddr = entryAddr + HEADER_SIZE + keySize;
            memUsed += HEADER_SIZE + keySize + FIELDS_SIZE;
            count++;

            UNSAFE.putLong(entryAddr, tailAndLen);
            UNSAFE.copyMemory(keyStartAddr, entryAddr + HEADER_SIZE, keySize);
            UNSAFE.putLong(fieldsAddr + FLD_COUNT, 1);
            UNSAFE.putLong(fieldsAddr + FLD_SUM, measurement);
            UNSAFE.putInt(fieldsAddr + FLD_MIN, measurement);
            UNSAFE.putInt(fieldsAddr + FLD_MAX, measurement);
            UNSAFE.putInt(fieldsAddr + FLD_HASH, hash);

            return entryAddr;
        }

        private static boolean update1(long entryAddr, long tailAndLen, int measurement) {
            if (UNSAFE.getLong(entryAddr) != tailAndLen) {
                return false;
            }

            updateStats(entryAddr + HEADER_SIZE, measurement);
            return true;
        }

        private static boolean update2(long entryAddr, long keyLong, long tailAndLen, int measurement) {
            if (UNSAFE.getLong(entryAddr) != tailAndLen) {
                return false;
            }
            if (UNSAFE.getLong(entryAddr + 8) != keyLong) {
                return false;
            }

            updateStats(entryAddr + HEADER_SIZE + 8, measurement);
            return true;
        }

        private static boolean updateN(long entryAddr, long keyStartAddr, long tailAndLen, int measurement) {
            if (UNSAFE.getLong(entryAddr) != tailAndLen) {
                return false;
            }
            long memPos = entryAddr + HEADER_SIZE;
            long memEnd = memPos + ((int) (tailAndLen & 0xF8));
            long bufPos = keyStartAddr;
            while (memPos != memEnd) {
                if (UNSAFE.getLong(memPos) != UNSAFE.getLong(bufPos)) {
                    return false;
                }
                memPos += 8;
                bufPos += 8;
            }

            updateStats(memPos, measurement);
            return true;
        }

        private static void updateStats(long addr, int measurement) {
            long oldCount = UNSAFE.getLong(addr + FLD_COUNT);
            long oldSum = UNSAFE.getLong(addr + FLD_SUM);
            long oldMin = UNSAFE.getInt(addr + FLD_MIN);
            long oldMax = UNSAFE.getInt(addr + FLD_MAX);

            UNSAFE.putLong(addr + FLD_COUNT, oldCount + 1);
            UNSAFE.putLong(addr + FLD_SUM, oldSum + measurement);
            if (measurement < oldMin) {
                UNSAFE.putInt(addr + FLD_MIN, measurement);
            }
            if (measurement > oldMax) {
                UNSAFE.putInt(addr + FLD_MAX, measurement);
            }
        }

        private static void updateStats(long addr, long count, long sum, int min, int max) {
            long oldCount = UNSAFE.getLong(addr + FLD_COUNT);
            long oldSum = UNSAFE.getLong(addr + FLD_SUM);
            long oldMin = UNSAFE.getInt(addr + FLD_MIN);
            long oldMax = UNSAFE.getInt(addr + FLD_MAX);

            UNSAFE.putLong(addr + FLD_COUNT, oldCount + count);
            UNSAFE.putLong(addr + FLD_SUM, oldSum + sum);
            if (min < oldMin) {
                UNSAFE.putInt(addr + FLD_MIN, min);
            }
            if (max > oldMax) {
                UNSAFE.putInt(addr + FLD_MAX, max);
            }
        }

        public Aggregator merge(Aggregator other) {
            var otherMemPos = other.memAddr;
            var otherMemEnd = other.memUsed;
            merge: for (long entrySize; otherMemPos < otherMemEnd; otherMemPos += entrySize) {
                int keySize = (int) (UNSAFE.getLong(otherMemPos) & 0xF8);
                long otherKeyEnd = otherMemPos + HEADER_SIZE + keySize;
                entrySize = HEADER_SIZE + keySize + FIELDS_SIZE;
                int hash = UNSAFE.getInt(otherKeyEnd + FLD_HASH);
                int idx = hash & INDEX_MASK;
                search: for (long entryAddr; (entryAddr = UNSAFE.getLong(indexAddr + idx)) != 0; idx = (idx + 8) & INDEX_MASK) {
                    var thisPos = entryAddr;
                    var otherPos = otherMemPos;
                    while (otherPos < otherKeyEnd) {
                        if (UNSAFE.getLong(thisPos) != UNSAFE.getLong(otherPos)) {
                            continue search;
                        }
                        thisPos += 8;
                        otherPos += 8;
                    }
                    updateStats(
                            thisPos,
                            UNSAFE.getLong(otherPos + FLD_COUNT),
                            UNSAFE.getLong(otherPos + FLD_SUM),
                            UNSAFE.getInt(otherPos + FLD_MIN),
                            UNSAFE.getInt(otherPos + FLD_MAX));
                    continue merge;
                }

                // create
                // assert (memUsed + MAX_STATION_SIZE < memAddr + MAX_STATION_SIZE * MAX_STATIONS) : "Too many stations (merge)";
                long entryAddr = memUsed;
                memUsed += entrySize;
                count++;
                UNSAFE.copyMemory(otherMemPos, entryAddr, entrySize);
                UNSAFE.putLong(indexAddr + idx, entryAddr);
            }
            return this;
        }

        @Override
        public String toString() {
            if (count == 0) {
                return "{}";
            }
            var entries = new Entry[count];
            int i = 0;
            for (long pos = memAddr; pos < memUsed; pos += (int) (UNSAFE.getLong(pos) & 0xF8) + HEADER_SIZE + FIELDS_SIZE) {
                entries[i++] = new Entry(pos);
            }
            Arrays.sort(entries);
            var sb = new StringBuilder(count * 50);
            sb.append('{');
            entries[0].appendTo(sb);
            for (int j = 1; j < entries.length; ++j) {
                sb.append(", ");
                entries[j].appendTo(sb);
            }
            sb.append('}');
            return sb.toString();
        }

        static class Chunk {
            final MemorySegment file;
            final long chunkStartAddr;
            final long chunkEndAddr;
            final long fileStartAddr;
            final long fileEndAddr;

            Chunk(MemorySegment file, long chunkStartAddr, long chunkEndAddr, long fileStartAddr, long fileEndAddr) {
                this.file = file;
                this.chunkStartAddr = chunkStartAddr;
                this.chunkEndAddr = chunkEndAddr;
                this.fileStartAddr = fileStartAddr;
                this.fileEndAddr = fileEndAddr;
            }
        }

        static class Entry implements Comparable<Entry> {
            private final long entryAddr;
            private final int keySize;
            private final String key;

            Entry(long entryAddr) {
                this.entryAddr = entryAddr;
                this.keySize = (int) UNSAFE.getLong(entryAddr) & 0xF8;
                try (var arena = Arena.ofConfined()) {
                    var ms = arena.allocate(keySize + 8);
                    UNSAFE.copyMemory(entryAddr + HEADER_SIZE, ms.address(), keySize);
                    UNSAFE.copyMemory(entryAddr + 1, ms.address() + keySize, 7);
                    this.key = ms.getUtf8String(0);
                }
            }

            @Override
            public int compareTo(Entry other) {
                return key.compareTo(other.key);
            }

            @Override
            public String toString() {
                long pos = entryAddr + HEADER_SIZE + keySize;
                return round(UNSAFE.getInt(pos + FLD_MIN))
                        + "/" + round(((double) UNSAFE.getLong(pos + FLD_SUM)) / UNSAFE.getLong(pos + FLD_COUNT))
                        + "/" + round(UNSAFE.getInt(pos + FLD_MAX));
            }

            void appendTo(StringBuilder sb) {
                long pos = entryAddr + HEADER_SIZE + keySize;
                sb.append(key);
                sb.append('=');
                sb.append(round(UNSAFE.getInt(pos + FLD_MIN)));
                sb.append('/');
                sb.append(round(((double) UNSAFE.getLong(pos + FLD_SUM)) / UNSAFE.getLong(pos + FLD_COUNT)));
                sb.append('/');
                sb.append(round(UNSAFE.getInt(pos + FLD_MAX)));
            }

            private static double round(double value) {
                return Math.round(value) / 10.0;
            }
        }
    }
}
