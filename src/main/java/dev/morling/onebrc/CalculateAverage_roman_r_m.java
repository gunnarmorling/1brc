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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class CalculateAverage_roman_r_m {

    private static final String FILE = "./measurements.txt";

    private static Unsafe UNSAFE;

    // based on http://0x80.pl/notesen/2023-03-06-swar-find-any.html
    static long hasZeroByte(long l) {
        return ((l - 0x0101010101010101L) & ~(l) & 0x8080808080808080L);
    }

    static long firstSetByteIndex(long l) {
        return ((((l - 1) & 0x101010101010101L) * 0x101010101010101L) >> 56) - 1;
    }

    static long broadcast(byte b) {
        return 0x101010101010101L * b;
    }

    static long SEMICOLON_MASK = broadcast((byte) ';');
    static long LINE_END_MASK = broadcast((byte) '\n');

    static long find(long l, long mask) {
        long xor = l ^ mask;
        long match = hasZeroByte(xor);
        return match != 0 ? firstSetByteIndex(match) : -1;
    }

    static long nextNewline(long from, MemorySegment ms) {
        long start = from;
        long i;
        long next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        while ((i = find(next, LINE_END_MASK)) < 0) {
            start += 8;
            next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        }
        return start + i;
    }

    static class Worker {
        private final MemorySegment ms;
        private final long end;
        private long offset;

        public Worker(FileChannel channel, long start, long end) {
            try {
                this.ms = channel.map(FileChannel.MapMode.READ_ONLY, start, end - start, Arena.ofConfined());
                this.offset = ms.address();
                this.end = ms.address() + end - start;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void parseName(ByteString station) {
            long start = offset;
            long pos = -1;

            while (end - offset > 8) {
                long next = UNSAFE.getLong(offset);
                pos = find(next, SEMICOLON_MASK);
                if (pos >= 0) {
                    offset += pos;
                    break;
                }
                else {
                    offset += 8;
                }
            }
            if (pos < 0) {
                while (UNSAFE.getByte(offset++) != ';') {
                }
                offset--;
            }

            int len = (int) (offset - start);
            station.offset = start;
            station.len = len;
            station.hash = 0;

            offset++;
        }

        long parseNumberFast() {
            long encodedVal = UNSAFE.getLong(offset);

            var len = find(encodedVal, LINE_END_MASK);
            offset += len + 1;

            encodedVal ^= broadcast((byte) 0x30);

            long c0 = len == 4 ? 100 : 10;
            long c1 = 10 * (len - 3);
            long c2 = 4 - len;
            long c3 = len - 3;
            long a = (encodedVal & 0xFF) * c0;
            long b = ((encodedVal & 0xFF00) >>> 8) * c1;
            long c = ((encodedVal & 0xFF0000L) >>> 16) * c2;
            long d = ((encodedVal & 0xFF000000L) >>> 24) * c3;

            return a + b + c + d;
        }

        long parseNumberSlow() {
            long val = UNSAFE.getByte(offset++) - '0';
            byte b;
            while ((b = UNSAFE.getByte(offset++)) != '.') {
                val = val * 10 + (b - '0');
            }
            b = UNSAFE.getByte(offset);
            val = val * 10 + (b - '0');
            offset += 2;
            return val;
        }

        long parseNumber() {
            long val;
            int neg = 1 - Integer.bitCount(UNSAFE.getByte(offset) & 0x10);
            offset += neg;

            if (end - offset > 8) {
                val = parseNumberFast();
            }
            else {
                val = parseNumberSlow();
            }
            val *= 1 - 2 * neg;
            return val;
        }

        public TreeMap<String, ResultRow> run() {
            var resultStore = new ResultStore();
            var station = new ByteString(ms);

            while (offset < end) {
                parseName(station);
                long val = parseNumber();
                var a = resultStore.get(station);
                a.min = Math.min(a.min, val);
                a.max = Math.max(a.max, val);
                a.sum += val;
                a.count++;
            }
            return resultStore.toMap();
        }
    }

    public static void main(String[] args) throws Exception {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        UNSAFE = (Unsafe) f.get(null);

        long fileSize = new File(FILE).length();

        var channel = FileChannel.open(Paths.get(FILE));
        MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.ofConfined());

        int numThreads = fileSize > Integer.MAX_VALUE ? Runtime.getRuntime().availableProcessors() : 1;
        long chunk = fileSize / numThreads;

        var bounds = IntStream.range(0, numThreads).mapToLong(i -> {
            boolean lastChunk = i == numThreads - 1;
            return lastChunk ? fileSize : nextNewline((i + 1) * chunk, ms);
        }).toArray();

        ms.unload();

        var result = IntStream.range(0, numThreads)
                .parallel()
                .mapToObj(i -> {
                    long start = i == 0 ? 0 : bounds[i - 1] + 1;
                    long end = bounds[i];
                    Worker worker = new Worker(channel, start, end);
                    var res = worker.run();
                    worker.ms.unload();
                    return res;
                }).reduce((m1, m2) -> {
                    m2.forEach((k, v) -> m1.merge(k, v, ResultRow::merge));
                    return m1;
                });

        System.out.println(result.get());
    }

    static final class ByteString {

        private final MemorySegment ms;
        private long offset;
        private int len = 0;
        private int hash = 0;

        ByteString(MemorySegment ms) {
            this.ms = ms;
        }

        @Override
        public String toString() {
            var bytes = new byte[len];
            UNSAFE.copyMemory(null, offset, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
            return new String(bytes, 0, len);
        }

        public ByteString copy() {
            var copy = new ByteString(ms);
            copy.offset = this.offset;
            copy.len = this.len;
            copy.hash = this.hash;
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ByteString that = (ByteString) o;

            if (len != that.len)
                return false;

            int i = 0;

            for (; i + 7 < len; i += 8) {
                long l1 = UNSAFE.getLong(offset + i);
                long l2 = UNSAFE.getLong(that.offset + i);
                if (l1 != l2) {
                    return false;
                }
            }
            if (len >= 8) {
                long l1 = UNSAFE.getLong(offset + len - 8);
                long l2 = UNSAFE.getLong(that.offset + len - 8);
                return l1 == l2;
            }
            for (; i < len; i++) {
                byte i1 = UNSAFE.getByte(offset + i);
                byte i2 = UNSAFE.getByte(that.offset + i);
                if (i1 != i2) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (hash == 0) {
                long h = UNSAFE.getLong(offset);
                h = Long.reverseBytes(h) >>> (8 * Math.max(0, 8 - len));
                hash = (int) (h ^ (h >>> 32));
            }
            return hash;
        }
    }

    private static final class ResultRow {
        long min = 1000;
        long sum = 0;
        long max = -1000;
        int count = 0;

        public String toString() {
            return round(min / 10.0) + "/" + round(sum / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public ResultRow merge(ResultRow other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }
    }

    static class ResultStore {
        private static final int SIZE = 16384;
        private final ByteString[] keys = new ByteString[SIZE];
        private final ResultRow[] values = new ResultRow[SIZE];

        ResultRow get(ByteString s) {
            int h = s.hashCode();
            int idx = (SIZE - 1) & h;

            int i = 0;
            while (keys[idx] != null && !keys[idx].equals(s)) {
                i++;
                idx = (idx + i * i) % SIZE;
            }
            ResultRow result;
            if (keys[idx] == null) {
                keys[idx] = s.copy();
                result = new ResultRow();
                values[idx] = result;
            }
            else {
                result = values[idx];
                // TODO see it it makes any difference
                // keys[idx].offset = s.offset;
            }
            return result;
        }

        TreeMap<String, ResultRow> toMap() {
            var result = new TreeMap<String, ResultRow>();
            for (int i = 0; i < SIZE; i++) {
                if (keys[i] != null) {
                    result.put(keys[i].toString(), values[i]);
                }
            }
            return result;
        }
    }
}
