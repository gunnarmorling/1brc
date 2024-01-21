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

    private static long broadcast(byte b) {
        return 0x101010101010101L * b;
    }

    private static final long SEMICOLON_MASK = broadcast((byte) ';');
    private static final long LINE_END_MASK = broadcast((byte) '\n');
    private static final long DOT_MASK = broadcast((byte) '.');

    // from netty

    /**
     * Applies a compiled pattern to given word.
     * Returns a word where each byte that matches the pattern has the highest bit set.
     */
    private static long applyPattern(final long word, final long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        return ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
    }

    static long nextNewline(long from, MemorySegment ms) {
        long start = from;
        long i;
        long next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        while ((i = applyPattern(next, LINE_END_MASK)) == 0) {
            start += 8;
            next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        }
        return start + Long.numberOfTrailingZeros(i) / 8;
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
            long pattern;
            long next = UNSAFE.getLong(offset);
            while ((pattern = applyPattern(next, SEMICOLON_MASK)) == 0) {
                offset += 8;
                next = UNSAFE.getLong(offset);
            }
            int bytes = Long.numberOfTrailingZeros(pattern) / 8;
            offset += bytes;

            int len = (int) (offset - start);
            station.offset = start;
            station.len = len;
            station.hash = 0;
            station.tail = next & ((1L << (8 * bytes)) - 1);

            offset++;
        }

        int parseNumberFast() {
            long encodedVal = UNSAFE.getLong(offset);

            int neg = 1 - Integer.bitCount((int) (encodedVal & 0x10));
            encodedVal >>>= 8 * neg;

            var len = applyPattern(encodedVal, DOT_MASK);
            len = Long.numberOfTrailingZeros(len) / 8;

            encodedVal ^= broadcast((byte) 0x30);

            int intPart = (int) (encodedVal & ((1 << (8 * len)) - 1));
            intPart <<= 8 * (2 - len);
            intPart *= (100 * 256 + 10);
            intPart = (intPart & 0x3FF80) >>> 8;

            int frac = (int) ((encodedVal >>> (8 * (len + 1))) & 0xFF);

            offset += neg + len + 3; // 1 for . + 1 for fractional part + 1 for new line char
            int sign = 1 - 2 * neg;
            int val = intPart + frac;
            return sign * val;
        }

        int parseNumberSlow() {
            int neg = 1 - Integer.bitCount(UNSAFE.getByte(offset) & 0x10);
            offset += neg;

            int val = UNSAFE.getByte(offset++) - '0';
            byte b;
            while ((b = UNSAFE.getByte(offset++)) != '.') {
                val = val * 10 + (b - '0');
            }
            b = UNSAFE.getByte(offset);
            val = val * 10 + (b - '0');
            offset += 2;
            val *= 1 - 2 * neg;
            return val;
        }

        int parseNumber() {
            if (end - offset >= 8) {
                return parseNumberFast();
            }
            else {
                return parseNumberSlow();
            }
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
        private long tail = 0L;

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
            copy.tail = this.tail;
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
            return this.tail == that.tail;
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
