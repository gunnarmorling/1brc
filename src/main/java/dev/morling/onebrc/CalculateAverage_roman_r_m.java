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
    private static final long ZEROES_MASK = broadcast((byte) '0');

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

    static int hashFull(long word) {
        return (int) (word ^ (word >>> 32));
    }

    static int hashPartial(long word, int bytes) {
        long h = Long.reverseBytes(word) >>> (8 * (8 - bytes));
        return (int) (h ^ (h >>> 32));
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
                    try {
                        long segmentStart = i == 0 ? 0 : bounds[i - 1] + 1;
                        long segmentEnd = bounds[i];
                        var segment = channel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentEnd - segmentStart, Arena.ofConfined());

                        var resultStore = new ResultStore();
                        var station = new ByteString(segment);
                        long offset = segment.address();
                        long end = offset + segment.byteSize();
                        long tailMask;
                        while (offset < end) {
                            // parsing station name
                            long start = offset;
                            long next = UNSAFE.getLong(offset);
                            long pattern = applyPattern(next, SEMICOLON_MASK);
                            int bytes;
                            if (pattern == 0) {
                                station.hash = hashFull(next);
                                do {
                                    offset += 8;
                                    next = UNSAFE.getLong(offset);
                                    pattern = applyPattern(next, SEMICOLON_MASK);
                                } while (pattern == 0);

                                bytes = Long.numberOfTrailingZeros(pattern) / 8;
                                offset += bytes;
                                tailMask = ((1L << (8 * bytes)) - 1);
                            }
                            else {
                                bytes = Long.numberOfTrailingZeros(pattern) / 8;
                                offset += bytes;
                                tailMask = ((1L << (8 * bytes)) - 1);

                                station.hash = hashPartial(next, bytes);
                            }

                            int len = (int) (offset - start);
                            station.offset = start;
                            station.len = len;
                            station.tail = next & tailMask;

                            offset++;

                            // parsing temperature
                            // TODO next may contain temperature as well, maybe try using it if we know the full number is there
                            // 8 - bytes >= 5 -> bytes <= 3
                            long val;
                            if (end - offset >= 8) {
                                long encodedVal = UNSAFE.getLong(offset);

                                int neg = 1 - Integer.bitCount((int) (encodedVal & 0x10));
                                encodedVal >>>= 8 * neg;

                                long numLen = applyPattern(encodedVal, DOT_MASK);
                                numLen = Long.numberOfTrailingZeros(numLen) / 8;

                                encodedVal ^= ZEROES_MASK;

                                int intPart = (int) (encodedVal & ((1 << (8 * numLen)) - 1));
                                intPart <<= 8 * (2 - numLen);
                                intPart *= (100 * 256 + 10);
                                intPart = (intPart & 0x3FF80) >>> 8;

                                int frac = (int) ((encodedVal >>> (8 * (numLen + 1))) & 0xFF);

                                offset += neg + numLen + 3; // 1 for . + 1 for fractional part + 1 for new line char
                                int sign = 1 - 2 * neg;
                                val = sign * (intPart + frac);
                            }
                            else {
                                int neg = 1 - Integer.bitCount(UNSAFE.getByte(offset) & 0x10);
                                offset += neg;

                                val = UNSAFE.getByte(offset++) - '0';
                                byte b;
                                while ((b = UNSAFE.getByte(offset++)) != '.') {
                                    val = val * 10 + (b - '0');
                                }
                                b = UNSAFE.getByte(offset);
                                val = val * 10 + (b - '0');
                                offset += 2;
                                val *= 1 - (2L * neg);
                            }

                            resultStore.update(station, (int) val);
                        }

                        segment.unload();

                        return resultStore.toMap();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
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

        public String asString(byte[] reusable) {
            UNSAFE.copyMemory(null, offset, reusable, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
            return new String(reusable, 0, len);
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

            for (int i = 0; i + 7 < len; i += 8) {
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
            return hash;
        }

        @Override
        public String toString() {
            byte[] buf = new byte[100];
            return asString(buf);
        }
    }

    private static final class ResultRow {
        long min;
        long sum;
        long max;
        int count;

        public ResultRow(int[] values) {
            min = values[0];
            max = values[1];
            sum = values[2];
            count = values[3];
        }

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
        private final int[][] values = new int[SIZE][];

        void update(ByteString s, int value) {
            int h = s.hashCode();
            int idx = (SIZE - 1) & h;

            var keys = this.keys;

            int idx0 = idx;
            int i = 0;
            while (true) {
                if (keys[idx] != null && keys[idx].equals(s)) {
                    values[idx][0] = Math.min(values[idx][0], value);
                    values[idx][1] = Math.max(values[idx][1], value);
                    values[idx][2] += value;
                    values[idx][3] += 1;
                    return;
                }
                else if (keys[idx] == null) {
                    keys[idx] = s.copy();
                    values[idx] = new int[4];
                    values[idx][0] = value;
                    values[idx][1] = value;
                    values[idx][2] = value;
                    values[idx][3] = 1;
                    return;
                }
                else {
                    i++;
                    idx = (idx0 + i * i) % SIZE;
                }
            }
        }

        TreeMap<String, ResultRow> toMap() {
            byte[] buf = new byte[100];
            var result = new TreeMap<String, ResultRow>();
            for (int i = 0; i < SIZE; i++) {
                if (keys[i] != null) {
                    result.put(keys[i].asString(buf), new ResultRow(values[i]));
                }
            }
            return result;
        }
    }
}
