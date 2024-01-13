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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class CalculateAverage_roman_r_m {

    public static final int DOT_3_RD_BYTE_MASK = (byte) '.' << 16;
    private static final String FILE = "./measurements.txt";
    private static MemorySegment ms;

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

    static long nextNewline(long from) {
        long start = from;
        long i;
        long next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        while ((i = find(next, LINE_END_MASK)) < 0) {
            start += 8;
            next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        }
        return start + i;
    }

    public static void main(String[] args) throws Exception {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        UNSAFE = (Unsafe) f.get(null);

        long fileSize = new File(FILE).length();

        var channel = FileChannel.open(Paths.get(FILE));
        ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.ofAuto());

        int numThreads = fileSize > Integer.MAX_VALUE ? Runtime.getRuntime().availableProcessors() : 1;
        long chunk = fileSize / numThreads;
        var result = IntStream.range(0, numThreads)
                .parallel()
                .mapToObj(i -> {
                    boolean lastChunk = i == numThreads - 1;
                    long chunkStart = i == 0 ? 0 : nextNewline(i * chunk) + 1;
                    long chunkEnd = lastChunk ? fileSize : nextNewline((i + 1) * chunk);

                    var resultStore = new ResultStore();
                    var station = new ByteString();

                    long offset = chunkStart;
                    while (offset < chunkEnd) {
                        long start = offset;
                        long pos = -1;

                        while (chunkEnd - offset >= 8) {
                            long next = UNSAFE.getLong(ms.address() + offset);
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
                            while (UNSAFE.getByte(ms.address() + offset++) != ';') {
                            }
                            offset--;
                        }

                        int len = (int) (offset - start);
                        // TODO can we not copy and use a reference into the memory segment to perform table lookup?

                        station.offset = start;
                        station.len = len;
                        station.hash = 0;

                        offset++;

                        long val;
                        boolean neg;
                        if (!lastChunk || fileSize - offset >= 8) {
                            long encodedVal = UNSAFE.getLong(ms.address() + offset);
                            neg = (encodedVal & (byte) '-') == (byte) '-';
                            if (neg) {
                                encodedVal >>= 8;
                                offset++;
                            }

                            if ((encodedVal & DOT_3_RD_BYTE_MASK) == DOT_3_RD_BYTE_MASK) {
                                val = (encodedVal & 0xFF - 0x30) * 100 + (encodedVal >> 8 & 0xFF - 0x30) * 10 + (encodedVal >> 24 & 0xFF - 0x30);
                                offset += 5;
                            }
                            else {
                                // based on http://0x80.pl/articles/simd-parsing-int-sequences.html#parsing-and-conversion-of-signed-numbers
                                val = Long.compress(encodedVal, 0xFF00FFL) - 0x303030;
                                val = ((val * 2561) >> 8) & 0xff;
                                offset += 4;
                            }
                        }
                        else {
                            neg = UNSAFE.getByte(ms.address() + offset) == '-';
                            if (neg) {
                                offset++;
                            }
                            val = UNSAFE.getByte(ms.address() + offset++) - '0';
                            byte b;
                            while ((b = UNSAFE.getByte(ms.address() + offset++)) != '.') {
                                val = val * 10 + (b - '0');
                            }
                            b = UNSAFE.getByte(ms.address() + offset);
                            val = val * 10 + (b - '0');
                            offset += 2;
                        }

                        if (neg) {
                            val = -val;
                        }

                        var a = resultStore.get(station);
                        a.min = Math.min(a.min, val);
                        a.max = Math.max(a.max, val);
                        a.sum += val;
                        a.count++;
                    }
                    return resultStore.toMap();
                }).reduce((m1, m2) -> {
                    m2.forEach((k, v) -> m1.merge(k, v, ResultRow::merge));
                    return m1;
                });

        System.out.println(result.get());
    }

    static final class ByteString {

        private long offset;
        private int len = 0;
        private int hash = 0;

        @Override
        public String toString() {
            var bytes = new byte[len];
            MemorySegment.copy(ms, ValueLayout.JAVA_BYTE, offset, bytes, 0, len);
            return new String(bytes, 0, len);
        }

        public ByteString copy() {
            var copy = new ByteString();
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

            long base1 = ms.address() + offset;
            long base2 = ms.address() + that.offset;
            for (; i + 3 < len; i += 4) {
                int i1 = UNSAFE.getInt(base1 + i);
                int i2 = UNSAFE.getInt(base2 + i);
                if (i1 != i2) {
                    return false;
                }
            }
            for (; i < len; i++) {
                byte i1 = UNSAFE.getByte(base1 + i);
                byte i2 = UNSAFE.getByte(base2 + i);
                if (i1 != i2) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (hash == 0) {
                // not sure why but it seems to be working a bit better
                hash = UNSAFE.getInt(ms.address() + offset);
                hash = hash >>> (8 * Math.max(0, 4 - len));
                hash |= len;
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
        private final ArrayList<ResultRow> results = new ArrayList<>(10000);
        private final Map<ByteString, Integer> indices = new HashMap<>(10000);

        ResultRow get(ByteString s) {
            var idx = indices.get(s);
            if (idx != null) {
                return results.get(idx);
            }
            else {
                ResultRow next = new ResultRow();
                results.add(next);
                indices.put(s.copy(), results.size() - 1);
                return next;
            }
        }

        TreeMap<String, ResultRow> toMap() {
            var result = new TreeMap<String, ResultRow>();
            indices.forEach((name, idx) -> result.put(name.toString(), results.get(idx)));
            return result;
        }
    }
}
