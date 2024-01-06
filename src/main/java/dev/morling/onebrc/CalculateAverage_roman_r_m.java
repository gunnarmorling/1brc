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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_roman_r_m {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        long fileSize = new File(FILE).length();

        var station = new ByteString();
        var value = new ValueDecoder();
        var parsingStation = true;

        long offset = 0;
        var channel = FileChannel.open(Paths.get(FILE));
        var resultStore = new ResultStore();
        MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, offset, fileSize, Arena.ofAuto());
        while (offset < fileSize) {
            byte c = ms.get(ValueLayout.JAVA_BYTE, offset);
            offset++;
            if (c == ';') {
                value.reset();
                parsingStation = false;
            }
            else if (c == '\r') {
            }
            else if (c == '\n') {
                long val = value.get();
                var a = resultStore.get(station);

                a.min = Math.min(a.min, val);
                a.max = Math.max(a.max, val);
                a.sum += val;
                a.count++;

                parsingStation = true;
                station.reset();
                value.reset();
            }
            else if (parsingStation) {
                station.append(c);
            }
            else {
                value.add(c);
            }
        }

        System.out.println(resultStore.toMap());
    }

    static final class ValueDecoder {
        private boolean negative;
        private final byte[] buf = new byte[3];
        private int len;

        void reset() {
            negative = false;
            len = 0;
        }

        void add(byte b) {
            if (b == '-') {
                negative = true;
            }
            else if (b == '.') {

            }
            else {
                buf[len++] = (byte) (b - '0');
            }
        }

        long get() {
            long res;
            if (len == 3) {
                res = buf[2] + buf[1] * 10 + buf[0] * 100;
            } else if (len == 2) {
                res = buf[1] + buf[0] * 10;
            } else {
                throw new IllegalStateException(STR."buf=\{buf}, len=\{len}");
            }
            return negative ? -res : res;
        }
    }

    static final class ByteString {

        private byte[] buf = new byte[100];
        private int len = 0;
        private int hash = 0;

        void append(byte b) {
            buf[len++] = b;
            hash = 31 * hash + (b & 255);
        }

        void reset() {
            len = 0;
            hash = 0;
        }

        @Override
        public String toString() {
            return new String(buf, 0, len);
        }

        public ByteString copy() {
            var copy = new ByteString();
            copy.len = this.len;
            copy.hash = this.hash;
            if (copy.buf.length < this.buf.length) {
                copy.buf = new byte[this.buf.length];
            }
            System.arraycopy(this.buf, 0, copy.buf, 0, this.len);
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

            // TODO use Vector
            for (int i = 0; i < len; i++) {
                if (buf[i] != that.buf[i]) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static final class ResultRow {
        long min = 100;
        long sum = 0;
        long max = -100;
        int count = 0;

        public String toString() {
            return round(min / 10.0) + "/" + round(sum / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
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
