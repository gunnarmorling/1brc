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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_roman_r_m {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        long fileSize = new File(FILE).length();

        var measurements = new HashMap<String, ResultRow>();

        var station = new ByteString();
        var value = new ByteString();
        var parsingStation = true;

        long offset = 0;
        var channel = FileChannel.open(Paths.get(FILE));
        var stringCache = new StringCache();
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
                String name = stringCache.get(station);
                double val = value.parseDouble();

                var a = measurements.computeIfAbsent(name, x -> new ResultRow());
                a.min = Math.min(a.min, val);
                a.max = Math.max(a.max, val);
                a.sum += val;
                a.count++;

                parsingStation = true;
                station.reset();

            }
            else if (parsingStation) {
                station.append(c);
            }
            else {
                value.append(c);
            }
        }

        System.out.println(new TreeMap<>(measurements));
    }

    static final class ByteString {

        private byte[] buf = new byte[128];
        private int len = 0;
        private int hash = 0;

        void append(byte b) {
            if (buf.length == len) {
                var newBuf = new byte[(int) (buf.length * 1.5)];
                System.arraycopy(this.buf, 0, newBuf, 0, len);
                this.buf = newBuf;
            }
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

        public double parseDouble() {
            double res = 0;
            boolean negate = buf[0] == '-';
            for (int i = negate ? 1 : 0; i < len - 2; i++) {
                res = res * 10 + (buf[i] - '0');
            }
            res = res + (buf[len - 1] - '0') / 10.0;
            return negate ? -res : res;
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
        double min = 0.0;
        double sum = 0.0;
        double max = 0.0;
        int count = 0;

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class StringCache {
        private final Map<ByteString, String> cache = new HashMap<>(1000);

        String get(ByteString s) {
            var v = cache.get(s);
            if (v != null) {
                return v;
            }
            else {
                String str = s.toString();
                cache.put(s.copy(), str);
                return str;
            }
        }
    }
}
