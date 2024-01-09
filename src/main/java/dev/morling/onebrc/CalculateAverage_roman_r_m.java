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

    public static final int DOT_3_RD_BYTE_MASK = (byte) '.' << 16;
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        long fileSize = new File(FILE).length();

        var station = new ByteString();

        long offset = 0;
        var channel = FileChannel.open(Paths.get(FILE));
        var resultStore = new ResultStore();
        MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, offset, fileSize, Arena.ofAuto());
        while (offset < fileSize) {
            int i = 0;
            while (ms.get(ValueLayout.JAVA_BYTE, offset + i) != ';') {
                i++;
            }

            MemorySegment.copy(ms, ValueLayout.JAVA_BYTE, offset, station.buf, 0, i);
            station.len = i;
            station.hash = 0;

            offset += i + 1; // skip semicolon
            long val;
            if (fileSize - offset >= 8) {
                long encodedVal = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

                boolean neg = (encodedVal & (byte) '-') == (byte) '-';
                if (neg) {
                    encodedVal >>= 8;
                    offset++;
                }

                if ((encodedVal & DOT_3_RD_BYTE_MASK) == DOT_3_RD_BYTE_MASK) {
                    val = (encodedVal & 0xFF - 0x30) * 100 + (encodedVal >> 8 & 0xFF - 0x30) * 10 + (encodedVal >> 24 & 0xFF - 0x30);
                    offset += 5;
                }
                else {
                    val = (encodedVal & 0xFF - 0x30) * 10 + (encodedVal >> 16 & 0xFF - 0x30);
                    offset += 4;
                }

                if (neg) {
                    val = -val;
                }
            }
            else {
                boolean neg = ms.get(ValueLayout.JAVA_BYTE, offset) == '-';
                if (neg) {
                    offset++;
                }
                val = ms.get(ValueLayout.JAVA_BYTE, offset++) - '0';
                byte b;
                while ((b = ms.get(ValueLayout.JAVA_BYTE, offset++)) != '.') {
                    val = val * 10 + (b - '0');
                }
                b = ms.get(ValueLayout.JAVA_BYTE, offset);
                val = val * 10 + (b - '0');

                offset += 2;

                if (neg) {
                    val = -val;
                }
            }

            var a = resultStore.get(station);
            a.min = Math.min(a.min, val);
            a.max = Math.max(a.max, val);
            a.sum += val;
            a.count++;
        }

        System.out.println(resultStore.toMap());
    }

    static final class ByteString {

        private byte[] buf = new byte[100];
        private int len = 0;
        private int hash = 0;

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
            if (hash == 0) {
                for (int i = 0; i < len; i++) {
                    hash = 31 * hash + (buf[i] & 255);
                }
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
