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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_kgeri {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregate {
        private double min = Double.MAX_VALUE;
        private double sum = 0d;
        private double max = Double.MIN_VALUE;
        private long count;

        void append(double measurement) {
            min = Math.min(min, measurement);
            max = Math.max(max, measurement);
            sum += measurement;
            count++;
        }

        @Override
		public String toString() {
			return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
		}

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    /**
     * This is to avoid instantiating `String`s during processing as much as possible.
     */
    private static class StringSlice {
        protected final byte[] buf;
        protected int len;
        protected int hash;

        public StringSlice(StringSlice other) {
            buf = Arrays.copyOfRange(other.buf, 0, other.len);
            len = other.len;
            hash = other.hash;
        }

        public StringSlice(byte[] buf, int len) {
            this.buf = buf;
            this.len = len;
            calculateHash();
        }

        public int length() {
            return len;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof StringSlice other && Arrays.equals(buf, 0, len, other.buf, 0, other.len);
        }

        @Override
        public String toString() {
            return new String(buf, 0, len, UTF_8);
        }

        protected void calculateHash() {
            hash = 1;
            for (int i = 0; i < len; i++) {
                hash = 31 * hash + buf[i];
            }
        }
    }

    /**
     * A flyweight of {@link StringSlice}, to avoid instantiating that as well.
     */
    private static class MutableStringSlice extends StringSlice {

        public MutableStringSlice(byte[] buf) {
            super(buf, 0);
        }

        public void updateLength(int length) {
            len = length;
            calculateHash();
        }

        public void clear() {
            len = 0;
            hash = 0;
        }
    }

    private static class MeasurementFlyweight {
        // Max name length is 100 UTF-8 chars + ';' + 5x UTF-8 chars = 206, rounding up
        private final byte[] buf = new byte[256];
        private final MutableStringSlice name = new MutableStringSlice(buf);
        private double measurement;

        long readNext(MemorySegment data, long position) {
            name.clear();
            for (int i = 0;; i++) {
                byte b = data.get(JAVA_BYTE, position + i); // Will throw an IOOBE for malformed files
                buf[i] = b;
                if (b == ';') {
                    name.updateLength(i);
                }
                else if (b == '\n') {
                    measurement = parseDoubleFrom(buf, name.length() + 1, i - name.length() - 1);
                    return position + i + 1;
                }
            }
        }

        public MutableStringSlice name() {
            return name;
        }

        public double measurement() {
            return measurement;
        }

        // Note: based on java.lang.Integer.parseInt, surely missing some edge cases but avoids allocation
        static double parseDoubleFrom(byte[] buf, int offset, int length) {
            boolean negative = false;
            int integer = 0;
            for (int i = 0; i < length; i++) {
                byte c = buf[offset + i];
                if (c == '-') {
                    negative = true;
                }
                else if (c != '.') {
                    integer *= 10;
                    integer -= c - '0';
                }
            }
            return (negative ? integer : -integer) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        Map<StringSlice, MeasurementAggregate> measurements = new HashMap<>(10000);

        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            long size = raf.length();
            MemorySegment data = raf.getChannel().map(READ_ONLY, 0, size, Arena.ofConfined());
            MeasurementFlyweight m = new MeasurementFlyweight();

            for (long position = 0L; position < size;) {
                position = m.readNext(data, position);
                MeasurementAggregate aggr = measurements.get(m.name());
                if (aggr == null) {
                    aggr = new MeasurementAggregate();
                    measurements.put(new StringSlice(m.name()), aggr);
                }
                aggr.append(m.measurement());
            }
        }

        System.out.println(measurements);
    }
}
