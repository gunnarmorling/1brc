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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.TreeMap;

public class CalculateAverage_roman_r_m {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        long fileSize = new File(FILE).length();

        var measurements = new HashMap<String, ResultRow>();

        var station = new ByteString();
        var value = new ByteString();
        var parsingStation = true;

        long chunkSize = 1024 * 1024 * 1024;
        long offset = 0;
        var channel = FileChannel.open(Paths.get(FILE));
        MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, offset, chunkSize);
        while (offset < fileSize) {
            if (buf == null || !buf.hasRemaining()) {
                buf = channel.map(FileChannel.MapMode.READ_ONLY, offset, Math.min(chunkSize, fileSize - offset));
            }
            byte c = buf.get();
            offset++;
            if (c == ';') {
                value.reset();
                parsingStation = false;
            }
            else if (c == '\r') {
            }
            else if (c == '\n') {
                String name = station.toString();
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

        void append(byte b) {
            if (buf.length == len) {
                var newBuf = new byte[(int) (buf.length * 1.5)];
                System.arraycopy(this.buf, 0, newBuf, 0, len);
                this.buf = newBuf;
            }
            buf[len++] = b;
        }

        void reset() {
            len = 0;
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
}
