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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    static interface Consumer {
        void accept(SafeString station, double value);
    }

    public static void main(String[] args) throws IOException {
        Map<SafeString, Station> map = new HashMap<>();

        readFile((station, value) -> {
            map.computeIfAbsent(station, Station::new).add(value);
        });

        var measurements = map.values().stream().sorted(comparing(Station::getName))
                .map(Station::asString).collect(joining(", ", "{", "}"));

        System.out.println(measurements);
    }

    private static void readFile(Consumer consumer) throws IOException {
        try (var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            for (long consumed = 0; channel.size() - consumed > 0;) {
                var buffer = ByteBuffer.allocate(1024 * 1024);
                var readed = channel.read(buffer);
                buffer.flip();

                var last = 0;
                var next = 0;
                while (true) {
                    last = next;
                    next = findNextEndOfLine(buffer, readed, last);
                    if (next < 0) {
                        break;
                    }
                    byte[] line = new byte[next - last];
                    buffer.get(line, 0, line.length);

                    var semicolon = findChar(line, 59);

                    var station = Arrays.copyOfRange(line, 0, semicolon);
                    var value = Arrays.copyOfRange(line, semicolon + 1, line.length);

                    consumer.accept(new SafeString(station), parseDouble(value));

                    // consume \n
                    buffer.get();
                    next = next + 1;
                }
                // set position of last line processed
                consumed += last;
                channel.position(consumed);
            }
        }
    }

    private static int findChar(byte[] line, int c) {
        for (int i = 0; i < line.length; i++) {
            if (line[i] == c) {
                return i;
            }
        }
        return -1;
    }

    private static int findNextEndOfLine(ByteBuffer buffer, int readed, int last) {
        for (int i = last; i < readed; i++) {
            if (buffer.get(i) == 10) {
                return i;
            }
        }
        return -1;
    }

    // non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
    private static double parseDouble(byte[] value) {
        var period = findChar(value, 46);
        if (value[0] == 45) {
            var left = parseLeft(Arrays.copyOfRange(value, 1, period));
            var right = parseRight(Arrays.copyOfRange(value, period + 1, value.length));
            return -(left + right);
        }
        var left = parseLeft(Arrays.copyOfRange(value, 0, period));
        var right = parseRight(Arrays.copyOfRange(value, period + 1, value.length));
        return left + right;
    }

    private static double parseLeft(byte[] left) {
        if (left.length == 1) {
            return charToDouble(left[0]);
        }
        // two chars
        var a = charToDouble(left[0]) * 10.;
        var b = charToDouble(left[1]);
        return a + b;
    }

    private static double parseRight(byte[] right) {
        var a = charToDouble(right[0]);
        return a / 10.;
    }

    private static double charToDouble(byte c) {
        return (double) c - 48;
    }

    static final class SafeString {

        private final byte[] value;

        SafeString(byte[] value) {
            this.value = value;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SafeString other) {
                return Arrays.equals(value, other.value);
            }
            return false;
        }

        @Override
        public String toString() {
            return new String(value, StandardCharsets.UTF_8);
        }
    }

    static final class Station {

        private final SafeString name;

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        Station(SafeString name) {
            this.name = name;
        }

        String getName() {
            return name.toString();
        }

        void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        public String asString() {
            return name + "=" + round(min) + "/" + round(mean()) + "/" + round(max);
        }

        private double mean() {
            return round(this.sum) / this.count;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
