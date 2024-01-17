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
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    private static final int CHUNK_SIZE = 1024 * 1024 * 4;

    private static final int EOL = 10;
    private static final int MINUS = 45;
    private static final int PERIOD = 46;
    private static final int SEMICOLON = 59;

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
        final byte[] line = new byte[256];
        try (var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            for (long consumed = 0; channel.size() - consumed > 0;) {
                var buffer = channel.map(MapMode.READ_ONLY, consumed, Math.min(channel.size() - consumed, CHUNK_SIZE));

                int last = 0;
                int next = 0;
                while (true) {
                    last = next;
                    next = findNextEndOfLine(buffer, last);
                    if (next < 0) {
                        break;
                    }
                    int length = next - last;
                    buffer.get(line, 0, length);

                    int semicolon = findChar(line, 0, SEMICOLON);

                    consumer.accept(
                            new SafeString(line, 0, semicolon),
                            parseDouble(line, semicolon + 1));

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

    private static int findChar(byte[] line, int offset, int c) {
        for (int i = offset; i < line.length; i++) {
            if (line[i] == c) {
                return i;
            }
        }
        return -1;
    }

    private static int findNextEndOfLine(ByteBuffer buffer, int last) {
        for (int i = last; i < buffer.remaining(); i++) {
            if (buffer.get(i) == EOL) {
                return i;
            }
        }
        return -1;
    }

    // non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
    private static double parseDouble(byte[] value, int offset) {
        int period = findChar(value, offset, PERIOD);
        if (value[offset] == MINUS) {
            double left = parseLeft(value, offset + 1, period);
            double right = parseRight(value[period + 1]);
            return -(left + right);
        }
        double left = parseLeft(value, offset, period);
        double right = parseRight(value[period + 1]);
        return left + right;
    }

    private static double parseLeft(byte[] value, int start, int end) {
        if (end - start == 1) {
            return charToDouble(value[start]);
        }
        // two chars
        double a = charToDouble(value[start]) * 10.;
        double b = charToDouble(value[start + 1]);
        return a + b;
    }

    private static double parseRight(byte right) {
        double a = charToDouble(right);
        return a / 10.;
    }

    private static double charToDouble(byte c) {
        return (double) c - 48;
    }

    static final class SafeString {

        private final byte[] value;

        SafeString(byte[] value, int offset, int length) {
            this.value = new byte[length];
            System.arraycopy(value, offset, this.value, 0, length);
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
