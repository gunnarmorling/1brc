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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    private static final int EOL = 10;
    private static final int MINUS = 45;
    private static final int SEMICOLON = 59;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Map<Name, Station> map = new ConcurrentHashMap<>();

        readFile((station, value) -> map.computeIfAbsent(station, Station::new).add(value));

        var measurements = map.values().stream().sorted(comparing(Station::getName))
                .map(Station::asString).collect(joining(", ", "{", "}"));

        System.out.println(measurements);
    }

    static interface Consumer {
        void accept(Name station, double value);
    }

    private static void readFile(Consumer consumer) throws IOException, InterruptedException, ExecutionException {
        try (var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long consumed = 0;
            long remaining = channel.size();
            while (remaining > 0) {
                var buffer = channel.map(
                        MapMode.READ_ONLY, consumed, Math.min(remaining, Integer.MAX_VALUE));

                if (buffer.remaining() <= 1024) {
                    int last = readChunk(buffer, consumer, 0, buffer.remaining());

                    consumed += last;
                    remaining -= last;
                }
                else {
                    var chunks = Runtime.getRuntime().availableProcessors();
                    var chunksSize = buffer.remaining() / chunks;
                    var leftover = buffer.remaining() % chunks;

                    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                        var tasks = new ArrayList<Subtask<Integer>>(chunks);
                        for (int c = 0; c < chunks; c++) {
                            int start = c * chunksSize;
                            int end = start + chunksSize + (c == chunks - 1 ? leftover : 0);
                            tasks.add(scope.fork(() -> readChunk(buffer, consumer, start, end)));
                        }
                        scope.join();
                        scope.throwIfFailed();

                        consumed += tasks.getLast().get();
                        remaining -= tasks.getLast().get();
                    }
                }
            }
        }
    }

    private static int readChunk(ByteBuffer buffer, Consumer consumer, int start, int end) {
        final byte[] name = new byte[128];
        final byte[] temp = new byte[8];
        int last = findStart(buffer, start);
        while (last < end) {
            int semicolon = readName(buffer, last, end - last, name);
            if (semicolon < 0) {
                break;
            }

            int endOfLine = readTemp(buffer, semicolon + 1, end - semicolon - 1, temp);
            if (endOfLine < 0) {
                break;
            }

            consumer.accept(
                    new Name(name, semicolon - last),
                    parseTemp(temp, endOfLine - semicolon - 1));

            // skip end of line
            last = endOfLine + 1;
        }
        return last;
    }

    private static int findStart(ByteBuffer buffer, int start) {
        if (start > 0 && buffer.get(start - 1) != EOL) {
            for (int i = start - 2; i > 0; i--) {
                byte b = buffer.get(i);
                if (b == EOL) {
                    return i + 1;
                }
            }
        }
        return start;
    }

    private static int readName(ByteBuffer buffer, int offset, int length, byte[] name) {
        return readUntil(buffer, offset, length, name, SEMICOLON);
    }

    private static int readTemp(ByteBuffer buffer, int offset, int length, byte[] percentage) {
        return readUntil(buffer, offset, length, percentage, EOL);
    }

    private static int readUntil(ByteBuffer buffer, int offset, int length, byte[] array, int target) {
        for (int i = 0; i < length; i++) {
            byte b = buffer.get(i + offset);
            if (b == target) {
                return i + offset;
            }
            array[i] = b;
        }
        return -1;
    }

    // non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
    private static double parseTemp(byte[] value, int length) {
        int period = length - 2;
        if (value[0] == MINUS) {
            double left = parseLeft(value, 1, period);
            double right = parseRight(value[period + 1]);
            return -(left + right);
        }
        double left = parseLeft(value, 0, period);
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

    static final class Name {

        private final byte[] value;

        Name(byte[] source, int length) {
            value = new byte[length];
            System.arraycopy(source, 0, value, 0, length);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Name other) {
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

        private final Name name;

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        private final Lock lock = new ReentrantLock();

        Station(Name name) {
            this.name = name;
        }

        String getName() {
            return name.toString();
        }

        void add(double value) {
            lock.lock();
            try {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                count++;
            }
            finally {
                lock.unlock();
            }
        }

        public String asString() {
            return name + "=" + round(min) + "/" + round(mean()) + "/" + round(max);
        }

        private double mean() {
            return round(sum) / count;
        }

        private double round(double value) {
            return Math.round(value * 10.) / 10.;
        }
    }
}
