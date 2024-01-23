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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    private static final int EOL = 10;
    private static final int MINUS = 45;
    private static final int SEMICOLON = 59;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        var result = readFile();

        var measurements = getMeasurements(result);

        System.out.println(measurements);
    }

    static record PartialResult(int end, Map<Name, Station> map) {

        void merge(Map<Name, Station> result) {
            map.forEach((name, station) -> result.merge(name, station, Station::merge));
        }
    }

    private static String getMeasurements(Map<Name, Station> result) {
        return result.values().stream().sorted(comparing(Station::getName))
                .map(Station::asString).collect(joining(", ", "{", "}"));
    }

    private static Map<Name, Station> readFile() throws IOException, InterruptedException, ExecutionException {
        Map<Name, Station> result = HashMap.newHashMap(10_000);
        try (var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long consumed = 0;
            long remaining = channel.size();
            while (remaining > 0) {
                var buffer = channel.map(
                        MapMode.READ_ONLY, consumed, Math.min(remaining, Integer.MAX_VALUE));

                if (buffer.remaining() <= 1024) {
                    var partialResult = readChunk(buffer, 0, buffer.remaining());

                    consumed += partialResult.end();
                    remaining -= partialResult.end();

                    partialResult.merge(result);
                }
                else {
                    var chunks = Runtime.getRuntime().availableProcessors();
                    var chunksSize = buffer.remaining() / chunks;
                    var leftover = buffer.remaining() % chunks;

                    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                        var tasks = new ArrayList<Subtask<PartialResult>>(chunks);
                        for (int i = 0; i < chunks; i++) {
                            int start = i * chunksSize;
                            int length = chunksSize + (i < chunks ? leftover : 0);
                            tasks.add(scope.fork(() -> readChunk(
                                    buffer, findStart(buffer, start), start + length)));
                        }
                        scope.join();
                        scope.throwIfFailed();

                        for (var subtask : tasks) {
                            subtask.get().merge(result);
                        }
                        consumed += tasks.getLast().get().end();
                        remaining -= tasks.getLast().get().end();
                    }
                }
            }
        }
        return result;
    }

    private static PartialResult readChunk(ByteBuffer buffer, int start, int end) {
        final byte[] name = new byte[128];
        final byte[] temp = new byte[8];
        final Map<Name, Station> map = HashMap.newHashMap(1000);
        int position = start;
        while (position < end) {
            int semicolon = readName(buffer, position, end - position, name);
            if (semicolon < 0) {
                break;
            }

            int endOfLine = readTemp(buffer, semicolon + 1, end - semicolon - 1, temp);
            if (endOfLine < 0) {
                break;
            }

            map.computeIfAbsent(new Name(name, semicolon - position), Station::new)
                    .add(parseTemp(temp, endOfLine - semicolon - 1));

            // skip end of line
            position = endOfLine + 1;
        }
        return new PartialResult(position, map);
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
    private static int parseTemp(byte[] value, int length) {
        int period = length - 2;
        if (value[0] == MINUS) {
            int left = parseLeft(value, 1, period - 1);
            int right = toInt(value[period + 1]);
            return -(left + right);
        }
        int left = parseLeft(value, 0, period);
        int right = toInt(value[period + 1]);
        return left + right;
    }

    private static int parseLeft(byte[] value, int start, int length) {
        if (length == 1) {
            return toInt(value[start]) * 10;
        }
        // two chars
        int a = toInt(value[start]) * 100;
        int b = toInt(value[start + 1]) * 10;
        return a + b;
    }

    private static int toInt(byte c) {
        return c - 48;
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

        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum;
        private long count;

        Station(Name name) {
            this.name = name;
        }

        String getName() {
            return name.toString();
        }

        void add(int value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        Station merge(Station other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        String asString() {
            return name + "=" + toDouble(min) + "/" + round(mean()) + "/" + toDouble(max);
        }

        private double mean() {
            return toDouble(sum) / count;
        }

        private double toDouble(int value) {
            return value / 10.;
        }

        private double round(double value) {
            return Math.round(value * 10.) / 10.;
        }
    }
}
