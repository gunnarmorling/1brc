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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    private static final int MIN_CHUNK_SIZE = 1024;
    private static final int MAX_NAME_LENGTH = 128;
    private static final int MAX_TEMP_LENGTH = 8;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        System.out.println(readFile());
    }

    private static Map<String, Station> readFile() throws IOException, InterruptedException, ExecutionException {
        Map<String, Station> result = new TreeMap<>();
        try (var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long consumed = 0;
            long remaining = channel.size();
            while (remaining > 0) {
                var buffer = channel.map(
                        MapMode.READ_ONLY, consumed, Math.min(remaining, Integer.MAX_VALUE));

                int chunks = Runtime.getRuntime().availableProcessors();
                int chunkSize = buffer.remaining() / chunks;
                int leftover = buffer.remaining() % chunks;
                if (chunkSize < MIN_CHUNK_SIZE) {
                    var partialResult = new Chunk(buffer, 0, buffer.remaining()).read();

                    consumed += partialResult.end();
                    remaining -= partialResult.end();

                    partialResult.merge(result);
                }
                else {
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                        var tasks = new ArrayList<Subtask<PartialResult>>(chunks);
                        for (int i = 0; i < chunks; i++) {
                            int start = i * chunkSize;
                            int length = chunkSize + (i < chunks ? leftover : 0);
                            tasks.add(scope.fork(new Chunk(buffer, start, length)::read));
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

    static final class Chunk {

        private static final int EOL = 10;
        private static final int MINUS = 45;
        private static final int SEMICOLON = 59;

        final ByteBuffer buffer;
        final int start;
        final int end;

        final byte[] name = new byte[MAX_NAME_LENGTH];
        final byte[] temp = new byte[MAX_TEMP_LENGTH];
        final Stations stations = new Stations();

        int hash;

        Chunk(ByteBuffer buffer, int start, int length) {
            this.buffer = buffer;
            this.start = findStart(buffer, start);
            this.end = start + length;
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

        PartialResult read() {
            int position = start;
            while (position < end) {
                int semicolon = readName(position, end - position);
                if (semicolon < 0) {
                    break;
                }

                int endOfLine = readTemp(semicolon + 1, end - semicolon - 1);
                if (endOfLine < 0) {
                    break;
                }

                stations.find(name, semicolon - position, hash)
                        .add(parseTemp(temp, endOfLine - semicolon - 1));

                // skip end of line
                position = endOfLine + 1;
            }
            return new PartialResult(position, stations.buckets);
        }

        private int readName(int offset, int length) {
            hash = 1;
            for (int i = 0; i < length; i++) {
                byte b = buffer.get(i + offset);
                if (b == SEMICOLON) {
                    return i + offset;
                }
                name[i] = b;
                hash = 31 * hash + b;
            }
            return -1;
        }

        private int readTemp(int offset, int length) {
            for (int i = 0; i < length; i++) {
                byte b = buffer.get(i + offset);
                if (b == EOL) {
                    return i + offset;
                }
                temp[i] = b;
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
    }

    static final class Stations {

        private static final int NUMBER_OF_BUCKETS = 1000;
        private static final int BUCKET_SIZE = 50;

        final Station[][] buckets = new Station[NUMBER_OF_BUCKETS][BUCKET_SIZE];

        Station find(byte[] name, int length, int hash) {
            var bucket = buckets[Math.abs(hash % NUMBER_OF_BUCKETS)];
            for (int i = 0; i < BUCKET_SIZE; i++) {
                if (bucket[i] == null) {
                    bucket[i] = new Station(name, length, hash);
                    return bucket[i];
                }
                else if (bucket[i].sameName(length, hash)) {
                    return bucket[i];
                }
            }
            throw new IllegalStateException("no more space left");
        }
    }

    static final class Station {

        private final byte[] name;
        private final int hash;

        private int min = 1000;
        private int max = -1000;
        private int sum;
        private long count;

        Station(byte[] source, int length, int hash) {
            name = new byte[length];
            System.arraycopy(source, 0, name, 0, length);
            this.hash = hash;
        }

        String getName() {
            return new String(name, StandardCharsets.UTF_8);
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

        @Override
        public String toString() {
            return toDouble(min) + "/" + round(mean()) + "/" + toDouble(max);
        }

        boolean sameName(int length, int hash) {
            return name.length == length && this.hash == hash;
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

    static record PartialResult(int end, Station[][] stations) {

        void merge(Map<String, Station> result) {
            for (Station[] bucket : stations) {
                for (Station station : bucket) {
                    if (station != null) {
                        result.merge(station.getName(), station, Station::merge);
                    }
                }
            }
        }
    }
}
