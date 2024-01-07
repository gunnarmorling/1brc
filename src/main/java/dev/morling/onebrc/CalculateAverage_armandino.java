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
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_armandino {

    private static final String FILE = "./measurements.txt";

    private static final int MAX_KEY_LENGTH = 100;
    private static final byte SEMICOLON = 59;
    private static final byte NL = 10;
    private static final byte DOT = 46;
    private static final byte MINUS = 45;

    public static void main(String[] args) throws Exception {
        Aggregator aggregator = new Aggregator();
        aggregator.process();
        aggregator.printStats();
    }

    private static class Aggregator {

        private final Map<Integer, Stats> map = new ConcurrentHashMap<>(2048);

        private record Chunk(long start, long end) {
        }

        void process() throws Exception {
            var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
            final Chunk[] chunks = split(channel);
            final Thread[] threads = new Thread[chunks.length];

            for (int i = 0; i < chunks.length; i++) {
                final Chunk chunk = chunks[i];

                threads[i] = Thread.ofVirtual().start(() -> {
                    try {
                        var bb = channel.map(READ_ONLY, chunk.start, chunk.end - chunk.start);
                        process(bb);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            for (Thread t : threads) {
                t.join();
            }
        }

        private static Chunk[] split(final FileChannel channel) throws IOException {
            final long fileSize = channel.size();
            if (fileSize < 10000) {
                return new Chunk[]{ new Chunk(0, fileSize) };
            }

            final int numChunks = 8;
            final long chunkSize = fileSize / numChunks;
            final var chunks = new Chunk[numChunks];

            for (int i = 0; i < numChunks; i++) {
                long start = 0;
                long end = chunkSize;

                if (i > 0) {
                    start = chunks[i - 1].end + 1;
                    end = Math.min(start + chunkSize, fileSize);
                }

                end = end == fileSize ? end : seekNextNewline(channel, end);
                chunks[i] = new Chunk(start, end);
            }
            return chunks;
        }

        private static long seekNextNewline(final FileChannel channel, final long end) throws IOException {
            var bb = ByteBuffer.allocate(MAX_KEY_LENGTH);
            channel.position(end).read(bb);

            for (int i = 0; i < bb.limit(); i++) {
                if (bb.get(i) == NL) {
                    return end + i;
                }
            }

            throw new IllegalStateException("Couldn't find next newline");
        }

        private void process(final ByteBuffer bb) {
            final var sample = new Sample();
            var isKey = true;

            for (long i = 0, sz = bb.limit(); i < sz; i++) {

                final byte b = bb.get();

                if (b == SEMICOLON) {
                    isKey = false;
                }
                else if (b == NL) {
                    isKey = true;
                    addSample(sample);
                    sample.reset();
                }
                else if (isKey) {
                    sample.pushKey(b);
                }
                else if (b == DOT) {
                    // skip
                }
                else if (b == MINUS) {
                    sample.sign = -1;
                }
                else {
                    sample.pushMeasurement(b);
                }
            }
        }

        private void addSample(final Sample sample) {
            final Stats stats = map.computeIfAbsent(sample.keyHash,
                    k -> new Stats(new String(sample.keyBytes, 0, sample.keyLength, UTF_8)));

            final var val = sample.getMeasurement();

            if (val < stats.min)
                stats.min = val;

            if (val > stats.max)
                stats.max = val;

            stats.sum += val;
            stats.count++;
        }

        void printStats() {
            var sorted = new ArrayList<>(map.values());
            Collections.sort(sorted);

            int size = sorted.size();

            System.out.print('{');

            for (Stats stats : sorted) {
                stats.print(System.out);
                if (--size > 0) {
                    System.out.print(", ");
                }
            }
            System.out.println('}');
        }
    }

    private static class Stats implements Comparable<Stats> {
        private final String city;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum;
        private int count;

        private Stats(String city) {
            this.city = city;
        }

        @Override
        public int compareTo(final Stats o) {
            return city.compareTo(o.city);
        }

        void print(final PrintStream out) {
            out.print(city);
            out.print('=');
            out.print(round(min / 10f));
            out.print('/');
            out.print(round((sum / 10f) / count));
            out.print('/');
            out.print(round(max) / 10f);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class Sample {
        private final byte[] keyBytes = new byte[MAX_KEY_LENGTH];
        private int keyLength;
        private int keyHash;
        private int measurement;
        private int sign = 1;

        void pushKey(byte b) {
            keyBytes[keyLength++] = b;
            keyHash = 31 * keyHash + b;
        }

        void pushMeasurement(byte b) {
            final int i = b - '0';
            measurement = measurement * 10 + i;
        }

        int getMeasurement() {
            return sign * measurement;
        }

        void reset() {
            keyHash = 0;
            keyLength = 0;
            measurement = 0;
            sign = 1;
        }
    }
}
