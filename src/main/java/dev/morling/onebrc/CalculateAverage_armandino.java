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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

public class CalculateAverage_armandino {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final int NUM_CHUNKS = Runtime.getRuntime().availableProcessors();
    private static final int MAX_KEY_LENGTH = 100;
    private static final byte SEMICOLON = 59;
    private static final byte NL = 10;
    private static final byte DOT = 46;
    private static final byte MINUS = 45;
    private static final byte ZERO_DIGIT = 48;

    public static void main(String[] args) throws Exception {
        var channel = FileChannel.open(FILE, StandardOpenOption.READ);

        var results = Arrays.stream(split(channel)).parallel()
                .map(chunk -> {
                    try {
                        var bb = channel.map(READ_ONLY, chunk.start, chunk.end - chunk.start);
                        return new ChunkProcessor().process(bb);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(Collection::stream)
                .collect(toMap(s -> s.city, s -> s, CalculateAverage_armandino::mergeStats, TreeMap::new));

        print(results.values());
    }

    private static Stats mergeStats(final Stats x, final Stats y) {
        x.min = Math.min(x.min, y.min);
        x.max = Math.max(x.max, y.max);
        x.count += y.count;
        x.sum += y.sum;
        return x;
    }

    private static class ChunkProcessor {
        private final List<Stats> list = new ArrayList<>(512);
        private final Map<Integer, Stats> map = new HashMap<>(2048);

        private List<Stats> process(final ByteBuffer bb) {
            // not sure if this is faster... from royvanrijn
            bb.order(ByteOrder.nativeOrder());

            final byte[] keyBytes = new byte[MAX_KEY_LENGTH];
            int keyLength = 0;
            int keyHash = 0;
            int measurement = 0;
            boolean negative = false;

            while (bb.hasRemaining()) {
                byte b;
                while ((b = bb.get()) != SEMICOLON) {
                    keyBytes[keyLength++] = b;
                    keyHash = 31 * keyHash + b;
                }
                while ((b = bb.get()) != NL) {
                    if (b == MINUS) {
                        negative = true;
                    }
                    else if (b != DOT) {
                        measurement = measurement * 10 + b - ZERO_DIGIT;
                    }
                }

                // add/update stats
                Stats stats = map.get(keyHash);

                if (stats == null) {
                    stats = new Stats(new String(keyBytes, 0, keyLength, UTF_8));
                    map.put(keyHash, stats);
                    list.add(stats);
                }

                // update stats
                final var val = negative ? -measurement : measurement;
                stats.min = Math.min(stats.min, val);
                stats.max = Math.max(stats.max, val);
                stats.sum += val;
                stats.count++;

                // reset
                keyHash = 0;
                keyLength = 0;
                measurement = 0;
                negative = false;
            }
            return list;
        }
    }

    private static class Stats implements Comparable<Stats> {
        private final String city;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int count;
        private long sum;

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

    private static void print(final Collection<Stats> sorted) {
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

    private static Chunk[] split(final FileChannel channel) throws IOException {
        final long fileSize = channel.size();
        if (fileSize < 10000) {
            return new Chunk[]{ new Chunk(0, fileSize) };
        }

        final int numChunks = NUM_CHUNKS;
        final long chunkSize = fileSize / numChunks;
        final var chunks = new Chunk[numChunks];
        long start = 0;
        long end = chunkSize;

        for (int i = 0; i < numChunks; i++) {
            if (i > 0) {
                start = chunks[i - 1].end;
                end = Math.min(start + chunkSize, fileSize);
            }

            if (end < fileSize) {
                var bb = ByteBuffer.allocate(MAX_KEY_LENGTH);
                channel.position(end).read(bb);

                for (int j = 0; j < bb.limit(); j++) {
                    if (bb.get(j) == NL) {
                        end = end + j + 1;
                        break;
                    }
                }
            }
            chunks[i] = new Chunk(start, end);
        }
        return chunks;
    }

    private record Chunk(long start, long end) {
    }
}
