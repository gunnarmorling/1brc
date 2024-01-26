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

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

public class CalculateAverage_armandino {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final int NUM_CHUNKS = Math.max(8, Runtime.getRuntime().availableProcessors());
    private static final int INITIAL_MAP_CAPACITY = 8192;
    private static final byte SEMICOLON = 59;
    private static final byte NL = 10;
    private static final byte DOT = 46;
    private static final byte MINUS = 45;
    private static final byte ZERO_DIGIT = 48;
    private static final int PRIME = 1117;
    private static final Unsafe UNSAFE = getUnsafe();

    public static void main(String[] args) throws Exception {
        var channel = FileChannel.open(FILE, StandardOpenOption.READ);

        var results = Arrays.stream(split(channel)).parallel()
                .map(chunk -> new ChunkProcessor().process(chunk.start, chunk.end))
                .flatMap(SimpleMap::stream)
                .collect(toMap(Stats::getKey, s -> s, CalculateAverage_armandino::mergeStats, TreeMap::new));

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
        private final SimpleMap map = new SimpleMap(INITIAL_MAP_CAPACITY);

        private SimpleMap process(final long chunkStart, final long chunkEnd) {
            long i = chunkStart;
            while (i < chunkEnd) {
                final long keyAddress = i;
                int keyHash = 0;
                int measurement = 0;
                byte b;

                while ((b = UNSAFE.getByte(i++)) != SEMICOLON) {
                    keyHash = PRIME * keyHash + b;
                }

                final int keyLength = (int) (i - keyAddress - 1);

                if ((b = UNSAFE.getByte(i++)) == MINUS) {
                    while ((b = UNSAFE.getByte(i++)) != DOT) {
                        measurement = measurement * 10 + b - ZERO_DIGIT;
                    }

                    b = UNSAFE.getByte(i);
                    measurement = measurement * 10 + b - ZERO_DIGIT;
                    measurement = -measurement;
                    i += 2;
                }
                else {
                    measurement = b - ZERO_DIGIT; // D1
                    b = UNSAFE.getByte(i); // dot or D2

                    if (b == DOT) {
                        measurement = measurement * 10 + UNSAFE.getByte(i + 1) - ZERO_DIGIT; // F
                        i += 3;
                    }
                    else {
                        measurement = measurement * 10 + b - ZERO_DIGIT; // D2
                        measurement = measurement * 10 + UNSAFE.getByte(i + 2) - ZERO_DIGIT; // F
                        i += 4; // skip NL
                    }
                }

                final Stats stats = map.putStats(keyHash, keyAddress, keyLength);
                stats.min = Math.min(stats.min, measurement);
                stats.max = Math.max(stats.max, measurement);
                stats.sum += measurement;
                stats.count++;
            }

            return map;
        }
    }

    private static class Stats implements Comparable<Stats> {
        private String key;
        private final long keyAddress;
        private final int keyLength;
        private final int keyHash;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int count;
        private long sum;

        private Stats(long keyAddress, int keyLength, int keyHash) {
            this.keyAddress = keyAddress;
            this.keyLength = keyLength;
            this.keyHash = keyHash;
        }

        String getKey() {
            if (key == null) {
                var keyBytes = new byte[keyLength];
                UNSAFE.copyMemory(null, keyAddress, keyBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, keyLength);
                key = new String(keyBytes, 0, keyLength, UTF_8);
            }
            return key;
        }

        @Override
        public int compareTo(final Stats o) {
            return getKey().compareTo(o.getKey());
        }

        void print(final PrintStream out) {
            out.print(key);
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
        long start = channel.map(READ_ONLY, 0, fileSize, Arena.global()).address();
        final long endAddress = start + fileSize;
        if (fileSize < 10000) {
            return new Chunk[]{ new Chunk(start, endAddress) };
        }

        final long chunkSize = fileSize / NUM_CHUNKS;
        final var chunks = new Chunk[NUM_CHUNKS];
        long end = start + chunkSize;

        for (int i = 0; i < NUM_CHUNKS; i++) {
            if (i > 0) {
                start = chunks[i - 1].end;
                end = Math.min(start + chunkSize, endAddress);
            }
            if (end < endAddress) {
                while (UNSAFE.getByte(end) != NL) {
                    end++;
                }
                end++;
            }
            chunks[i] = new Chunk(start, end);
        }
        return chunks;
    }

    private record Chunk(long start, long end) {
    }

    private static Unsafe getUnsafe() {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            return (Unsafe) unsafe.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class SimpleMap {
        private Stats[] table;

        SimpleMap(int initialCapacity) {
            table = new Stats[initialCapacity];
        }

        Stream<Stats> stream() {
            return Arrays.stream(table).filter(Objects::nonNull);
        }

        Stats putStats(final int keyHash, final long keyAddress, final int keyLength) {
            final int pos = (table.length - 1) & keyHash;

            Stats stats = table[pos];
            if (stats == null)
                return createAt(table, keyAddress, keyLength, keyHash, pos);
            if (stats.keyHash == keyHash && keysEqual(stats, keyAddress, keyLength))
                return stats;

            int i = pos;
            while (++i < table.length) {
                stats = table[i];
                if (stats == null)
                    return createAt(table, keyAddress, keyLength, keyHash, i);
                if (keyHash == stats.keyHash && keysEqual(stats, keyAddress, keyLength))
                    return stats;
            }

            i = pos;
            while (i-- > 0) {
                stats = table[i];
                if (stats == null)
                    return createAt(table, keyAddress, keyLength, keyHash, i);
                if (keyHash == stats.keyHash && keysEqual(stats, keyAddress, keyLength))
                    return stats;
            }
            resize();
            return putStats(keyHash, keyAddress, keyLength);
        }

        private static Stats createAt(Stats[] table, long keyAddress, int keyLength, int key, int i) {
            Stats stats = new Stats(keyAddress, keyLength, key);
            table[i] = stats;
            return stats;
        }

        private static boolean keysEqual(Stats stats, long keyAddress, final int keyLength) {
            // credit: abeobk
            long xsum = 0;
            int n = keyLength & 0xF8;
            for (int i = 0; i < n; i += 8) {
                xsum |= (UNSAFE.getLong(stats.keyAddress + i) ^ UNSAFE.getLong(keyAddress + i));
            }
            return xsum == 0;
        }

        private void resize() {
            var copy = new SimpleMap(table.length * 2);
            for (Stats s : table) {
                if (s != null) {
                    final int pos = (copy.table.length - 1) & s.keyHash;
                    int i = pos;
                    if (copy.table[i] == null) {
                        copy.table[i] = s;
                        continue;
                    }
                    while (i < copy.table.length && copy.table[i] != null) {
                        i++;
                    }
                    if (i == copy.table.length) {
                        i = pos;
                        while (i >= 0 && copy.table[i] != null) {
                            i--;
                        }
                    }
                    if (i < 0) {
                        // if we reach here it's a bug!
                        throw new IllegalStateException("table is full");
                    }
                    copy.table[i] = s;
                }
            }
            table = copy.table;
        }
    }
}
