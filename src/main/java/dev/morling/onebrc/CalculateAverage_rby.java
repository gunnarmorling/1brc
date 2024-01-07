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

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class CalculateAverage_rby {

    private static final String FILE = "./measurements.txt";
    // private static final int CHUNK_SIZE = 8 * 1024 * 1024;
    private static final int CHUNK_SIZE = 32 << 20;

    /**
     * Computes good enough partitions which end on a newline
     */
    static long[] cuts(Path p, int workers) throws IOException {
        var channel = (FileChannel) Files.newByteChannel(p, EnumSet.of(StandardOpenOption.READ));
        final long size = channel.size();

        if (size < 10000l) {
            return new long[]{ 0l, size };
        }
        long chunk = size / workers;
        long position = size - chunk;

        long[] cuts = new long[workers + 1];
        cuts[workers] = size;
        // 1024 should cover enough to catch a newline
        var buf = ByteBuffer.allocateDirect(1024);
        byte[] bytes = new byte[1024];

        while (workers-- > 0) {
            var read = channel.read(buf, position);
            buf.flip();
            buf.get(bytes, 0, read);
            var nextNL = position;
            while (read-- > 0) {
                if (bytes[read] == '\n') {
                    nextNL += read;
                    cuts[workers] = nextNL;
                    break;
                }
            }
            position -= chunk;
            buf.rewind();
        }
        cuts[0] = 0L;
        return cuts;

    }

    public static void main(String[] args) throws IOException {
        var p = Paths.get(FILE);
        var cpus = Runtime.getRuntime().availableProcessors();
        final long[] cuts = cuts(p, cpus);

        var stats = IntStream.range(0, cuts.length - 1)
                .parallel()
                .mapToObj((i) -> stats(p, cuts[i], cuts[i + 1]))
                .reduce(Stats.IDENTITY, Stats::combine);

        stats.print();

    }

    static record Stats(Map<String, Integer> indexes, int nextIx, int[] stats) {
        private final static Stats IDENTITY = new Stats(new HashMap(), 0, new int[0]);
        // not much optimization needed here
        Stats combine(Stats other) {
            if (this == IDENTITY) return other;
            if (other == IDENTITY) return this;
            var myNextIx = nextIx; 
            for(var e : other.indexes.entrySet()) {
                int ix;
                var ixi = indexes.get(e.getKey());
                if ( ixi == null) {
                    ix = myNextIx++ * 4;
                } else {
                     ix = ixi.intValue() * 4;
                }
                var oix = e.getValue() * 4;
                stats[ix] = Math.min(stats[ix], other.stats[oix]);
                stats[ix + 1] = Math.max(stats[ix + 1], other.stats[oix + 1]);
                stats[ix + 2] += other.stats[oix + 2];
                stats[ix + 3] += other.stats[oix + 3];
            }
            return new Stats(indexes, myNextIx, stats);
        }
        // or here
        void print() {
            var iter = new TreeMap<>(indexes).entrySet().iterator();
            System.out.print("{");
            if (iter.hasNext()) {
                var e = iter.next();
                var ix = e.getValue().intValue() * 4;
                var avg = Math.round(stats[ix + 2]/((double)stats[ix+3]))/10.0;
                System.out.print(e.getKey() + "="
                        + (stats[ix]/10.0) + "/"
                        + avg + "/"
                        + (stats[ix + 1]/10.0));
            }
            while(iter.hasNext()) {
                var e = iter.next();
                var ix = e.getValue().intValue() * 4;
                var avg = Math.round(stats[ix + 2]/((double)stats[ix+3]))/10.0;
                System.out.print(", " + e.getKey() + "="
                        + (stats[ix]/10.0) + "/"
                        + avg + "/"
                        + (stats[ix + 1]/10.0)) ;
            }
            System.out.println("}");
        }
    }

    static final int MAX_CITIES = 1000;
    static final int ARRAY_SIZE = 1 << 20;

    static Stats stats(Path p, long start, long end) {
        int nextCityIx = 0;
        var cityIndexes = new HashMap<String, Integer>(MAX_CITIES, 1.0f);
        int[] stats = new int[MAX_CITIES * 4];
        for (int i = 0; i < MAX_CITIES; i++) {
            stats[i * 4] = Integer.MAX_VALUE;
            stats[i * 4 + 1] = Integer.MIN_VALUE;
        }

        try {
            final var channel = (FileChannel) Files.newByteChannel(p, EnumSet.of(StandardOpenOption.READ));
            channel.position(start);
            var offset = start;
            final byte[] array = new byte[ARRAY_SIZE];
            // the next expected char, the most simple stateMachine
            char nextChar = ';';
            // good enough for a city name, or a double
            byte[] strbuff = new byte[128];
            int strbuffIx = 0;
            int cityIndex = 0;
            final var buffer = ByteBuffer.allocateDirect(CHUNK_SIZE);

            while (offset < end) {
                final int limit = channel.read(buffer);
                if (limit <= 0)
                    break;
                offset += limit;
                int totalRead = 0;
                buffer.flip();
                while (totalRead < limit) {
                    int read = Math.min(array.length, limit - totalRead);
                    buffer.get(array, 0, read);
                    totalRead += read;

                    for (int i = 0; i < read; i++) {
                        if (nextChar == '\n' && array[i] == '.')
                            continue;
                        strbuff[strbuffIx++] = array[i];
                        if (array[i] == nextChar) {
                            var str = new String(strbuff, 0, strbuffIx - 1, "utf8");
                            strbuffIx = 0;
                            switch (nextChar) {
                                case ';':
                                    nextChar = '\n';
                                    var mbCityIx = cityIndexes.get(str);
                                    if (mbCityIx == null) {
                                        cityIndex = nextCityIx;
                                        cityIndexes.put(str, nextCityIx++);
                                        if (nextCityIx * 4 >= stats.length) {
                                            var newStats = Arrays.copyOf(stats, stats.length * 2);
                                            for (int j = stats.length; j < newStats.length; j += 4) {
                                                newStats[j] = Integer.MAX_VALUE;
                                                newStats[j + 1] = Integer.MIN_VALUE;
                                            }
                                            stats = newStats;
                                        }
                                    }
                                    else {
                                        cityIndex = mbCityIx.intValue();
                                    }
                                    break;
                                case '\n':
                                    nextChar = ';';
                                    int temp = Integer.parseInt(str);
                                    var ix = cityIndex * 4;
                                    if (temp < stats[ix])
                                        stats[ix] = temp;
                                    if (temp > stats[ix + 1])
                                        stats[ix + 1] = temp;
                                    stats[ix + 2] += temp;
                                    stats[ix + 3]++;

                                    break;
                                default:
                            }

                        }
                    }
                }
                buffer.rewind();
            }
            return new Stats(cityIndexes, nextCityIx, stats);
        }
        catch (IOException err) {
            return null;
        }
    }
}
