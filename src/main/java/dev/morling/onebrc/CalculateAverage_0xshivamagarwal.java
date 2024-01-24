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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_0xshivamagarwal {
    private static final Path FILE = Path.of("./measurements.txt");
    private static final byte COLON = ';';
    private static final byte NEW_LINE = '\n';
    private static final byte HYPHEN = '-';
    private static final byte DOT = '.';
    private static final int NO_OF_THREADS = Runtime.getRuntime().availableProcessors();

    private static long[] mergeFn(final long[] v1, final long[] v2) {
        v1[0] = Math.min(v1[0], v2[0]);
        v1[1] = Math.max(v1[1], v2[1]);
        v1[2] += v2[2];
        v1[3] += v2[3];
        return v1;
    }

    private static String toString(final Map.Entry<String, long[]> entry) {
        var m = entry.getValue();

        return entry.getKey()
                + '='
                + m[0] / 10.0
                + '/'
                + Math.round(1.0 * m[2] / m[3]) / 10.0
                + '/'
                + m[1] / 10.0;
    }

    private static Map<String, long[]> parseData(
                                                 final MemorySegment data, long offset, final long limit) {
        var map = new HashMap<String, long[]>(10000, 1);
        var sep = false;
        var neg = false;
        var key = new byte[100];
        var len = 0;
        var val = 0;

        while (offset < limit) {
            var b = data.get(JAVA_BYTE, offset++);
            if (sep) {
                if (b == NEW_LINE) {
                    val = neg ? -val : val;
                    map.merge(
                            new String(key, 0, len),
                            new long[]{ val, val, val, 1 },
                            CalculateAverage_0xshivamagarwal::mergeFn);
                    sep = false;
                    neg = false;
                    len = 0;
                    val = 0;
                }
                else if (b == HYPHEN) {
                    neg = true;
                }
                else if (b != DOT) {
                    val = val * 10 + (b - 48);
                }
            }
            else if (b == COLON) {
                sep = true;
            }
            else {
                key[len++] = b;
            }
        }

        return map;
    }

    public static void main(String[] args) throws IOException {
        final String result;

        try (var channel = FileChannel.open(FILE, READ);
                var arena = Arena.ofShared()) {
            var data = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            var chunkSize = data.byteSize() / NO_OF_THREADS;
            var chunks = new long[NO_OF_THREADS + 1];
            chunks[NO_OF_THREADS] = data.byteSize();

            for (int i = 1; i < NO_OF_THREADS; ++i) {
                var chunkPos = i * chunkSize;

                while (data.get(JAVA_BYTE, chunkPos++) != NEW_LINE) {
                }

                chunks[i] = chunkPos;
            }

            result = IntStream.range(0, NO_OF_THREADS)
                    .mapToObj(i -> parseData(data, chunks[i], chunks[i + 1]))
                    .parallel()
                    .reduce(
                            (m1, m2) -> {
                                m2.forEach((k, v) -> m1.merge(k, v, CalculateAverage_0xshivamagarwal::mergeFn));
                                return m1;
                            })
                    .map(
                            map -> map.entrySet().parallelStream()
                                    .sorted(Map.Entry.comparingByKey())
                                    .map(CalculateAverage_0xshivamagarwal::toString)
                                    .collect(Collectors.joining(", ", "{", "}")))
                    .orElse(null);
        }

        System.out.println(result);
    }
}
