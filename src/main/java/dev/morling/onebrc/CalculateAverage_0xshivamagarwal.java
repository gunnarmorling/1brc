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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_0xshivamagarwal {
    private static final String FILE = "./measurements.txt";

    private static Map.Entry<String, long[]> parseLine(final String line) {
        var n = line.length();

        var pos = n - 4;
        if (!(line.charAt(pos) == ';' || line.charAt(--pos) == ';' || line.charAt(--pos) == ';')) {
            throw new UnsupportedOperationException("unable to find `;`");
        }

        var v = parseValue(line, pos + 1, n);
        return Map.entry(line.substring(0, pos), new long[]{ v, v, v, 1L });
    }

    private static long parseValue(final String input, final int beginIndex, final int endIndex) {
        long value = 0;

        boolean isNegative = false;
        int i = beginIndex;

        if (input.charAt(i) == '-') {
            ++i;
            isNegative = true;
        }

        for (char c; i < endIndex; ++i) {
            c = input.charAt(i);
            if (c == '.') {
                c = input.charAt(++i);
            }
            value = value * 10 + (c - 48);
        }

        return isNegative ? -value : value;
    }

    private static long[] mergeFunction(final long[] v1, final long[] v2) {
        v1[0] = Math.min(v1[0], v2[0]);
        v1[1] = Math.max(v1[1], v2[1]);
        v1[2] += v2[2];
        v1[3] += v2[3];
        return v1;
    }

    private static String toString(final Map.Entry<String, long[]> entry) {
        var m = entry.getValue();

        return entry.getKey() +
                '=' +
                m[0] / 10.0 +
                '/' +
                Math.round(1.0 * m[2] / m[3]) / 10.0 +
                '/' +
                m[1] / 10.0;
    }

    public static void main(String[] args) throws IOException {
        final String result;

        try (var lines = Files.lines(Paths.get(FILE), UTF_8).parallel()) {
            result = lines
                    .map(CalculateAverage_0xshivamagarwal::parseLine)
                    .parallel()
                    .collect(
                            () -> new HashMap<String, long[]>(10000, 1),
                            (m, e) -> m.merge(e.getKey(), e.getValue(), CalculateAverage_0xshivamagarwal::mergeFunction),
                            (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, CalculateAverage_0xshivamagarwal::mergeFunction)))
                    .entrySet()
                    .parallelStream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(CalculateAverage_0xshivamagarwal::toString)
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        System.out.println(result);
    }
}
