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
import java.nio.file.Path;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_kuduwa_keshavram {

    private static final String FILE = "./measurements.txt";
    private static final Function<String, String> KEY_MAPPER = line -> {
        int pivot = line.indexOf(";");
        return line.substring(0, pivot);
    };
    private static final ToDoubleFunction<String> VALUE_MAPPER = line -> {
        int pivot = line.indexOf(";");
        return toDouble(line.substring(pivot + 1));
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        try (Stream<String> lines = Files.lines(Path.of(FILE))) {
            Map<String, DoubleSummaryStatistics> resultMap = lines
                    .parallel()
                    .collect(
                            Collectors.groupingBy(KEY_MAPPER, Collectors.summarizingDouble(VALUE_MAPPER)));
            System.out.println(
                    resultMap.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .map(
                                    entry -> String.format(
                                            "%s=%.1f/%.1f/%.1f",
                                            entry.getKey(),
                                            entry.getValue().getMin(),
                                            entry.getValue().getAverage(),
                                            entry.getValue().getMax()))
                            .collect(Collectors.joining(", ", "{", "}")));
        }
    }

    private static final long MAX_VALUE_DIVIDE_10 = Long.MAX_VALUE / 10;

    private static double toDouble(String num) {
        long value = 0;
        int exp = 0;
        boolean negative = false;
        int decimalPlaces = Integer.MIN_VALUE;
        for (byte ch : num.getBytes()) {
            if (ch >= '0' && ch <= '9') {
                value = value * 10 + (ch - '0');
                decimalPlaces++;
            }
            else if (ch == '-') {
                negative = true;
            }
            else if (ch == '.') {
                decimalPlaces = 0;
            }
            else {
                break;
            }
        }

        return asDouble(value, exp, negative, decimalPlaces);
    }

    private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
        if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
            if (value < Long.MAX_VALUE / (1L << 32)) {
                exp -= 32;
                value <<= 32;
            }
            if (value < Long.MAX_VALUE / (1L << 16)) {
                exp -= 16;
                value <<= 16;
            }
            if (value < Long.MAX_VALUE / (1L << 8)) {
                exp -= 8;
                value <<= 8;
            }
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
            }
        }
        for (; decimalPlaces > 0; decimalPlaces--) {
            exp--;
            long mod = value % 5;
            value /= 5;
            int modDiv = 1;
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
                modDiv <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
                modDiv <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
                modDiv <<= 1;
            }
            if (decimalPlaces > 1)
                value += modDiv * mod / 5;
            else
                value += (modDiv * mod + 4) / 5;
        }
        final double d = Math.scalb((double) value, exp);
        return negative ? -d : d;
    }
}
