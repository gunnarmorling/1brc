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
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;

import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collectors.groupingByConcurrent;

public class CalculateAverage_fatroom {

    private static final String FILE = "./measurements.txt";

    private static final Map<String, Integer> numberCache = new ConcurrentHashMap<>(2000);

    private static class MeasurementAggregator {
        private int min;
        private int max;
        private long sum;
        private long count;

        public MeasurementAggregator() {
            this(Integer.MAX_VALUE, Integer.MIN_VALUE, 0, 0);
        }

        public MeasurementAggregator(int min, int max, long sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public void consume(String v) {
            int measurement = 0;
            for (int i = 0; i < v.length(); i++) {
                if (v.charAt(i) == ';') {
                    measurement = numberCache.computeIfAbsent(v.substring(i + 1), key -> {
                        int sign = 1;
                        int value = 0;
                        for (int j = 0; j < key.length(); j++) {
                            int c = key.charAt(j);
                            if (c == '-') {
                                sign = -1;
                                continue;
                            }
                            if (c == '.') {
                                continue;
                            }
                            value = value * 10 + (c - '0');
                        }
                        value *= sign;
                        return value;
                    });
                    break;
                }
            }

            this.min = this.min < measurement ? this.min : measurement;
            this.max = this.max > measurement ? this.max : measurement;
            this.sum += measurement;
            this.count++;
        }

        public MeasurementAggregator combineWith(MeasurementAggregator that) {
            return new MeasurementAggregator(
                    this.min < that.min ? this.min : that.min,
                    this.max > that.max ? this.max : that.max,
                    this.sum + that.sum,
                    this.count + that.count);
        }
    }

    public static void main(String[] args) throws IOException {
        var reader = new BufferedReader(new InputStreamReader(FileSystems.getDefault().provider().newInputStream(Paths.get(FILE)), StandardCharsets.UTF_8),
                1024 * 32);

        Collector<String, MeasurementAggregator, String> collector = Collector.of(
                MeasurementAggregator::new,
                MeasurementAggregator::consume,
                MeasurementAggregator::combineWith,
                agg -> String.format(Locale.ROOT, "%.1f/%.1f/%.1f", agg.min / 10.0, agg.sum / agg.count / 10.0, agg.max / 10.0),
                CONCURRENT);

        var measurements = new TreeMap<>(reader.lines()
                .parallel()
                .unordered()
                .collect(groupingByConcurrent(v -> v.substring(0, v.indexOf(";")), collector)));

        System.out.println(measurements);
    }
}
