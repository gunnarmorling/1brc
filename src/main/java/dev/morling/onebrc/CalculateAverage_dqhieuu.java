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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateAverage_dqhieuu {
    private static final String FILE = "measurements.txt";

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static class MeasurementAggregator {
        private Lock lock = new ReentrantLock();
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private int count = 0;

        @Override
        public String toString() {
            return round(min) + "/" + round(round(sum) / count) + "/" + round(max);
        }
    }

    public static void main(String[] args) throws IOException {
        var lineStream = Files.lines(Paths.get(FILE)).parallel();

        Map<String, MeasurementAggregator> measurements = new ConcurrentHashMap<>(10_000);

        lineStream.forEach(
                l -> {
                    var sepIdx = 0;
                    while (l.charAt(sepIdx) != ';') {
                        sepIdx++;
                    }

                    var station = l.substring(0, sepIdx);

                    int valueInt = 0;
                    int sign = l.charAt(sepIdx + 1) == '-' ? -1 : 1;

                    var lineLength = l.length();
                    for (var i = sepIdx + 1; i < lineLength; i++) {
                        var c = l.charAt(i);
                        if (c == '-' || c == '.') {
                            continue;
                        }
                        valueInt = valueInt * 10 + (c - '0');
                    }

                    var value = ((double) valueInt / 10.0) * sign;

                    var agg = measurements.computeIfAbsent(station, k -> new MeasurementAggregator());

                    agg.lock.lock();

                    if (value < agg.min) {
                        agg.min = value;
                    }
                    if (value > agg.max) {
                        agg.max = value;
                    }
                    agg.sum += value;
                    agg.count++;

                    agg.lock.unlock();
                });

        Map<String, MeasurementAggregator> sortedEntries = new TreeMap<>(measurements);

        var res = new StringBuilder();
        res.append("{");

        var first = true;
        for (var entry : sortedEntries.entrySet()) {
            if (first) {
                first = false;
            }
            else {
                res.append(", ");
            }

            var k = entry.getKey();
            var v = entry.getValue();

            res.append(k);
            res.append('=');
            res.append(v);
        }

        res.append("}");

        System.out.println(res);
    }
}
