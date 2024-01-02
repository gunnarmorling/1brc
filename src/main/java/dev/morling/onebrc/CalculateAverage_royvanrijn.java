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
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CalculateAverage_royvanrijn {

    private static final String FILE = "./measurements.txt";

    private record Measurement(double min, double max, double sum, long count) {

        Measurement(double initialMeasurement) {
            this(initialMeasurement, initialMeasurement, initialMeasurement, 1);
        }

        public static Measurement combineWith(Measurement m1, Measurement m2) {
            return new Measurement(
                    m1.min < m2.min ? m1.min : m2.min,
                    m1.max > m2.max ? m1.max : m2.max,
                    m1.sum + m2.sum,
                    m1.count + m2.count
            );
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {

        // long before = System.currentTimeMillis();

        Map<String, Measurement> resultMap = Files.lines(Path.of(FILE)).parallel()
                .map(record -> {
                    // Map to <String,double>
                    int pivot = record.indexOf(";");
                    String key = record.substring(0, pivot);
                    double measured = Double.parseDouble(record.substring(pivot + 1));
                    return new AbstractMap.SimpleEntry<>(key, measured);
                })
                .collect(Collectors.toConcurrentMap(
                        // Combine/reduce:
                        AbstractMap.SimpleEntry::getKey,
                        entry -> new Measurement(entry.getValue()),
                        Measurement::combineWith));

        System.out.print("{");
        System.out.print(
                resultMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");

        // System.out.println("Took: " + (System.currentTimeMillis() - before));

    }
}
