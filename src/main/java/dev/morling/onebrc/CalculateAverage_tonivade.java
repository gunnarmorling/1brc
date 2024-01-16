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

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    private static record ResultRow(double min, double mean, double max) {

        @Override
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, Station> map = new HashMap<>();

        Files.lines(Paths.get(FILE))
                .forEach(line -> {
                    var index = line.indexOf(';');
                    var name = line.substring(0, index);
                    var value = Double.parseDouble(line.substring(index + 1));
                    map.computeIfAbsent(name, Station::new).add(value);
                });

        Map<String, ResultRow> measurements = map.values().stream()
                .collect(toMap(Station::getName, Station::finish, throwingMerger(), TreeMap::new));

        System.out.println(measurements);
    }

    private static BinaryOperator<ResultRow> throwingMerger() {
        return (a, b) -> {
            throw new IllegalStateException("Duplicated key exception");
        };
    }

    static final class Station {

        private final String name;

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        Station(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }

        void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        ResultRow finish() {
            return new ResultRow(
                    this.min, (Math.round(this.sum * 10.0) / 10.0) / this.count, this.max);
        }
    }
}
