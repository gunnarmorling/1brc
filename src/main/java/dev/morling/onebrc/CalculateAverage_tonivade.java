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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, Station> map = new HashMap<>();

        Files.lines(Paths.get(FILE))
                .forEach(line -> {
                    var index = line.indexOf(';');
                    var name = line.substring(0, index);
                    var value = parseDouble(line.substring(index + 1));
                    map.computeIfAbsent(name, Station::new).add(value);
                });

        var measurements = map.values().stream().sorted(comparing(Station::getName))
                .map(Station::toString).collect(joining(", ", "{", "}"));

        System.out.println(measurements);
    }

    // non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
    private static double parseDouble(String value) {
        var period = value.indexOf('.');
        if (value.charAt(0) == '-') {
            var left = parseLeft(value.substring(1, period));
            var right = parseRight(value.substring(period + 1));
            return -(left + right);
        }
        var left = parseLeft(value.substring(0, period));
        var right = parseRight(value.substring(period + 1));
        return left + right;
    }

    private static double parseLeft(String left) {
        if (left.length() == 1) {
            return (double) left.charAt(0) - 48;
        }
        // two chars
        var a = (double) left.charAt(0) - 48;
        var b = (double) left.charAt(1) - 48;
        return (a * 10) + b;
    }

    private static double parseRight(String right) {
        var a = (double) (right.charAt(0) - 48);
        return a / 10.;
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

        @Override
        public String toString() {
            return name + "=" + round(min) + "/" + round(mean()) + "/" + round(max);
        }

        private double mean() {
            return round(this.sum) / this.count;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
