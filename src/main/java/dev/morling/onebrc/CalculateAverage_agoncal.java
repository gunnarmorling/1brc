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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_agoncal {

    private static final String FILE = "./measurements-20.txt";

    record Measurement(String station, double temperature) {}

    static class StationStats {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        int count = 0;

        synchronized void update(double temperature) {
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
            sum += temperature;
            count++;
        }

        double getAverage() {
            return sum / count;
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, StationStats> stats = new ConcurrentHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(FILE))) {
            reader.lines().parallel().forEach(line -> {
                String[] parts = line.split(";");
                Measurement m = new Measurement(parts[0], Double.parseDouble(parts[1]));
                stats.computeIfAbsent(m.station, k -> new StationStats()).update(m.temperature);
            });
        }

        TreeMap<String, StationStats> sortedStats = new TreeMap<>(stats);
        for (Map.Entry<String, StationStats> entry : sortedStats.entrySet()) {
            StationStats s = entry.getValue();
            System.out.printf("%s=%.1f/%.1f/%.1f\n", entry.getKey(), s.min, s.getAverage(), s.max);
        }
    }
}
