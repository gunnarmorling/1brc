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

    private static final String FILE = "./measurements.txt";

    record Measurement(String station, float temperature) {
    }

    static class StationStats {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        float sum = 0;
        int count = 0;

        synchronized void update(float temperature) {
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
        long start = System.currentTimeMillis();
        Map<String, StationStats> stats = new ConcurrentHashMap<>(10_000);
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(FILE))) {
            reader.lines().parallel().forEach(line -> {
                int separatorIndex = line.indexOf(';');
                String station = line.substring(0, separatorIndex);
                String temperature = line.substring(separatorIndex + 1);
                Measurement m = new Measurement(station, Float.parseFloat(temperature));
                stats.computeIfAbsent(m.station, k -> new StationStats()).update(m.temperature);
            });
        }

        TreeMap<String, StationStats> sortedStats = new TreeMap<>(stats);
        for (Map.Entry<String, StationStats> entry : sortedStats.entrySet()) {
            StationStats s = entry.getValue();
            System.out.printf("%s=%.1f/%.1f/%.1f\n", entry.getKey(), s.min, s.getAverage(), s.max);
        }
        System.out.printf("Measure made in %s ms%n", System.currentTimeMillis() - start);
    }
}
