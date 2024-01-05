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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public class CalculateAverage_eriklumme {

    private static final String FILE = "./measurements.txt";

    private static class StationMeasurement {
        private final String stationName;

        private StationMeasurement(String stationName) {
            this.stationName = stationName;
        }

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private long count = 0;
    }

    public static void main(String[] args) throws IOException {
        TreeMap<String, StationMeasurement> treeSet = new TreeMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(FILE))) {
            reader.lines().forEach(line -> {
                int splitIndex = line.indexOf(";");
                String stationName = line.substring(0, splitIndex);
                double value = Double.parseDouble(line.substring(splitIndex + 1));
                StationMeasurement stationMeasurement = treeSet.computeIfAbsent(stationName, StationMeasurement::new);
                stationMeasurement.count++;
                stationMeasurement.min = Math.min(value, stationMeasurement.min);
                stationMeasurement.max = Math.max(value, stationMeasurement.max);
                stationMeasurement.sum += value;
            });
        }

        StringBuilder result = new StringBuilder("{");
        boolean first = true;
        for (StationMeasurement stationMeasurement : treeSet.values()) {
            if (first) {
                first = false;
                result.append(", ");
            }
            result.append(stationMeasurement.stationName).append("=");
            result.append(stationMeasurement.min);
            result.append(String.format("/%.1f/", (stationMeasurement.sum / stationMeasurement.count)));
            result.append(stationMeasurement.max);
        }
        result.append("}");

        System.out.println(result);
    }
}
