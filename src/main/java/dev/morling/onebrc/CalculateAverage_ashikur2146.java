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
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_ashikur2146 {
    private static final String FILE_PATH = "./measurements.txt";

    public static void main(String[] args) {
        try {
            Map<String, TemperatureStats> stationStats = processFile(FILE_PATH);
            System.out.println(stationStats);

        }
        catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, TemperatureStats> processFile(String filePath) throws IOException {
        try {
            Path path = Paths.get(filePath);
            return Files.lines(path).map(line -> line.split(";")).parallel().filter(parts -> parts.length == 2)
                    .collect(Collectors.groupingByConcurrent(parts -> parts[0],
                            Collectors.mapping(parts -> new TemperatureStats(Double.parseDouble(parts[1])),
                                    Collectors.reducing(TemperatureStats::merge))))
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().orElseThrow(),
                            (e1, e2) -> e1,
                            TreeMap::new));
        }
        catch (IOException e) {
            throw new IOException("Error reading file: " + e.getMessage(), e);
        }
    }

}

record TemperatureStats(double min, double mean, double max, int count) {

    public TemperatureStats(double temperature) {
		this(temperature, temperature, temperature, 1);
	}

    public TemperatureStats merge(TemperatureStats other) {
        double newMin = Math.min(min, other.min);
        double newMax = Math.max(max, other.max);
        double newMean = ((mean * count) + (other.mean * other.count)) / (count + other.count);
        return new TemperatureStats(newMin, newMean, newMax, count + other.count);
    }

    @Override
    public String toString() {
        return String.format("%.1f/%.1f/%.1f", min, mean, max);
    }
}
