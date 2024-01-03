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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_shervilg {
    private static final String FILE = "./measurements.txt";

    private record Measurement(double min, double max, double sum, long count) {
        Measurement(double initialMeasurement) {
            this(initialMeasurement, initialMeasurement, initialMeasurement, 1);
        }

        public static Measurement addMeasurement(
            Measurement measurementOne,
            Measurement measurementTwo) {
            return new Measurement(
                Math.min(measurementOne.min, measurementTwo.min),
                Math.max(measurementOne.max, measurementTwo.max),
                measurementOne.sum + measurementTwo.sum,
                measurementOne.count + measurementTwo.count
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
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(FILE)))) {
            Map<String, Measurement> map = new HashMap<>();

            String line = reader.readLine();
            while (line != null) {
                String[] data = line.split(";");
                map.merge(data[0], new Measurement(Double.parseDouble(data[1])), Measurement::addMeasurement);

                line = reader.readLine();
            }

            System.out.println(map);
        }
        catch (Exception ignored) {
        }
    }
}
