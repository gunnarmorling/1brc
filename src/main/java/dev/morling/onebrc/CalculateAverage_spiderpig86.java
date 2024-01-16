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
import java.util.Map;
import java.util.TreeMap;

/**
 * Changelog:
 *      - 1/15/24 - initial minor changes with syntax. 100k lines - 209ms
 *      - 1/15/24 - new algorithm without streams, new splitting method, use of BufferedReader. https://stackoverflow.com/questions/11001330/java-split-string-performances
 *          100k lines - 154ms,
 *          1m lines - 578ms
 *      - 1/16/24 - changed to single linear pass, use indexOf instead of string tokenizer, other changes.
 *          1m lines - 544ms
 */
public class CalculateAverage_spiderpig86 {

    private static final String FILE = "./measurements2.txt";

    private record Measurement(String station, double value) { }

    private static class ResultRow {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double total = 0.0;
        private long count = 0;

        public void processValue(double val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            total += val;
            ++count;
        }

        public double getMean() {
            return total / count;
        }

        public String toString() {
            return round(min) + "/" + round(getMean()) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    public static void main(String[] args) throws IOException {
        // TODO Remove
        long start = System.currentTimeMillis();

        // Read file
        Map<String, ResultRow> stations = new TreeMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE))) {
            String line;

            // Read to the end
            while ((line = reader.readLine()) != null) {
                Measurement measurement = parseRow(line);
                if (!stations.containsKey(measurement.station)) {
                    stations.put(measurement.station, new ResultRow());
                }
                stations.get(measurement.station).processValue(measurement.value());
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        System.out.println(stations);
        // TODO Remove
         System.out.println("Elapsed time ms: " + (System.currentTimeMillis() - start));
    }

    private static Measurement parseRow(String row) {
        // Substring is faster than split by a long shot
        // https://stackoverflow.com/questions/13997361/string-substring-vs-string-split#:~:text=When%20you%20run%20this%20multiple,would%20be%20even%20more%20drastic.
        int delimiterIndex = row.indexOf(';');
        return new Measurement(row.substring(0, delimiterIndex), Double.parseDouble(row.substring(delimiterIndex + 1)));
    }
}