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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * Changelog:
 *      - 1/15/24 - initial minor changes with syntax. 100k lines - 209ms
 *      - 1/15/24 - new algorithm without streams, new splitting method, use of BufferedReader. 100k lines - 154ms,
 *      1m lines - 578ms
 */
public class CalculateAverage_spiderpig86 {

    private static final String FILE = "./measurements2.txt";

    private record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    public static void main(String[] args) throws IOException {
        // TODO Remove
        long start = System.currentTimeMillis();

        // Read file
        Map<String, List<Measurement>> stations = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE))) {
            String line;

            // Read to the end
            while ((line = reader.readLine()) != null) {
                Measurement measurement = parseRow(line);
                if (!stations.containsKey(measurement.station)) {
                    stations.put(measurement.station, new ArrayList<>());
                }
                stations.get(measurement.station).add(measurement);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // For each station, find the min, max, and mean. Station names must be in sorted order
        Map<String, ResultRow> result = new TreeMap<>();
        for (Map.Entry<String, List<Measurement>> entry : stations.entrySet()) {
            result.put(entry.getKey(), getStats(entry.getValue()));
        }

        System.out.println(result);
        // TODO Remove
         System.out.println("Elapsed time ms: " + (System.currentTimeMillis() - start));
    }

    private static Measurement parseRow(String row) {
        // Testing faster String splits
        // https://stackoverflow.com/questions/11001330/java-split-string-performances
        StringTokenizer stringTokenizer = new StringTokenizer(row, ";");

        return new Measurement(new String[]{ stringTokenizer.nextToken(), stringTokenizer.nextToken() });
    }

    private static ResultRow getStats(List<Measurement> measurements) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0.0;
        long count = 0;

        for (Measurement m : measurements) {
            min = Math.min(min, m.value);
            max = Math.max(max, m.value);
            ++count;
            sum += m.value;
        }

        return new ResultRow(min, (Math.round(sum * 10.0) / 10.0) / count, max);
    }
}