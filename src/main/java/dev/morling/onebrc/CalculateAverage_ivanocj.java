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
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_ivanocj {

    private static final String FILE = "./measurements.txt";

    private record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private record ResultRow(double min, double mean, double max) {
        @Override
        public String toString() {
            return STR."Min: \{round(min)} | Mean: \{round(mean)} | Max: \{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) {
        try {
            Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                    MeasurementAggregator::new,
                    (a, m) -> {
                        a.min = Math.min(a.min, m.value);
                        a.max = Math.max(a.max, m.value);
                        a.sum += m.value;
                        a.count++;
                    },
                    (agg1, agg2) -> {
                        var res = new MeasurementAggregator();
                        res.min = Math.min(agg1.min, agg2.min);
                        res.max = Math.max(agg1.max, agg2.max);
                        res.sum = agg1.sum + agg2.sum;
                        res.count = agg1.count + agg2.count;
                        return res;
                    },
                    agg -> new ResultRow(agg.min, agg.sum / agg.count, agg.max),
                    Collector.Characteristics.UNORDERED, Collector.Characteristics.CONCURRENT);

            Map<String, ResultRow> measurements;

            try (var lines = Files.lines(Paths.get(FILE)).parallel()) {
                measurements = lines
                        .map(line -> line.split(";"))
                        .map(Measurement::new)
                        .collect(Collectors.groupingByConcurrent(Measurement::station, collector));
            }

            measurements.forEach((station, resultRow) -> System.out.println(STR."\{station}: \{resultRow}"));

        }
        catch (IOException e) {
            System.err.println(STR."Error reading file: \{e.getMessage()}");
        }
        catch (NumberFormatException e) {
            System.err.println(STR."Error parsing file: \{e.getMessage()}");
        }
    }
}
