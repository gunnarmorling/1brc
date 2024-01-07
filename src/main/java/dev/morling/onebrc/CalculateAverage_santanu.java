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
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Double.parseDouble;
import static java.lang.String.format;
import static java.util.stream.Collectors.summarizingDouble;

public class CalculateAverage_santanu {
    private static final String DATA_FILE = "./measurements.txt";
    private static final String PRINT_FORMAT = "%s=%.1f/%.1f/%.1f";

    public static void main(String[] args) throws IOException {

        Map<String, DoubleSummaryStatistics> summaryStatisticsMap = processMeasurements();

        printResults(summaryStatisticsMap);
    }

    private static final Function<String, StationMeasurementPair> stationMeasurements = row -> {
        int splitPosition = row.indexOf(";");
        return new StationMeasurementPair(row.substring(0, splitPosition), parseDouble(row.substring(splitPosition + 1)));
    };

    private static Map<String, DoubleSummaryStatistics> processMeasurements() throws IOException {
        return Files.lines(Path.of(DATA_FILE)).map(stationMeasurements).parallel()
                .collect(Collectors.groupingBy(
                        StationMeasurementPair::station, summarizingDouble(StationMeasurementPair::measurement)));
    }

    private static void printResults(Map<String, DoubleSummaryStatistics> collect) {

        String result = collect.entrySet().parallelStream().sorted(Map.Entry.comparingByKey()).map(s -> format(PRINT_FORMAT, s.getKey(),
                s.getValue().getMin(), s.getValue().getAverage(), s.getValue().getMax())).collect(Collectors.joining(", "));

        System.out.println("{" + result + "}");
    }

    private record StationMeasurementPair(String station, Double measurement) {
    }
}
