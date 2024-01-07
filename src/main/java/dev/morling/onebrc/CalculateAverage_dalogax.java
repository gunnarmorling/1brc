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
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_dalogax {

    public static void main(String[] args) throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get("./measurements.txt"))) {
            Map<String, DoubleSummaryStatistics> cityStatsMap = lines
                    .parallel()
                    .map(CalculateAverage_dalogax::splitSemicolon)
                    .collect(Collectors.groupingBy(
                            parts -> parts[0],
                            Collectors.summarizingDouble(value -> Double.parseDouble(value[1]))));

            StringJoiner result = new StringJoiner(", ", "{", "}");
            cityStatsMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> result.add(entry.getKey() + "="
                            + String.format("%.1f", entry.getValue().getMin()) + "/"
                            + String.format("%.1f", entry.getValue().getMax()) + "/"
                            + String.format("%.1f", entry.getValue().getAverage())));

            System.out.println(result.toString());
        }
    }

    private static String[] splitSemicolon(String line) {
        String[] parts = new String[2];
        int semicolonIndex = line.indexOf(';');
        parts[0] = line.substring(0, semicolonIndex).trim();
        parts[1] = line.substring(semicolonIndex + 1).trim();
        return parts;
    }
}