/*
 *  Copyright 2024 The original authors
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
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;

public class CalculateAverage_JuanMorenoDeveloper {
    private static final Path MEASUREMENT_FILES = Path.of("./measurements.txt");

    public static void main(String[] args) throws IOException {
        try (Stream<String> lines = Files.lines(MEASUREMENT_FILES)) {
            var results = lines
                    .parallel()
                    .map(line -> line.split(";"))
                    .map(values -> Map.entry(values[0]/*City*/, Double.parseDouble(values[1]) /*Measurement*/))
                    /*Grouping and computation*/
                    .collect(groupingBy(Map.Entry::getKey, summarizingDouble(Map.Entry::getValue)))
                    .entrySet()
                    .parallelStream()
                    /*Format & sorting*/
                    .collect(Collectors
                            .toMap(Map.Entry::getKey,
                                    entry -> "%.1f/%.1f/%.1f".formatted(entry.getValue().getMin(), entry.getValue().getAverage(), entry.getValue().getMax()),
                                    (o1, o2) -> o1 /*Unused*/,
                                    TreeMap::new));

            System.out.println(results);
        }
    }
}
