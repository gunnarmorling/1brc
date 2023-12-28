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

import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, Double> measurements = Files.lines(Paths.get(FILE))
                .limit(10_000_000)
                .map(l -> l.split(";"))
                .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));

        measurements = new TreeMap<>(measurements.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));

        System.out.println(measurements);
    }
}
