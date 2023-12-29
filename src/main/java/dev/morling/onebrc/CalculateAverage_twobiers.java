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
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_twobiers {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, Double> measurements = Files.lines(Paths.get(FILE))
                .parallel()
                .map(l -> fastSplit(l, ';'))
                .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));

        measurements = new TreeMap<>(measurements.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> Math.round(e.getValue() * 10.0) / 10.0)));

        System.out.println(measurements);
    }

    private static String[] fastSplit(String str, char delim) {
        var splitArray = new String[2];
        var chars = str.toCharArray();

        int i = 0;
        for (char c : chars) {
            if (c == delim) {
                splitArray[0] = new String(Arrays.copyOfRange(chars, 0, i));
                break;
            }
            i++;
        }

        splitArray[1] = new String(Arrays.copyOfRange(chars, i + 1, chars.length));
        return splitArray;
    }
}
