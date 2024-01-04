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

import org.radughiorma.Arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_khmarbaise {

    private record MeasurementRecord(String city, Double measuredValue) {
    }

    private static final Function<String, MeasurementRecord> toMeasurementRecord = line -> {
        var posOf = line.indexOf(";");
        var city = line.substring(0, posOf);
        var measuredValue = line.substring(posOf + 1);
        return new MeasurementRecord(city, Double.parseDouble(measuredValue));
    };

    private static final Function<Map.Entry<String, DoubleSummaryStatistics>, String> MIN_AVG_MAX = s -> String.format("%s=%.1f/%.1f/%.1f", s.getKey(),
            s.getValue().getMin(), s.getValue().getAverage(), s.getValue().getMax());

    public static void main(String[] args) throws IOException {
        try (Stream<String> lines = Files.lines(Arguments.measurmentsPath(args))) {
            var collect = lines
                    .parallel()
                    .map(toMeasurementRecord)
                    .collect(groupingBy(MeasurementRecord::city, Collectors.summarizingDouble(MeasurementRecord::measuredValue)))
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(MIN_AVG_MAX)
                    .collect(Collectors.joining(", "));

            System.out.println("{" + collect + "}");
        }
    }
}
