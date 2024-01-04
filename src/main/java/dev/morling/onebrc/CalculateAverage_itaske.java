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
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class CalculateAverage_itaske {

    private record Measurement(long count, double sum, double min, double max) {

        Measurement(double value) {
            this(1, value, value, value);
        }


        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(round(min));
            builder.append("/");
            builder.append(round(sum/count));
            builder.append("/");
            builder.append(round(max));

            return builder.toString();
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {

        Map<String, Measurement> resultMap = Files.lines(Arguments.measurmentsPath(args)).parallel()
                .map(line -> {
                    int separatorIndex = line.indexOf(";");
                    String key = line.substring(0, separatorIndex);
                    double value = Double.parseDouble(line.substring(separatorIndex + 1));
                    return new AbstractMap.SimpleEntry<>(key, value);
                })
                .collect(Collectors.toConcurrentMap(
                        entry -> entry.getKey(),
                        entry -> new Measurement(entry.getValue()),
                        ((measurement1, measurement2) -> new Measurement(
                                measurement1.count + measurement2.count,
                                measurement1.sum + measurement2.sum,
                                Math.min(measurement1.min, measurement2.min),
                                Math.max(measurement1.max, measurement2.max)))));

        System.out.print(
                resultMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ", "{", "}")));

    }
}
