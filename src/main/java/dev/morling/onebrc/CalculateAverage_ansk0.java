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
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingByConcurrent;

public class CalculateAverage_ansk0 {

    private static final String FILE = "./measurements.txt";

    private record Measurement(String station, Float value) { }

    private static class MA {
        private float min;
        private float max;
        private double sum;
        private long count;

        MA() {
            this.min = Float.POSITIVE_INFINITY;
            this.max = Float.NEGATIVE_INFINITY;
            this.sum = 0;
            this.count = 0;
        }
    }

    public static void main(String[] args) throws IOException {

        Collector<Measurement, MA, String> collector = Collector.of(
                MA::new,
                (a, m) -> {
                    a.min = a.min < m.value ? a.min : m.value;
                    a.max = a.max > m.value ? a.max : m.value;
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    agg1.min = agg1.min < agg2.min ? agg1.min : agg2.min;
                    agg1.max = agg1.max > agg2.max ? agg1.max : agg2.max;
                    agg1.sum += agg2.sum;
                    agg1.count += agg2.count;
                    return agg1;
                },
                agg -> new StringBuilder()
                        .append(String.format("%.1f", agg.min))
                        .append('/')
                        .append(String.format("%.1f", (agg.sum / agg.count)))
                        .append('/')
                        .append(String.format("%.1f", agg.max))
                        .toString());


        Map<String, String> measurements = new BufferedReader(new FileReader(Paths.get(FILE).toFile()), 131072)
                        .lines()
                        .parallel()
                        .map(l -> {
                            final int semiColonAt = l.indexOf(';');
                            return new Measurement(
                                    l.substring(0, semiColonAt),
                                    Float.parseFloat(l.substring(semiColonAt + 1)));
                        })
                        .collect(groupingByConcurrent(Measurement::station, collector));

        System.out.println(measurements);
    }
}
