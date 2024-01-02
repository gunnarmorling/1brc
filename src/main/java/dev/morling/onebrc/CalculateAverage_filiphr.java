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

import static java.util.stream.Collectors.groupingByConcurrent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class CalculateAverage_filiphr {

    private static final String FILE = "./measurements.txt";

    private static final class Measurement {

        private final AtomicReference<Double> min = new AtomicReference<>(Double.POSITIVE_INFINITY);
        private final AtomicReference<Double> max = new AtomicReference<>(Double.NEGATIVE_INFINITY);
        private final AtomicReference<Double> sum = new AtomicReference<>(0d);
        private final AtomicLong count = new AtomicLong(0);

        private Measurement combine(double value) {
            this.min.accumulateAndGet(value, Math::min);
            this.max.accumulateAndGet(value, Math::max);
            this.sum.accumulateAndGet(value, Double::sum);
            this.count.incrementAndGet();
            return this;
        }

        private Measurement combine(Measurement measurement) {
            this.min.accumulateAndGet(measurement.min.get(), Math::min);
            this.max.accumulateAndGet(measurement.max.get(), Math::max);
            this.sum.accumulateAndGet(measurement.sum.get(), Double::sum);
            this.count.addAndGet(measurement.count.get());
            return this;
        }

        @Override
        public String toString() {
            return round(min.get()) + "/" + round(sum.get() / count.get()) + "/" + round(max.get());
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        // long start = System.nanoTime();

        Collector<Map.Entry<String, Double>, Measurement, Measurement> collector = Collector.of(
                Measurement::new,
                (measurement, entry) -> {
                    Double value = entry.getValue();
                    measurement.combine(value);
                },
                Measurement::combine,
                Collector.Characteristics.CONCURRENT, Collector.Characteristics.UNORDERED);

        Map<String, Measurement> measurements;
        try (Stream<String> stream = Files.lines(Paths.get(FILE))) {
            measurements = new TreeMap<>(stream
                    .parallel()
                    .map(line -> {
                        int index = line.indexOf(';');
                        String station = line.substring(0, index);
                        double temperature = Double.parseDouble(line.substring(index + 1));
                        return Map.entry(station, temperature);
                    })
                    .collect(groupingByConcurrent(Map.Entry::getKey, collector)));
        }

        System.out.println(measurements);
        // System.out.println("Done in " + (System.nanoTime() - start) / 1000000 + " ms");
    }
}
