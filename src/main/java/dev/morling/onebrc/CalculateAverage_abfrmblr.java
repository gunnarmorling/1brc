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
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.nio.file.Files.lines;
import static java.nio.file.Paths.*;

public class CalculateAverage_abfrmblr {

    private static final String FILE = "./measurements.txt";

    private static record Stats (double min, double max, double mean, long count) {
        @Override
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }
        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static record MeasurementTuple (String station, double temp) {
        public MeasurementTuple (String[] values) {
            this(values[0], Double.parseDouble(values[1]));
        }
    }

    public static void main(String[] args) throws IOException {
        Stream<String> lines = lines(get(FILE));

        ConcurrentMap<String, Stats> aggregatedStats = new ConcurrentSkipListMap<>();

        lines.parallel().forEach(s -> {
            MeasurementTuple tuple = new MeasurementTuple(s.split(";"));
            aggregatedStats.compute(tuple.station(), (s1, stats) -> {
                if (stats == null) {
                    return new Stats(tuple.temp, tuple.temp, tuple.temp, 1L);
                }
                else {
                    long latestCount = stats.count + 1;
                    double min = Math.min(stats.min, tuple.temp);
                    double max = Math.max(stats.max, tuple.temp);
                    double mean = ((stats.mean * stats.count) + tuple.temp) / latestCount;
                    return new Stats(min, max, mean, latestCount);
                }
            });
        });

        System.out.println(aggregatedStats);

    }

}
