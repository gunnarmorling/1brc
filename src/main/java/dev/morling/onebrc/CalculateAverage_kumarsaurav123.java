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

import static java.util.stream.Collectors.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_kumarsaurav123 {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(String station, double min, double mean, double max, double sum, double count) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    ;

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        private String station;
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        Collector<ResultRow, MeasurementAggregator, ResultRow> collector2 = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.min);
                    a.max = Math.max(a.max, m.max);
                    a.sum += m.sum;
                    a.count += m.count;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.station, agg.min, agg.sum / agg.count, agg.max, agg.sum, agg.count);
                });
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.station = m.station;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.station, agg.min, agg.sum / agg.count, agg.max, agg.sum, agg.count);
                });
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        List<ResultRow> measurements = Collections.synchronizedList(new ArrayList<ResultRow>());

        BufferedReader reader = new BufferedReader(new FileReader(FILE));
        for (int i = 0; i < 10000; i++) {
            List<String> lines = new ArrayList<>(1000_00);
            for (int j = 0; j < 1000_00; j++) {

                lines.add(reader.readLine());
            }
            executorService.submit(createRunnable(lines, measurements, collector, i));

        }
        executorService.shutdown();

        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Map<String, ResultRow> measurements2 = new TreeMap<>(measurements
                .stream()
                .parallel()
                .collect(groupingBy(ResultRow::station, collector2)));

        System.out.println(measurements2);
        System.out.println(System.currentTimeMillis() - start);
    }

    public static Runnable createRunnable(List<String> lines, Collection<ResultRow> measurements, Collector<Measurement, MeasurementAggregator, ResultRow> collector,
                                          int i) {
        return () -> {
            HashMap<Byte, Integer> map = new HashMap<>();
            map.put((byte) 48, 0);
            map.put((byte) 49, 1);
            map.put((byte) 50, 2);
            map.put((byte) 51, 3);
            map.put((byte) 52, 4);
            map.put((byte) 53, 5);
            map.put((byte) 54, 6);
            map.put((byte) 55, 7);
            map.put((byte) 56, 8);
            map.put((byte) 57, 9);
            byte[] sep = ";".getBytes(StandardCharsets.UTF_8);
            List<Measurement> mst = new ArrayList<>();

            lines.stream().filter(Objects::nonNull).forEach(l -> {
                byte[] s2 = l.getBytes(StandardCharsets.UTF_8);
                for (int j = 0; j < s2.length; j++) {
                    if (s2[j] == sep[0]) {
                        byte[] city = new byte[j];
                        byte[] value = new byte[s2.length - j - 1];
                        System.arraycopy(s2, 0, city, 0, city.length);
                        System.arraycopy(s2, city.length + 1, value, 0, value.length);
                        double d = 0.0;
                        int s = -1;
                        for (int k = value.length - 1; k >= 0; k--) {
                            if (value[k] == 45) {
                                d = d * -1;
                            }
                            else if (value[k] == 46) {
                            }
                            else {
                                d = d + map.get(value[k]) * Math.pow(10, s);
                                s++;
                            }
                        }
                        mst.add(new Measurement(new String(city), d));
                        break;
                    }
                }
            });
            measurements.addAll(mst.stream()
                    .collect(Collectors.groupingBy(Measurement::station, collector)).values());
            lines.clear();
            mst.clear();
        };
    }
}
