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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_kumarsaurav123 {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(String station,double min, double mean, double max,double sum,double count) {
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

    public static void main(String[] args) {
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

        long start = System.currentTimeMillis();
        long len = Paths.get(FILE).toFile().length();
        Map<Integer, List<byte[]>> leftOutsMap = new ConcurrentSkipListMap<>();
        int chunkSize = 1000_000;
        long proc = Math.max(1, (len / chunkSize));
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 8);
        List<ResultRow> measurements = Collections.synchronizedList(new ArrayList<ResultRow>());
        IntStream.range(0, (int) proc)
                .mapToObj(i -> {
                    return new Runnable() {
                        @Override
                        public void run() {
                            try {
                                RandomAccessFile file = new RandomAccessFile(FILE, "r");
                                byte[] allBytes2 = new byte[chunkSize];
                                file.seek((long) i * (long) chunkSize);
                                int l = file.read(allBytes2);
                                byte[] eol = "\n".getBytes(StandardCharsets.UTF_8);
                                List<String> indexs = new ArrayList<>();
                                int st = 0;
                                int cnt = 0;
                                ArrayList<byte[]> local = new ArrayList<>();

                                for (int i = 0; i < l; i++) {
                                    if (allBytes2[i] == eol[0]) {
                                        if (i != 0) {
                                            byte[] s2 = new byte[i - st];
                                            System.arraycopy(allBytes2, st, s2, 0, s2.length);
                                            if (cnt != 0) {
                                                indexs.add(new String(s2));
                                            }
                                            else {
                                                local.add(s2);
                                            }

                                        }
                                        cnt++;
                                        st = i + 1;
                                    }
                                }
                                if (st < l) {
                                    byte[] s2 = new byte[allBytes2.length - st];
                                    System.arraycopy(allBytes2, st, s2, 0, s2.length);
                                    local.add(s2);
                                }
                                leftOutsMap.put(i, local);
                                allBytes2 = null;
                                Collection<ResultRow> newmeasurements = indexs.stream()
                                        .map(ii -> {
                                            Measurement m = new Measurement(ii.split(";"));
                                            return m;
                                        })
                                        .collect(groupingBy(Measurement::station, collector))
                                        .values();
                                measurements.addAll(newmeasurements);
                            }
                            catch (Exception e) {
                                // throw new RuntimeException(e);
                            }
                        }
                    };
                })
                .forEach(executor::submit);
        executor.shutdown();

        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Collection<Measurement> lMeasure = new ArrayList<>();
        List<byte[]> leftOuts = leftOutsMap.values()
                .stream()
                .flatMap(List::stream)
                .toList();
        int size = 0;
        for (int i = 0; i < leftOuts.size(); i++) {
            size = size + leftOuts.get(i).length;
        }
        byte[] allBytes = new byte[size];
        int pos = 0;
        for (int i = 0; i < leftOuts.size(); i++) {
            System.arraycopy(leftOuts.get(i), 0, allBytes, pos, leftOuts.get(i).length);
            pos = pos + leftOuts.get(i).length;
        }
        List<String> l = Arrays.asList(new String(allBytes).split(";"));
        List<Measurement> measurements1 = new ArrayList<>();
        String city = l.get(0);
        for (int i = 0; i < l.size() - 1; i++) {
            int sIndex = l.get(i + 1).indexOf('.') + 2;

            String tempp = l.get(i + 1).substring(0, sIndex);

            measurements1.add(new Measurement(city, Double.parseDouble(tempp)));
            city = l.get(i + 1).substring(sIndex);
        }
        measurements.addAll(measurements1.stream()
                .collect(groupingBy(Measurement::station, collector))
                .values());
        Map<String, ResultRow> measurements2 = new TreeMap<>(measurements
                .stream()
                .parallel()
                .collect(groupingBy(ResultRow::station, collector2)));

        // Read from bytes 1000 to 2000
        // Something like this

        //
        // Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
        // .map(l -> new Measurement(l.split(";")))
        // .collect(groupingBy(m -> m.station(), collector)));

        System.out.println(measurements2);
    }
}
