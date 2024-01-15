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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;

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
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 8 * 2);
        List<ResultRow> measurements = Collections.synchronizedList(new ArrayList<ResultRow>());
        int memory = (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024.0 * 8));
        int chunkSize = 1_0000_00 * memory;
        Map<Integer, List<byte[]>> leftOutsMap = new ConcurrentSkipListMap<>();
        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        long filelength = file.length();
        int kk = 0;
        long loaded = 0;
        while (filelength > loaded) {
            FileChannel fileChannel = file.getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel
                    .map(FileChannel.MapMode.READ_ONLY, loaded, Math.min(filelength - loaded, Integer.MAX_VALUE));
            loaded = loaded + Math.min(filelength - loaded, Integer.MAX_VALUE);
            long st = 0;

            while (st < mappedByteBuffer.limit()) {
                int size = (int) Math.min(chunkSize, mappedByteBuffer.limit() - st);
                st = st + size;
                byte[] allBytes2 = new byte[size];
                mappedByteBuffer.get(allBytes2);
                executorService.submit(createRunnable(kk++, allBytes2, allBytes2.length, collector, leftOutsMap, measurements));
            }
        }

        executorService.shutdown();

        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
        byte[] city = new byte[200];
        byte[] value = new byte[200];
        int cnt = 0;
        boolean isCity = true;
        int citylen = 0;
        List<Measurement> measurements1 = new ArrayList<>();
        for (int i = 0; i < allBytes.length; i++) {
            if (allBytes[i] == ';') {
                isCity = !isCity;
                if (isCity) {
                    city = new byte[200];
                    cnt = 0;
                    citylen = 0;
                }
                else {
                    value = new byte[200];
                    cnt = 0;
                }
                continue;
            }
            if (isCity) {
                city[cnt++] = allBytes[i];
                citylen = cnt;
            }
            else {
                if (allBytes[i] < 45 || allBytes[i] > 57) {

                    measurements1.add(addNewMeasurement(value, citylen, city));
                    cnt = 0;
                    isCity = true;
                    city = new byte[200];
                    city[cnt++] = allBytes[i];
                    citylen = cnt;
                }
                else {
                    value[cnt++] = allBytes[i];
                }
            }
        }
        // read last city
        measurements1.add(addNewMeasurement(value, citylen, city));
        measurements.addAll(measurements1.stream()
                .collect(groupingBy(Measurement::station, collector))
                .values());
        Map<String, ResultRow> measurements2 = new TreeMap<>(measurements
                .stream()
                .parallel()
                .collect(groupingBy(ResultRow::station, collector2)));

        System.out.println(measurements2);
        // System.out.println(System.currentTimeMillis() - start);
    }

    private static Measurement addNewMeasurement(byte[] value, int citylen, byte[] city) {
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
        double d = 0.0;
        int s = -1;
        for (int k = value.length - 1; k >= 0; k--) {
            if (value[k] == 0) {
                continue;
            }
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
        byte[] fcity = new byte[citylen];
        System.arraycopy(city, 0, fcity, 0, citylen);
        return new Measurement(new String(fcity), d);
    }

    public static Runnable createRunnable(final int kk, final byte[] allBytes2, int l, Collector<Measurement, MeasurementAggregator, ResultRow> collector,
                                          Map<Integer, List<byte[]>> leftOutsMap, List<ResultRow> measurements) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
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
                    byte[] eol = "\n".getBytes(StandardCharsets.UTF_8);
                    byte[] sep = ";".getBytes(StandardCharsets.UTF_8);

                    List<Measurement> mst = new ArrayList<>();
                    int st = 0;
                    int cnt = 0;
                    ArrayList<byte[]> local = new ArrayList<>();

                    for (int i = 0; i < l; i++) {
                        if (allBytes2[i] == eol[0]) {
                            if (i != 0) {
                                byte[] s2 = new byte[i - st];
                                System.arraycopy(allBytes2, st, s2, 0, s2.length);
                                if (cnt != 0) {
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
                                                    d = d + map.get(value[k]).intValue() * Math.pow(10, s);
                                                    s++;
                                                }
                                            }
                                            mst.add(new Measurement(new String(city), d));

                                        }
                                    }

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
                    leftOutsMap.put(kk, local);
                    measurements.addAll(mst.stream()
                            .collect(groupingBy(Measurement::station, collector))
                            .values());
                    // System.out.println("Task " + kk + "Completed in " + (System.currentTimeMillis() - start));
                }
                catch (Exception e) {
                    // throw new RuntimeException(e);
                    System.out.println("");
                }
            }
        };
    }
}
