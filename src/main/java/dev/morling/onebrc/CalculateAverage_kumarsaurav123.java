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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_kumarsaurav123 {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record Pair(long start, int size) {
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
        System.out.println(run(FILE));
        // System.out.println(System.currentTimeMillis() - start);
    }

    public static String run(String filePath) throws IOException {
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
                    return new ResultRow(agg.station, agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max, agg.sum, agg.count);
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
        int chunkSize = 1_0000_00;
        Map<Integer, List<byte[]>> leftOutsMap = new ConcurrentSkipListMap<>();
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        long filelength = file.length();
        AtomicInteger kk = new AtomicInteger();
        MemorySegment memorySegment = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, filelength, Arena.global());
        int nChunks = 1000;

        int pChunkSize = Math.min(Integer.MAX_VALUE, (int) (memorySegment.byteSize() / (1000 * 20)));
        if (pChunkSize < 100) {
            pChunkSize = (int) memorySegment.byteSize();
            nChunks = 1;
        }
        ArrayList<Pair> chunks = createStartAndEnd(pChunkSize, nChunks, memorySegment);
        chunks.stream()
                .map(p -> {

                    return createRunnable(memorySegment, p, collector, measurements, kk.getAndIncrement());
                })
                .forEach(executorService::submit);
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
        return measurements2.toString();
    }

    private static ArrayList<Pair> createStartAndEnd(int chunksize, int nChunks, MemorySegment memorySegment) {
        ArrayList<Pair> startSizePairs = new ArrayList<>();
        byte eol = "\n".getBytes(StandardCharsets.UTF_8)[0];
        long start = 0;
        long end = -1;
        if (nChunks == 1) {
            startSizePairs.add(new Pair(0, chunksize));
            return startSizePairs;
        }
        else {
            while (start < memorySegment.byteSize()) {
                start = end + 1;
                end = Math.min(memorySegment.byteSize() - 1, start + chunksize - 1);
                while (memorySegment.get(ValueLayout.JAVA_BYTE, end) != eol) {
                    end--;

                }
                startSizePairs.add(new Pair(start, (int) (end - start + 1)));
            }
        }
        return startSizePairs;
    }

    public static Runnable createRunnable(MemorySegment memorySegment, Pair p, Collector<Measurement, MeasurementAggregator, ResultRow> collector,
                                          List<ResultRow> measurements, int kk) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();

                    byte[] allBytes2 = new byte[p.size];
                    MemorySegment lMemory = memorySegment.asSlice(p.start, p.size);
                    lMemory.asByteBuffer().get(allBytes2);
                    HashMap<Byte, Integer> map = new HashMap<>();
                    // Runtime runtime = Runtime.getRuntime();
                    // long memoryMax = runtime.maxMemory();
                    // long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
                    // double memoryUsedPercent = (memoryUsed * 100.0) / memoryMax;
                    // System.out.println("memoryUsedPercent: " + memoryUsedPercent);
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

                    for (int i = 0; i < allBytes2.length; i++) {
                        if (allBytes2[i] == eol[0]) {
                            byte[] s2 = new byte[i - st];
                            System.arraycopy(allBytes2, st, s2, 0, s2.length);
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
                            st = i + 1;
                        }
                    }
                    // System.out.println("Task " + kk + "Completed in " + (System.currentTimeMillis() - start));
                    measurements.addAll(mst.stream()
                            .collect(groupingBy(Measurement::station, collector))
                            .values());

                }
                catch (Exception e) {
                    // throw new RuntimeException(e);
                    System.out.println("");
                }
            }
        };
    }
}
