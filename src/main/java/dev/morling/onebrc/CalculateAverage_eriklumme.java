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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_eriklumme {

    private static final String FILE = "./measurements.txt";
    private static final int NUM_CPUS = 8;
    private static final int LINE_OVERHEAD = 200;
    private static final int NUM_TASKS = NUM_CPUS * 6;

    private static class StationMeasurement {
        private final String stationName;

        private StationMeasurement(String stationName) {
            this.stationName = stationName;
        }

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private int count = 0;
    }

    private enum Mode {
        UNINITIALIZED,
        READ_STATION,
        READ_VALUE
    }

    public static class DataProcessor implements Callable<Map<String, StationMeasurement>> {

        private final int processorIndex;
        private final long size;
        private final FileChannel fileChannel;
        private final CountDownLatch countDownLatch;

        public DataProcessor(int processorIndex, long size, FileChannel fileChannel,
                             CountDownLatch countDownLatch) {
            this.processorIndex = processorIndex;
            this.size = size;
            this.fileChannel = fileChannel;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public Map<String, StationMeasurement> call() throws Exception {
            Map<String, StationMeasurement> map = new HashMap<>();

            byte[] stationBuffer = new byte[200];
            int stationIndex = 0;

            byte[] valueBuffer = new byte[10];
            int valueIndex = 0;

            Mode mode = processorIndex == 0 ? Mode.READ_STATION : Mode.UNINITIALIZED;
            byte c = 0;

            long offset = size * processorIndex;
            long sizeWithOverhead = Math.min(size + LINE_OVERHEAD, fileSize - offset);

            try {
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, sizeWithOverhead);

                while (buffer.hasRemaining()) {
                    c = buffer.get();
                    if (c == '\n') {
                        // We have a station to store
                        if (mode == Mode.READ_VALUE) {
                            String stationName = new String(Arrays.copyOfRange(stationBuffer, 0, stationIndex), StandardCharsets.UTF_8);
                            double value = Double.parseDouble(new String(Arrays.copyOfRange(valueBuffer, 0, valueIndex)));

                            StationMeasurement stationMeasurement = map.computeIfAbsent(stationName, StationMeasurement::new);

                            stationMeasurement.count++;
                            stationMeasurement.min = Math.min(value, stationMeasurement.min);
                            stationMeasurement.max = Math.max(value, stationMeasurement.max);
                            stationMeasurement.sum += value;

                            stationIndex = 0;
                            valueIndex = 0;
                        }
                        mode = Mode.READ_STATION;

                        // We've run past our size, can happen
                        if (buffer.position() > size) {
                            break;
                        }
                    }
                    else if (mode == Mode.UNINITIALIZED) {
                        // Do-nothing, read more
                    }
                    else if (c == ';') {
                        mode = Mode.READ_VALUE;
                    }
                    else if (mode == Mode.READ_STATION) {
                        stationBuffer[stationIndex++] = c;
                    }
                    else {
                        valueBuffer[valueIndex++] = c;
                    }
                }
                if (mode == Mode.READ_VALUE && valueIndex > 0) {
                    // One value left to store
                    String stationName = new String(Arrays.copyOfRange(stationBuffer, 0, stationIndex), StandardCharsets.UTF_8);

                    // TODO: More efficient way?
                    double value = Double.parseDouble(new String(Arrays.copyOfRange(valueBuffer, 0, valueIndex)));

                    StationMeasurement stationMeasurement = map.computeIfAbsent(stationName, StationMeasurement::new);

                    stationMeasurement.count++;
                    stationMeasurement.min = Math.min(value, stationMeasurement.min);
                    stationMeasurement.max = Math.max(value, stationMeasurement.max);
                    stationMeasurement.sum += value;
                }

            }
            catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
            finally {
                countDownLatch.countDown();
            }
            return map;
        }
    }

    // TODO: Create new instance of some class if needed, to store similar fields
    private static long fileSize;

    public static void main(String[] args) throws Exception {
        Map<String, StationMeasurement> map = new TreeMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(NUM_TASKS);
        Locale.setDefault(Locale.US);

        try (ExecutorService executorService = Executors.newFixedThreadPool(NUM_CPUS);
                FileInputStream fileInputStream = new FileInputStream(FILE);
                FileChannel channel = fileInputStream.getChannel()) {

            fileSize = channel.size();

            int fileSizePerThread = (int) Math.max(Math.ceil(fileSize / (float) NUM_TASKS), 1000);
            long sizeAccountedFor = 0;

            List<Future<Map<String, StationMeasurement>>> futures = new ArrayList<>(NUM_TASKS);
            for (int i = 0; i < NUM_TASKS; i++) {
                if (sizeAccountedFor >= fileSize) {
                    // The file is so small that because of the minimum file size per thread, we've covered it in less
                    // threads than expected
                    countDownLatch.countDown();
                    continue;
                }
                futures.add(executorService.submit(new DataProcessor(i, fileSizePerThread, channel, countDownLatch)));
                sizeAccountedFor += fileSizePerThread;
            }
            countDownLatch.await();

            // TODO: Try using multiple threads, try freeing up memory quicker by merging as they complete
            for (Future<Map<String, StationMeasurement>> future : futures) {
                Map<String, StationMeasurement> futureMap = future.get();
                futureMap.entrySet().forEach(entry -> map.merge(entry.getKey(), entry.getValue(),
                        (st1, st2) -> {
                            st1.sum += st2.sum;
                            st1.count += st2.count;
                            st1.min = Math.min(st1.min, st2.min);
                            st1.max = Math.max(st1.max, st2.max);
                            return st1;
                        }));
            }
        }

        StringBuilder result = new StringBuilder("{");
        boolean first = true;
        for (StationMeasurement stationMeasurement : map.values()) {
            if (!first) {
                result.append(", ");
            }
            first = false;
            result.append(stationMeasurement.stationName).append("=");
            result.append(stationMeasurement.min);
            result.append(String.format("/%.1f/", (stationMeasurement.sum / stationMeasurement.count)));
            result.append(stationMeasurement.max);
        }
        result.append("}");

        System.out.println(result);
    }

    private static long getGarbageCollectionTime() {
        long collectionTime = 0;
        for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            collectionTime += garbageCollectorMXBean.getCollectionTime();
        }
        return collectionTime;
    }
}
