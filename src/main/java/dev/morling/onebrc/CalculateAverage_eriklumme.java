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

import java.io.FileInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_eriklumme {

    private static final String FILE = "./measurements.txt";
    private static final int NUM_CPUS = 8;
    private static final int LINE_OVERHEAD = 200;
    private static final int NUM_TASKS = NUM_CPUS * 6;

    private final CountDownLatch countDownLatch = new CountDownLatch(NUM_TASKS);

    private final FileInputStream fileInputStream = new FileInputStream(FILE);
    private final FileChannel fileChannel = fileInputStream.getChannel();
    private final long fileSize = fileChannel.size();
    private final int fileSizePerThread = (int) Math.max(Math.ceil(fileSize / (float) NUM_TASKS), 1000);

    private CalculateAverage_eriklumme() throws Exception {
        Map<ByteArrayWrapper, StationMeasurement> map = new HashMap<>();

        try (ExecutorService executorService = Executors.newFixedThreadPool(NUM_CPUS); fileInputStream; fileChannel) {
            ;
            long sizeAccountedFor = 0;

            List<Future<Map<ByteArrayWrapper, StationMeasurement>>> futures = new ArrayList<>(NUM_TASKS);
            for (int i = 0; i < NUM_TASKS; i++) {
                if (sizeAccountedFor >= fileSize) {
                    // The file is so small that because of the minimum file size per thread, we've covered it in less
                    // threads than expected
                    countDownLatch.countDown();
                    continue;
                }
                futures.add(executorService.submit(new DataProcessor(i)));
                sizeAccountedFor += fileSizePerThread;
            }
            countDownLatch.await();

            for (Future<Map<ByteArrayWrapper, StationMeasurement>> future : futures) {
                Map<ByteArrayWrapper, StationMeasurement> futureMap = future.get();
                futureMap.forEach((key, value) -> map.merge(key, value,
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
        List<StationMeasurement> values = new ArrayList<>(map.values());
        values.sort(Comparator.comparing(StationMeasurement::stringName));

        for (StationMeasurement stationMeasurement : values) {
            if (!first) {
                result.append(", ");
            }
            first = false;
            result.append(new String(stationMeasurement.stationName.value, StandardCharsets.UTF_8)).append("=");
            result.append(stationMeasurement.min);
            result.append(String.format("/%.1f/", (stationMeasurement.sum / stationMeasurement.count)));
            result.append(stationMeasurement.max);
        }
        result.append("}");

        System.out.println(result);
    }

    private static class StationMeasurement {
        private final ByteArrayWrapper stationName;

        private StationMeasurement(ByteArrayWrapper stationName) {
            this.stationName = stationName;
        }

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private int count = 0;

        public String stringName() {
            return new String(stationName.value, StandardCharsets.UTF_8);
        }
    }

    private enum Mode {
        UNINITIALIZED,
        READ_STATION,
        READ_VALUE
    }

    private record ByteArrayWrapper(byte[] value) {

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o instanceof ByteArrayWrapper that) {
                return Arrays.equals(value, that.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    public class DataProcessor implements Callable<Map<ByteArrayWrapper, StationMeasurement>> {

        private final int processorIndex;

        public DataProcessor(int processorIndex) {
            this.processorIndex = processorIndex;
        }

        @Override
        public Map<ByteArrayWrapper, StationMeasurement> call() throws Exception {
            Map<ByteArrayWrapper, StationMeasurement> map = new HashMap<>();

            byte[] stationBuffer = new byte[200];
            int stationIndex = 0;

            byte[] valueBuffer = new byte[10];
            int valueIndex = 0;

            Mode mode = processorIndex == 0 ? Mode.READ_STATION : Mode.UNINITIALIZED;
            byte b;

            long offset = ((long) fileSizePerThread) * processorIndex;
            long sizeWithOverhead = Math.min(((long) fileSizePerThread) + LINE_OVERHEAD, fileSize - offset);

            try {
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, sizeWithOverhead);

                while (buffer.hasRemaining()) {
                    b = buffer.get();
                    if (b == '\n') {
                        // We have a station to store
                        if (mode == Mode.READ_VALUE) {
                            storeStation(map, stationBuffer, stationIndex, valueBuffer, valueIndex);
                            stationIndex = 0;
                            valueIndex = 0;
                        }
                        mode = Mode.READ_STATION;

                        // We've run past our size, can happen
                        if (buffer.position() > fileSizePerThread) {
                            break;
                        }
                    }
                    else if (mode == Mode.UNINITIALIZED) {
                        // Do-nothing, read more
                    }
                    else if (b == ';') {
                        mode = Mode.READ_VALUE;
                    }
                    else if (mode == Mode.READ_STATION) {
                        stationBuffer[stationIndex++] = b;
                    }
                    else {
                        valueBuffer[valueIndex++] = b;
                    }
                }
                if (mode == Mode.READ_VALUE && valueIndex > 0) {
                    // One value left to store
                    storeStation(map, stationBuffer, stationIndex, valueBuffer, valueIndex);
                }
            }
            finally {
                countDownLatch.countDown();
            }
            return map;
        }

        private void storeStation(Map<ByteArrayWrapper, StationMeasurement> map, byte[] stationBuffer, int stationIndex, byte[] valueBuffer, int valueIndex) {
            ByteArrayWrapper stationName = new ByteArrayWrapper(Arrays.copyOfRange(stationBuffer, 0, stationIndex));
            double value = Double.parseDouble(new String(Arrays.copyOfRange(valueBuffer, 0, valueIndex)));

            StationMeasurement stationMeasurement = map.computeIfAbsent(stationName, StationMeasurement::new);
            stationMeasurement.count++;
            stationMeasurement.min = Math.min(value, stationMeasurement.min);
            stationMeasurement.max = Math.max(value, stationMeasurement.max);
            stationMeasurement.sum += value;
        }
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.US);
        new CalculateAverage_eriklumme();
    }
}
