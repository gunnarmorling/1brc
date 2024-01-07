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
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_eriklumme {

    private static final String FILE = "./measurements.txt";
    private static final int NUM_CPUS = 8;
    private static final int LINE_OVERHEAD = 40;

    private static class StationMeasurement {
        private final String stationName;

        private StationMeasurement(String stationName) {
            this.stationName = stationName;
        }

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private long count = 0;
    }

    private enum Mode {
        UNINITIALIZED,
        READ_STATION,
        READ_VALUE
    }

    public static class DataProcessor implements Callable<Void> {

        private final Map<String, StationMeasurement> stationMeasurementMap;
        private final int processorIndex;
        private final int size;
        private final FileChannel fileChannel;
        private final CountDownLatch countDownLatch;

        public DataProcessor(Map<String, StationMeasurement> stationMeasurementMap, int processorIndex, int size, FileChannel fileChannel,
                             CountDownLatch countDownLatch) {
            this.stationMeasurementMap = stationMeasurementMap;
            this.processorIndex = processorIndex;
            this.size = size;
            this.fileChannel = fileChannel;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public Void call() throws Exception {
            char[] stationBuffer = new char[40];
            int stationIndex = 0;

            char[] valueBuffer = new char[5];
            int valueIndex = 0;

            Mode mode = processorIndex == 0 ? Mode.READ_STATION : Mode.UNINITIALIZED;
            char c = 0;

            String name = Thread.currentThread().getName();

            long offset = ((long) size) * processorIndex;
            // TODO: Don't use hardcoded index
            long sizeWithOffset = processorIndex == 19 ? size : size + LINE_OVERHEAD;

            // System.out.println("[" + Thread.currentThread().getName() + "] " + "Starting...");

            try {
                // System.out.println("[" + Thread.currentThread().getName() + "] " + "2Starting...");
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, sizeWithOffset);
                // System.out.println("[" + Thread.currentThread().getName() + "] " + "Buffer position is: " + buffer.position() + ", reading from " + offset + " to "
                // + (offset + sizeWithOffset));

                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

                // System.out.println("[" + name + "] " + "Size is: " + size);
                // System.out.println("Chars: " + charBuffer.length());

                while (charBuffer.hasRemaining()) {
                    c = charBuffer.get();
                    if (c == '\n') {
                        // We have a station to store
                        if (mode == Mode.READ_VALUE) {
                            String stationName = String.valueOf(Arrays.copyOfRange(stationBuffer, 0, stationIndex));

                            double value = Double.parseDouble(String.valueOf(Arrays.copyOfRange(valueBuffer, 0, valueIndex)));

                            // TODO: Synchronization or atomic fields
                            StationMeasurement stationMeasurement = stationMeasurementMap.computeIfAbsent(stationName, StationMeasurement::new);

                            stationMeasurement.count++;
                            stationMeasurement.min = Math.min(value, stationMeasurement.min);
                            stationMeasurement.max = Math.max(value, stationMeasurement.max);
                            stationMeasurement.sum += value;

                            stationIndex = 0;
                            valueIndex = 0;
                        }
                        mode = Mode.READ_STATION;

                        // We've run past our boundary
                        if (charBuffer.position() >= size) {
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
                    String stationName = String.valueOf(Arrays.copyOfRange(stationBuffer, 0, stationIndex));
                    double value = Double.parseDouble(String.valueOf(Arrays.copyOfRange(valueBuffer, 0, valueIndex)));

                    StationMeasurement stationMeasurement = stationMeasurementMap.computeIfAbsent(stationName, StationMeasurement::new);

                    stationMeasurement.count++;
                    stationMeasurement.min = Math.min(value, stationMeasurement.min);
                    stationMeasurement.max = Math.max(value, stationMeasurement.max);
                    stationMeasurement.sum += value;
                }

            }
            catch (Error e) {
                System.out.println("[" + name + "] ERROREREROROOR");
                System.out.println(Thread.currentThread().getName() + " >>>>>>>>>>>>>>>>>>>>>>>>>>>>> MUCH EXCEPTION WOW");
                System.out.println("Station buffer: " + Arrays.toString(stationBuffer));
                System.out.println("Value buffer: " + Arrays.toString(valueBuffer));
                System.out.println("Mode: " + mode + ", char '" + c + "'");
                e.printStackTrace();
                System.exit(1);
            }
            finally {
                countDownLatch.countDown();
            }
            return null;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int numDividers = 100;
        Map<String, StationMeasurement> map = new ConcurrentSkipListMap<>();
        CountDownLatch countDownLatch = new CountDownLatch(numDividers);

        // try (BufferedReader reader = new BufferedReader(new FileReader(FILE))) {
        // reader.lines().forEach(line -> {
        // if (line.startsWith("İzmir")) {
        // double value = Double.parseDouble(line.substring(6));
        // if (value < -20) {
        // System.out.println("Got Izmir value " + value);
        // }
        // }
        // });
        // }
        // System.exit(1);

        try (ExecutorService executorService = Executors.newFixedThreadPool(NUM_CPUS);
                FileInputStream fileInputStream = new FileInputStream(FILE);
                FileChannel channel = fileInputStream.getChannel()) {

            long fileSize = channel.size();
            System.out.println("File is " + fileSize);

            int fileSizePerThread = (int) (fileSize / numDividers);

            for (int i = 0; i < numDividers; i++) {
                executorService.submit(new DataProcessor(map, i, fileSizePerThread, channel, countDownLatch));
            }
            countDownLatch.await();
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
}
