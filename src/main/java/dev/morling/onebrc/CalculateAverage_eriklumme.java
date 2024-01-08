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
            result.append(DECIMAL_LOOKUP[stationMeasurement.min + 1000]);
            result.append(String.format("/%.1f/", (stationMeasurement.sum / (stationMeasurement.count * 10.0))));
            result.append(DECIMAL_LOOKUP[stationMeasurement.max + 1000]);
        }
        result.append("}");

        System.out.println(result);
    }

    private static class StationMeasurement {
        private final ByteArrayWrapper stationName;

        private StationMeasurement(ByteArrayWrapper stationName) {
            this.stationName = stationName;
        }

        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0;
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
                // Read from buffer in chunks for improved performance
                byte[] bytes = new byte[(int) (sizeWithOverhead / 6) + 1];

                while (buffer.hasRemaining()) {
                    long bytesRemaining = sizeWithOverhead - buffer.position();
                    int bytesOffset, bytesLength;
                    if (bytesRemaining >= bytes.length) {
                        bytesOffset = 0;
                        bytesLength = bytes.length;
                    }
                    else {
                        bytesOffset = (int) (bytes.length - bytesRemaining);
                        bytesLength = (int) bytesRemaining;
                    }
                    buffer.get(bytes, bytesOffset, bytesLength);

                    for (int i = bytesOffset; i < bytes.length; i++) {
                        b = bytes[i];
                        if (b == '\n') {
                            // We have a station to store
                            if (mode == Mode.READ_VALUE) {
                                storeStation(map, stationBuffer, stationIndex, valueBuffer, valueIndex);
                                stationIndex = 0;
                                valueIndex = 0;
                            }
                            mode = Mode.READ_STATION;

                            // We've run past our size, can happen
                            if (buffer.position() - bytes.length + i > fileSizePerThread) {
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

            int value = 0;
            for (int i = 0; i < valueIndex; i++) {
                byte b = valueBuffer[valueIndex - i - 1];
                if (i == 1) {
                    // Skip the decimal point
                }
                else if (b == '-') {
                    // Number is negative
                    value = (-value);
                }
                else {
                    int valueAtIndex = b - 48;
                    if (i == 0) {
                        value += valueAtIndex;
                    }
                    else {
                        value += valueAtIndex * (i == 2 ? 10 : 100);
                    }
                }
            }
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

    private static final String[] DECIMAL_LOOKUP = new String[]{
            "-100.0", "-99.9", "-99.8", "-99.7", "-99.6", "-99.5", "-99.4", "-99.3", "-99.2", "-99.1", "-99.0", "-98.9", "-98.8", "-98.7", "-98.6", "-98.5", "-98.4",
            "-98.3", "-98.2", "-98.1", "-98.0", "-97.9", "-97.8", "-97.7", "-97.6", "-97.5", "-97.4", "-97.3", "-97.2", "-97.1", "-97.0", "-96.9", "-96.8", "-96.7",
            "-96.6", "-96.5", "-96.4", "-96.3", "-96.2", "-96.1", "-96.0", "-95.9", "-95.8", "-95.7", "-95.6", "-95.5", "-95.4", "-95.3", "-95.2", "-95.1", "-95.0",
            "-94.9", "-94.8", "-94.7", "-94.6", "-94.5", "-94.4", "-94.3", "-94.2", "-94.1", "-94.0", "-93.9", "-93.8", "-93.7", "-93.6", "-93.5", "-93.4", "-93.3",
            "-93.2", "-93.1", "-93.0", "-92.9", "-92.8", "-92.7", "-92.6", "-92.5", "-92.4", "-92.3", "-92.2", "-92.1", "-92.0", "-91.9", "-91.8", "-91.7", "-91.6",
            "-91.5", "-91.4", "-91.3", "-91.2", "-91.1", "-91.0", "-90.9", "-90.8", "-90.7", "-90.6", "-90.5", "-90.4", "-90.3", "-90.2", "-90.1", "-90.0", "-89.9",
            "-89.8", "-89.7", "-89.6", "-89.5", "-89.4", "-89.3", "-89.2", "-89.1", "-89.0", "-88.9", "-88.8", "-88.7", "-88.6", "-88.5", "-88.4", "-88.3", "-88.2",
            "-88.1", "-88.0", "-87.9", "-87.8", "-87.7", "-87.6", "-87.5", "-87.4", "-87.3", "-87.2", "-87.1", "-87.0", "-86.9", "-86.8", "-86.7", "-86.6", "-86.5",
            "-86.4", "-86.3", "-86.2", "-86.1", "-86.0", "-85.9", "-85.8", "-85.7", "-85.6", "-85.5", "-85.4", "-85.3", "-85.2", "-85.1", "-85.0", "-84.9", "-84.8",
            "-84.7", "-84.6", "-84.5", "-84.4", "-84.3", "-84.2", "-84.1", "-84.0", "-83.9", "-83.8", "-83.7", "-83.6", "-83.5", "-83.4", "-83.3", "-83.2", "-83.1",
            "-83.0", "-82.9", "-82.8", "-82.7", "-82.6", "-82.5", "-82.4", "-82.3", "-82.2", "-82.1", "-82.0", "-81.9", "-81.8", "-81.7", "-81.6", "-81.5", "-81.4",
            "-81.3", "-81.2", "-81.1", "-81.0", "-80.9", "-80.8", "-80.7", "-80.6", "-80.5", "-80.4", "-80.3", "-80.2", "-80.1", "-80.0", "-79.9", "-79.8", "-79.7",
            "-79.6", "-79.5", "-79.4", "-79.3", "-79.2", "-79.1", "-79.0", "-78.9", "-78.8", "-78.7", "-78.6", "-78.5", "-78.4", "-78.3", "-78.2", "-78.1", "-78.0",
            "-77.9", "-77.8", "-77.7", "-77.6", "-77.5", "-77.4", "-77.3", "-77.2", "-77.1", "-77.0", "-76.9", "-76.8", "-76.7", "-76.6", "-76.5", "-76.4", "-76.3",
            "-76.2", "-76.1", "-76.0", "-75.9", "-75.8", "-75.7", "-75.6", "-75.5", "-75.4", "-75.3", "-75.2", "-75.1", "-75.0", "-74.9", "-74.8", "-74.7", "-74.6",
            "-74.5", "-74.4", "-74.3", "-74.2", "-74.1", "-74.0", "-73.9", "-73.8", "-73.7", "-73.6", "-73.5", "-73.4", "-73.3", "-73.2", "-73.1", "-73.0", "-72.9",
            "-72.8", "-72.7", "-72.6", "-72.5", "-72.4", "-72.3", "-72.2", "-72.1", "-72.0", "-71.9", "-71.8", "-71.7", "-71.6", "-71.5", "-71.4", "-71.3", "-71.2",
            "-71.1", "-71.0", "-70.9", "-70.8", "-70.7", "-70.6", "-70.5", "-70.4", "-70.3", "-70.2", "-70.1", "-70.0", "-69.9", "-69.8", "-69.7", "-69.6", "-69.5",
            "-69.4", "-69.3", "-69.2", "-69.1", "-69.0", "-68.9", "-68.8", "-68.7", "-68.6", "-68.5", "-68.4", "-68.3", "-68.2", "-68.1", "-68.0", "-67.9", "-67.8",
            "-67.7", "-67.6", "-67.5", "-67.4", "-67.3", "-67.2", "-67.1", "-67.0", "-66.9", "-66.8", "-66.7", "-66.6", "-66.5", "-66.4", "-66.3", "-66.2", "-66.1",
            "-66.0", "-65.9", "-65.8", "-65.7", "-65.6", "-65.5", "-65.4", "-65.3", "-65.2", "-65.1", "-65.0", "-64.9", "-64.8", "-64.7", "-64.6", "-64.5", "-64.4",
            "-64.3", "-64.2", "-64.1", "-64.0", "-63.9", "-63.8", "-63.7", "-63.6", "-63.5", "-63.4", "-63.3", "-63.2", "-63.1", "-63.0", "-62.9", "-62.8", "-62.7",
            "-62.6", "-62.5", "-62.4", "-62.3", "-62.2", "-62.1", "-62.0", "-61.9", "-61.8", "-61.7", "-61.6", "-61.5", "-61.4", "-61.3", "-61.2", "-61.1", "-61.0",
            "-60.9", "-60.8", "-60.7", "-60.6", "-60.5", "-60.4", "-60.3", "-60.2", "-60.1", "-60.0", "-59.9", "-59.8", "-59.7", "-59.6", "-59.5", "-59.4", "-59.3",
            "-59.2", "-59.1", "-59.0", "-58.9", "-58.8", "-58.7", "-58.6", "-58.5", "-58.4", "-58.3", "-58.2", "-58.1", "-58.0", "-57.9", "-57.8", "-57.7", "-57.6",
            "-57.5", "-57.4", "-57.3", "-57.2", "-57.1", "-57.0", "-56.9", "-56.8", "-56.7", "-56.6", "-56.5", "-56.4", "-56.3", "-56.2", "-56.1", "-56.0", "-55.9",
            "-55.8", "-55.7", "-55.6", "-55.5", "-55.4", "-55.3", "-55.2", "-55.1", "-55.0", "-54.9", "-54.8", "-54.7", "-54.6", "-54.5", "-54.4", "-54.3", "-54.2",
            "-54.1", "-54.0", "-53.9", "-53.8", "-53.7", "-53.6", "-53.5", "-53.4", "-53.3", "-53.2", "-53.1", "-53.0", "-52.9", "-52.8", "-52.7", "-52.6", "-52.5",
            "-52.4", "-52.3", "-52.2", "-52.1", "-52.0", "-51.9", "-51.8", "-51.7", "-51.6", "-51.5", "-51.4", "-51.3", "-51.2", "-51.1", "-51.0", "-50.9", "-50.8",
            "-50.7", "-50.6", "-50.5", "-50.4", "-50.3", "-50.2", "-50.1", "-50.0", "-49.9", "-49.8", "-49.7", "-49.6", "-49.5", "-49.4", "-49.3", "-49.2", "-49.1",
            "-49.0", "-48.9", "-48.8", "-48.7", "-48.6", "-48.5", "-48.4", "-48.3", "-48.2", "-48.1", "-48.0", "-47.9", "-47.8", "-47.7", "-47.6", "-47.5", "-47.4",
            "-47.3", "-47.2", "-47.1", "-47.0", "-46.9", "-46.8", "-46.7", "-46.6", "-46.5", "-46.4", "-46.3", "-46.2", "-46.1", "-46.0", "-45.9", "-45.8", "-45.7",
            "-45.6", "-45.5", "-45.4", "-45.3", "-45.2", "-45.1", "-45.0", "-44.9", "-44.8", "-44.7", "-44.6", "-44.5", "-44.4", "-44.3", "-44.2", "-44.1", "-44.0",
            "-43.9", "-43.8", "-43.7", "-43.6", "-43.5", "-43.4", "-43.3", "-43.2", "-43.1", "-43.0", "-42.9", "-42.8", "-42.7", "-42.6", "-42.5", "-42.4", "-42.3",
            "-42.2", "-42.1", "-42.0", "-41.9", "-41.8", "-41.7", "-41.6", "-41.5", "-41.4", "-41.3", "-41.2", "-41.1", "-41.0", "-40.9", "-40.8", "-40.7", "-40.6",
            "-40.5", "-40.4", "-40.3", "-40.2", "-40.1", "-40.0", "-39.9", "-39.8", "-39.7", "-39.6", "-39.5", "-39.4", "-39.3", "-39.2", "-39.1", "-39.0", "-38.9",
            "-38.8", "-38.7", "-38.6", "-38.5", "-38.4", "-38.3", "-38.2", "-38.1", "-38.0", "-37.9", "-37.8", "-37.7", "-37.6", "-37.5", "-37.4", "-37.3", "-37.2",
            "-37.1", "-37.0", "-36.9", "-36.8", "-36.7", "-36.6", "-36.5", "-36.4", "-36.3", "-36.2", "-36.1", "-36.0", "-35.9", "-35.8", "-35.7", "-35.6", "-35.5",
            "-35.4", "-35.3", "-35.2", "-35.1", "-35.0", "-34.9", "-34.8", "-34.7", "-34.6", "-34.5", "-34.4", "-34.3", "-34.2", "-34.1", "-34.0", "-33.9", "-33.8",
            "-33.7", "-33.6", "-33.5", "-33.4", "-33.3", "-33.2", "-33.1", "-33.0", "-32.9", "-32.8", "-32.7", "-32.6", "-32.5", "-32.4", "-32.3", "-32.2", "-32.1",
            "-32.0", "-31.9", "-31.8", "-31.7", "-31.6", "-31.5", "-31.4", "-31.3", "-31.2", "-31.1", "-31.0", "-30.9", "-30.8", "-30.7", "-30.6", "-30.5", "-30.4",
            "-30.3", "-30.2", "-30.1", "-30.0", "-29.9", "-29.8", "-29.7", "-29.6", "-29.5", "-29.4", "-29.3", "-29.2", "-29.1", "-29.0", "-28.9", "-28.8", "-28.7",
            "-28.6", "-28.5", "-28.4", "-28.3", "-28.2", "-28.1", "-28.0", "-27.9", "-27.8", "-27.7", "-27.6", "-27.5", "-27.4", "-27.3", "-27.2", "-27.1", "-27.0",
            "-26.9", "-26.8", "-26.7", "-26.6", "-26.5", "-26.4", "-26.3", "-26.2", "-26.1", "-26.0", "-25.9", "-25.8", "-25.7", "-25.6", "-25.5", "-25.4", "-25.3",
            "-25.2", "-25.1", "-25.0", "-24.9", "-24.8", "-24.7", "-24.6", "-24.5", "-24.4", "-24.3", "-24.2", "-24.1", "-24.0", "-23.9", "-23.8", "-23.7", "-23.6",
            "-23.5", "-23.4", "-23.3", "-23.2", "-23.1", "-23.0", "-22.9", "-22.8", "-22.7", "-22.6", "-22.5", "-22.4", "-22.3", "-22.2", "-22.1", "-22.0", "-21.9",
            "-21.8", "-21.7", "-21.6", "-21.5", "-21.4", "-21.3", "-21.2", "-21.1", "-21.0", "-20.9", "-20.8", "-20.7", "-20.6", "-20.5", "-20.4", "-20.3", "-20.2",
            "-20.1", "-20.0", "-19.9", "-19.8", "-19.7", "-19.6", "-19.5", "-19.4", "-19.3", "-19.2", "-19.1", "-19.0", "-18.9", "-18.8", "-18.7", "-18.6", "-18.5",
            "-18.4", "-18.3", "-18.2", "-18.1", "-18.0", "-17.9", "-17.8", "-17.7", "-17.6", "-17.5", "-17.4", "-17.3", "-17.2", "-17.1", "-17.0", "-16.9", "-16.8",
            "-16.7", "-16.6", "-16.5", "-16.4", "-16.3", "-16.2", "-16.1", "-16.0", "-15.9", "-15.8", "-15.7", "-15.6", "-15.5", "-15.4", "-15.3", "-15.2", "-15.1",
            "-15.0", "-14.9", "-14.8", "-14.7", "-14.6", "-14.5", "-14.4", "-14.3", "-14.2", "-14.1", "-14.0", "-13.9", "-13.8", "-13.7", "-13.6", "-13.5", "-13.4",
            "-13.3", "-13.2", "-13.1", "-13.0", "-12.9", "-12.8", "-12.7", "-12.6", "-12.5", "-12.4", "-12.3", "-12.2", "-12.1", "-12.0", "-11.9", "-11.8", "-11.7",
            "-11.6", "-11.5", "-11.4", "-11.3", "-11.2", "-11.1", "-11.0", "-10.9", "-10.8", "-10.7", "-10.6", "-10.5", "-10.4", "-10.3", "-10.2", "-10.1", "-10.0",
            "-9.9", "-9.8", "-9.7", "-9.6", "-9.5", "-9.4", "-9.3", "-9.2", "-9.1", "-9.0", "-8.9", "-8.8", "-8.7", "-8.6", "-8.5", "-8.4", "-8.3", "-8.2", "-8.1",
            "-8.0", "-7.9", "-7.8", "-7.7", "-7.6", "-7.5", "-7.4", "-7.3", "-7.2", "-7.1", "-7.0", "-6.9", "-6.8", "-6.7", "-6.6", "-6.5", "-6.4", "-6.3", "-6.2",
            "-6.1", "-6.0", "-5.9", "-5.8", "-5.7", "-5.6", "-5.5", "-5.4", "-5.3", "-5.2", "-5.1", "-5.0", "-4.9", "-4.8", "-4.7", "-4.6", "-4.5", "-4.4", "-4.3",
            "-4.2", "-4.1", "-4.0", "-3.9", "-3.8", "-3.7", "-3.6", "-3.5", "-3.4", "-3.3", "-3.2", "-3.1", "-3.0", "-2.9", "-2.8", "-2.7", "-2.6", "-2.5", "-2.4",
            "-2.3", "-2.2", "-2.1", "-2.0", "-1.9", "-1.8", "-1.7", "-1.6", "-1.5", "-1.4", "-1.3", "-1.2", "-1.1", "-1.0", "-0.9", "-0.8", "-0.7", "-0.6", "-0.5",
            "-0.4", "-0.3", "-0.2", "-0.1", "0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7",
            "1.8", "1.9", "2.0", "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8", "2.9", "3.0", "3.1", "3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "3.9",
            "4.0", "4.1", "4.2", "4.3", "4.4", "4.5", "4.6", "4.7", "4.8", "4.9", "5.0", "5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7", "5.8", "5.9", "6.0", "6.1",
            "6.2", "6.3", "6.4", "6.5", "6.6", "6.7", "6.8", "6.9", "7.0", "7.1", "7.2", "7.3", "7.4", "7.5", "7.6", "7.7", "7.8", "7.9", "8.0", "8.1", "8.2", "8.3",
            "8.4", "8.5", "8.6", "8.7", "8.8", "8.9", "9.0", "9.1", "9.2", "9.3", "9.4", "9.5", "9.6", "9.7", "9.8", "9.9", "10.0", "10.1", "10.2", "10.3", "10.4",
            "10.5", "10.6", "10.7", "10.8", "10.9", "11.0", "11.1", "11.2", "11.3", "11.4", "11.5", "11.6", "11.7", "11.8", "11.9", "12.0", "12.1", "12.2", "12.3",
            "12.4", "12.5", "12.6", "12.7", "12.8", "12.9", "13.0", "13.1", "13.2", "13.3", "13.4", "13.5", "13.6", "13.7", "13.8", "13.9", "14.0", "14.1", "14.2",
            "14.3", "14.4", "14.5", "14.6", "14.7", "14.8", "14.9", "15.0", "15.1", "15.2", "15.3", "15.4", "15.5", "15.6", "15.7", "15.8", "15.9", "16.0", "16.1",
            "16.2", "16.3", "16.4", "16.5", "16.6", "16.7", "16.8", "16.9", "17.0", "17.1", "17.2", "17.3", "17.4", "17.5", "17.6", "17.7", "17.8", "17.9", "18.0",
            "18.1", "18.2", "18.3", "18.4", "18.5", "18.6", "18.7", "18.8", "18.9", "19.0", "19.1", "19.2", "19.3", "19.4", "19.5", "19.6", "19.7", "19.8", "19.9",
            "20.0", "20.1", "20.2", "20.3", "20.4", "20.5", "20.6", "20.7", "20.8", "20.9", "21.0", "21.1", "21.2", "21.3", "21.4", "21.5", "21.6", "21.7", "21.8",
            "21.9", "22.0", "22.1", "22.2", "22.3", "22.4", "22.5", "22.6", "22.7", "22.8", "22.9", "23.0", "23.1", "23.2", "23.3", "23.4", "23.5", "23.6", "23.7",
            "23.8", "23.9", "24.0", "24.1", "24.2", "24.3", "24.4", "24.5", "24.6", "24.7", "24.8", "24.9", "25.0", "25.1", "25.2", "25.3", "25.4", "25.5", "25.6",
            "25.7", "25.8", "25.9", "26.0", "26.1", "26.2", "26.3", "26.4", "26.5", "26.6", "26.7", "26.8", "26.9", "27.0", "27.1", "27.2", "27.3", "27.4", "27.5",
            "27.6", "27.7", "27.8", "27.9", "28.0", "28.1", "28.2", "28.3", "28.4", "28.5", "28.6", "28.7", "28.8", "28.9", "29.0", "29.1", "29.2", "29.3", "29.4",
            "29.5", "29.6", "29.7", "29.8", "29.9", "30.0", "30.1", "30.2", "30.3", "30.4", "30.5", "30.6", "30.7", "30.8", "30.9", "31.0", "31.1", "31.2", "31.3",
            "31.4", "31.5", "31.6", "31.7", "31.8", "31.9", "32.0", "32.1", "32.2", "32.3", "32.4", "32.5", "32.6", "32.7", "32.8", "32.9", "33.0", "33.1", "33.2",
            "33.3", "33.4", "33.5", "33.6", "33.7", "33.8", "33.9", "34.0", "34.1", "34.2", "34.3", "34.4", "34.5", "34.6", "34.7", "34.8", "34.9", "35.0", "35.1",
            "35.2", "35.3", "35.4", "35.5", "35.6", "35.7", "35.8", "35.9", "36.0", "36.1", "36.2", "36.3", "36.4", "36.5", "36.6", "36.7", "36.8", "36.9", "37.0",
            "37.1", "37.2", "37.3", "37.4", "37.5", "37.6", "37.7", "37.8", "37.9", "38.0", "38.1", "38.2", "38.3", "38.4", "38.5", "38.6", "38.7", "38.8", "38.9",
            "39.0", "39.1", "39.2", "39.3", "39.4", "39.5", "39.6", "39.7", "39.8", "39.9", "40.0", "40.1", "40.2", "40.3", "40.4", "40.5", "40.6", "40.7", "40.8",
            "40.9", "41.0", "41.1", "41.2", "41.3", "41.4", "41.5", "41.6", "41.7", "41.8", "41.9", "42.0", "42.1", "42.2", "42.3", "42.4", "42.5", "42.6", "42.7",
            "42.8", "42.9", "43.0", "43.1", "43.2", "43.3", "43.4", "43.5", "43.6", "43.7", "43.8", "43.9", "44.0", "44.1", "44.2", "44.3", "44.4", "44.5", "44.6",
            "44.7", "44.8", "44.9", "45.0", "45.1", "45.2", "45.3", "45.4", "45.5", "45.6", "45.7", "45.8", "45.9", "46.0", "46.1", "46.2", "46.3", "46.4", "46.5",
            "46.6", "46.7", "46.8", "46.9", "47.0", "47.1", "47.2", "47.3", "47.4", "47.5", "47.6", "47.7", "47.8", "47.9", "48.0", "48.1", "48.2", "48.3", "48.4",
            "48.5", "48.6", "48.7", "48.8", "48.9", "49.0", "49.1", "49.2", "49.3", "49.4", "49.5", "49.6", "49.7", "49.8", "49.9", "50.0", "50.1", "50.2", "50.3",
            "50.4", "50.5", "50.6", "50.7", "50.8", "50.9", "51.0", "51.1", "51.2", "51.3", "51.4", "51.5", "51.6", "51.7", "51.8", "51.9", "52.0", "52.1", "52.2",
            "52.3", "52.4", "52.5", "52.6", "52.7", "52.8", "52.9", "53.0", "53.1", "53.2", "53.3", "53.4", "53.5", "53.6", "53.7", "53.8", "53.9", "54.0", "54.1",
            "54.2", "54.3", "54.4", "54.5", "54.6", "54.7", "54.8", "54.9", "55.0", "55.1", "55.2", "55.3", "55.4", "55.5", "55.6", "55.7", "55.8", "55.9", "56.0",
            "56.1", "56.2", "56.3", "56.4", "56.5", "56.6", "56.7", "56.8", "56.9", "57.0", "57.1", "57.2", "57.3", "57.4", "57.5", "57.6", "57.7", "57.8", "57.9",
            "58.0", "58.1", "58.2", "58.3", "58.4", "58.5", "58.6", "58.7", "58.8", "58.9", "59.0", "59.1", "59.2", "59.3", "59.4", "59.5", "59.6", "59.7", "59.8",
            "59.9", "60.0", "60.1", "60.2", "60.3", "60.4", "60.5", "60.6", "60.7", "60.8", "60.9", "61.0", "61.1", "61.2", "61.3", "61.4", "61.5", "61.6", "61.7",
            "61.8", "61.9", "62.0", "62.1", "62.2", "62.3", "62.4", "62.5", "62.6", "62.7", "62.8", "62.9", "63.0", "63.1", "63.2", "63.3", "63.4", "63.5", "63.6",
            "63.7", "63.8", "63.9", "64.0", "64.1", "64.2", "64.3", "64.4", "64.5", "64.6", "64.7", "64.8", "64.9", "65.0", "65.1", "65.2", "65.3", "65.4", "65.5",
            "65.6", "65.7", "65.8", "65.9", "66.0", "66.1", "66.2", "66.3", "66.4", "66.5", "66.6", "66.7", "66.8", "66.9", "67.0", "67.1", "67.2", "67.3", "67.4",
            "67.5", "67.6", "67.7", "67.8", "67.9", "68.0", "68.1", "68.2", "68.3", "68.4", "68.5", "68.6", "68.7", "68.8", "68.9", "69.0", "69.1", "69.2", "69.3",
            "69.4", "69.5", "69.6", "69.7", "69.8", "69.9", "70.0", "70.1", "70.2", "70.3", "70.4", "70.5", "70.6", "70.7", "70.8", "70.9", "71.0", "71.1", "71.2",
            "71.3", "71.4", "71.5", "71.6", "71.7", "71.8", "71.9", "72.0", "72.1", "72.2", "72.3", "72.4", "72.5", "72.6", "72.7", "72.8", "72.9", "73.0", "73.1",
            "73.2", "73.3", "73.4", "73.5", "73.6", "73.7", "73.8", "73.9", "74.0", "74.1", "74.2", "74.3", "74.4", "74.5", "74.6", "74.7", "74.8", "74.9", "75.0",
            "75.1", "75.2", "75.3", "75.4", "75.5", "75.6", "75.7", "75.8", "75.9", "76.0", "76.1", "76.2", "76.3", "76.4", "76.5", "76.6", "76.7", "76.8", "76.9",
            "77.0", "77.1", "77.2", "77.3", "77.4", "77.5", "77.6", "77.7", "77.8", "77.9", "78.0", "78.1", "78.2", "78.3", "78.4", "78.5", "78.6", "78.7", "78.8",
            "78.9", "79.0", "79.1", "79.2", "79.3", "79.4", "79.5", "79.6", "79.7", "79.8", "79.9", "80.0", "80.1", "80.2", "80.3", "80.4", "80.5", "80.6", "80.7",
            "80.8", "80.9", "81.0", "81.1", "81.2", "81.3", "81.4", "81.5", "81.6", "81.7", "81.8", "81.9", "82.0", "82.1", "82.2", "82.3", "82.4", "82.5", "82.6",
            "82.7", "82.8", "82.9", "83.0", "83.1", "83.2", "83.3", "83.4", "83.5", "83.6", "83.7", "83.8", "83.9", "84.0", "84.1", "84.2", "84.3", "84.4", "84.5",
            "84.6", "84.7", "84.8", "84.9", "85.0", "85.1", "85.2", "85.3", "85.4", "85.5", "85.6", "85.7", "85.8", "85.9", "86.0", "86.1", "86.2", "86.3", "86.4",
            "86.5", "86.6", "86.7", "86.8", "86.9", "87.0", "87.1", "87.2", "87.3", "87.4", "87.5", "87.6", "87.7", "87.8", "87.9", "88.0", "88.1", "88.2", "88.3",
            "88.4", "88.5", "88.6", "88.7", "88.8", "88.9", "89.0", "89.1", "89.2", "89.3", "89.4", "89.5", "89.6", "89.7", "89.8", "89.9", "90.0", "90.1", "90.2",
            "90.3", "90.4", "90.5", "90.6", "90.7", "90.8", "90.9", "91.0", "91.1", "91.2", "91.3", "91.4", "91.5", "91.6", "91.7", "91.8", "91.9", "92.0", "92.1",
            "92.2", "92.3", "92.4", "92.5", "92.6", "92.7", "92.8", "92.9", "93.0", "93.1", "93.2", "93.3", "93.4", "93.5", "93.6", "93.7", "93.8", "93.9", "94.0",
            "94.1", "94.2", "94.3", "94.4", "94.5", "94.6", "94.7", "94.8", "94.9", "95.0", "95.1", "95.2", "95.3", "95.4", "95.5", "95.6", "95.7", "95.8", "95.9",
            "96.0", "96.1", "96.2", "96.3", "96.4", "96.5", "96.6", "96.7", "96.8", "96.9", "97.0", "97.1", "97.2", "97.3", "97.4", "97.5", "97.6", "97.7", "97.8",
            "97.9", "98.0", "98.1", "98.2", "98.3", "98.4", "98.5", "98.6", "98.7", "98.8", "98.9", "99.0", "99.1", "99.2", "99.3", "99.4", "99.5", "99.6", "99.7",
            "99.8", "99.9" };
}
