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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_tkosachev {

    private static final String FILE = "./measurements.txt";
    public static int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), 8);

    private record ResultRow(int min, double mean, int max) {
        public String toString() {
            return STR."\{round(min)}/\{round(mean)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0;
        private long count = 0;

        public void newValue(int m) {
            if (m < min) {
                min = m;
            }
            if (m > max) {
                max = m;
            }
            sum += m;
            count++;
        }

        public void mergeIn(MeasurementAggregator add) {
            if (add.min < min) {
                min = add.min;
            }
            if (add.max > max) {
                max = add.max;
            }
            sum += add.sum;
            count += add.count;
        }
    }

    public static void main(String[] args) {
        Path path = Paths.get(args.length == 0 ? FILE : args[0]);

        Map<String, MeasurementAggregator> total;
        try (RandomAccessFile aFile = new RandomAccessFile(path.toFile(), "r");
                ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            FileChannel inChannel = aFile.getChannel();
            int numChunks = args.length > 1 ? Integer.parseInt(args[1]) : 100;

            if (inChannel.size() < 1024 * 1024 * 1024) {
                numThreads = 1;
                numChunks = 1;
            }

            List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>(numThreads);
            int bufferSize = (int) (inChannel.size() / numChunks) + 100;
            for (int i = 0; i < numChunks; i++) {
                final int finalI = i;
                futures.add(executorService.submit(() -> processBuffer(inChannel, bufferSize, finalI)));
            }
            executorService.shutdown();
            total = new HashMap<>();
            for (Future<Map<String, MeasurementAggregator>> future : futures) {
                mergeIn(total, future.get());
            }
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        printResults(total);
    }

    private static void mergeIn(Map<String, MeasurementAggregator> total, Map<String, MeasurementAggregator> result) {
        for (String name : result.keySet()) {
            MeasurementAggregator totalAggregator = total.computeIfAbsent(name, _ -> new MeasurementAggregator());
            totalAggregator.mergeIn(result.get(name));
        }
    }

    private static Map<String, MeasurementAggregator> processBuffer(FileChannel channel, int bufferSize, int nr) throws IOException {
        HashMap<String, MeasurementAggregator> aggregatorMap = new HashMap<>();
        long start = ((long) nr) * bufferSize;
        long length = Math.min(bufferSize, channel.size() - start);
        ByteBuffer byteBuffer = channel.map(
                FileChannel.MapMode.READ_ONLY,
                start,
                length);
        int i = 0;
        int smcIndex = -1;
        byte[] buf = new byte[1024];
        int count = 0;
        if (nr > 0) {
            do {
                i++;
            } while (byteBuffer.get() != '\n');
        }
        while (i < length) {
            byte b = byteBuffer.get();
            buf[count] = b;
            if (b == ';') {
                smcIndex = count;
            }
            count++;
            if (b == '\n') {
                String name = new String(buf, 0, smcIndex);
                int value = fastParse(buf, smcIndex + 1, count - smcIndex - 2);
                aggregatorMap.computeIfAbsent(name, _ -> new MeasurementAggregator()).newValue(value);
                count = 0;
            }
            i++;
        }

        return aggregatorMap;
    }

    private static void printResults(Map<String, MeasurementAggregator> result) {
        Map<String, ResultRow> measurements = new TreeMap<>();
        for (Map.Entry<String, MeasurementAggregator> entry : result.entrySet()) {
            MeasurementAggregator value = entry.getValue();
            measurements.put(entry.getKey(), new ResultRow(value.min, ((double) value.sum / value.count), value.max));
        }
        System.out.println(measurements);
    }

    public static int fastParse(byte[] buf, int start, int len) {
        int i = 0;
        int sign = 1;
        for (int index = start; index < start + len; index++) {
            byte b = buf[index];
            if (b == '-') {
                sign = -1;
            }
            if (b >= '0' && b <= '9') {
                i = i * 10 + (b - '0');
            }
        }
        return i * sign;
    }
}
