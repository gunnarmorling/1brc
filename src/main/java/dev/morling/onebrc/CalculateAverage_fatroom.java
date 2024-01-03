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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_fatroom {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator() {
            this(1000, -1000, 0, 0);
        }

        public MeasurementAggregator(int min, int max, long sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public void consume(double value) {
            this.min = this.min < value ? this.min : value;
            this.max = this.max > value ? this.max : value;
            this.sum += value;
            this.count++;
        }

        public MeasurementAggregator combineWith(MeasurementAggregator that) {
            this.min = this.min < that.min ? this.min : that.min;
            this.max = this.max > that.max ? this.max : that.max;
            this.sum += that.sum;
            this.count += that.count;
            return this;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "%.1f/%.1f/%.1f", min / 10.0, Math.round(sum / count * 10.0) / 100.0, max / 10.0);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        int SEGMENT_LENGTH = 256_000_000; // 256 MB

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        long fileSize = file.length();
        long position = 0;

        List<Callable<Map<String, MeasurementAggregator>>> tasks = new ArrayList<>();
        while (position < fileSize) {
            long end = Math.min(position + SEGMENT_LENGTH, fileSize);
            int length = (int) (end - position);
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, position, length);
            while (buffer.get(length - 1) != '\n') {
                length--;
            }
            final int finalLength = length;
            tasks.add(() -> processBuffer(buffer, finalLength));
            position += length;
        }

        var executor = Executors.newFixedThreadPool(tasks.size());

        Map<String, MeasurementAggregator> aggregates = new TreeMap<>();
        for (Future<Map<String, MeasurementAggregator>> future : executor.invokeAll(tasks)) {
            Map<String, MeasurementAggregator> segmentAggregates = future.get();
            for (Map.Entry<String, MeasurementAggregator> entry : segmentAggregates.entrySet()) {
                MeasurementAggregator aggregator = aggregates.computeIfAbsent(entry.getKey(), s -> new MeasurementAggregator());
                aggregator.combineWith(entry.getValue());
            }
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        System.out.println(aggregates);
    }

    private static Map<String, MeasurementAggregator> processBuffer(MappedByteBuffer source, int length) {
        Map<String, MeasurementAggregator> aggregates = new HashMap<>();
        String station = null;
        byte[] buffer = new byte[64];
        int idx = 0;
        for (int i = 0; i < length; ++i) {
            byte b = source.get(i);
            buffer[idx++] = b;
            if (b == ';') {
                station = new String(buffer, 0, idx - 1, StandardCharsets.UTF_8);
                idx = 0;
            }
            else if (b == '\n') {
                double temperature = parseMeasurement(buffer, idx - 1);
                MeasurementAggregator aggregator = aggregates.computeIfAbsent(station, s -> new MeasurementAggregator());
                aggregator.consume(temperature);
                idx = 0;
            }
        }
        return aggregates;
    }

    static double parseMeasurement(byte[] source, int size) {
        int isNegativeSignPresent = ~(source[0] >> 4) & 1;
        int has4 = (size - isNegativeSignPresent) >> 2;
        int firstDigit = source[isNegativeSignPresent] - '0';
        int secondDigit = source[isNegativeSignPresent + has4] - '0';
        int thirdDigit = source[isNegativeSignPresent + 2 + has4] - '0';
        int value = has4 * firstDigit * 100 + secondDigit * 10 + thirdDigit;
        return -isNegativeSignPresent ^ value - isNegativeSignPresent;
    }
}
