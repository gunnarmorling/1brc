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

import org.radughiorma.Arguments;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_criccomini {
    private static final long SEGMENT_SIZE = 256_000_000;

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static Map<String, MeasurementAggregator> processSegment(MappedByteBuffer buffer, int length) {
        Map<String, MeasurementAggregator> aggregates = new HashMap<>();
        int lineStart = 0;
        int doubleStart = 0;
        String station = null;
        for (int i = 0; i < length; ++i) {
            byte b = buffer.get(i);
            if (b == ';') {
                byte[] stationBuffer = new byte[i - lineStart];
                buffer.position(lineStart);
                buffer.get(stationBuffer);
                station = new String(stationBuffer, StandardCharsets.UTF_8);
                doubleStart = i + 1;
            }
            else if (b == '\n') {
                byte[] doubleBuffer = new byte[i - doubleStart];
                buffer.position(doubleStart);
                buffer.get(doubleBuffer);
                Double temperature = Double.parseDouble(new String(doubleBuffer));
                lineStart = i + 1;

                MeasurementAggregator aggregator = aggregates.computeIfAbsent(station, s -> new MeasurementAggregator());
                aggregator.min = Math.min(aggregator.min, temperature);
                aggregator.max = Math.max(aggregator.max, temperature);
                aggregator.sum += temperature;
                aggregator.count++;
            }
        }
        return aggregates;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        String FILE = Arguments.measurmentsFilename(args);
        long FILE_SIZE = new File(FILE).length();
        ExecutorService executor = Executors.newFixedThreadPool(128);
        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        long position = 0;
        List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
        while (position < FILE_SIZE) {
            int end = (int) Math.min(position + SEGMENT_SIZE, FILE_SIZE);
            int length = (int) (end - position);
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, position, length);
            while (buffer.get(length - 1) != '\n') {
                --length;
            }
            position += length;
            int finalLength = length;
            futures.add(executor.submit(() -> processSegment(buffer, finalLength)));
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        // Merge results into a single TreeMap<String, MeasurementAggregator>
        Map<String, MeasurementAggregator> aggregates = new TreeMap<>();
        for (Future<Map<String, MeasurementAggregator>> future : futures) {
            Map<String, MeasurementAggregator> segmentAggregates = future.get();
            for (Map.Entry<String, MeasurementAggregator> entry : segmentAggregates.entrySet()) {
                MeasurementAggregator aggregator = aggregates.computeIfAbsent(entry.getKey(), s -> new MeasurementAggregator());
                aggregator.min = Math.min(aggregator.min, entry.getValue().min);
                aggregator.max = Math.max(aggregator.max, entry.getValue().max);
                aggregator.sum += entry.getValue().sum;
                aggregator.count += entry.getValue().count;
            }
        }
        System.out.println(aggregates);
    }
}
