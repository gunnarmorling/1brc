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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class CalculateAverage_DIL8654 {

    private static final String FILE_PATH = "./measurements.txt";
    private static final int MAX_STATION_NAME_LENGTH = 100;
    private static final ConcurrentLinkedQueue<Map<String, Measurement>> partitionResultsQueue = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        File file = new File(FILE_PATH);
        int processors = Runtime.getRuntime().availableProcessors();
        final List<FilePartition> partitions = partitionFile(file, processors);

        ExecutorService executorService = Executors.newFixedThreadPool(processors);
        for (FilePartition partition : partitions) {
            executorService.submit(() -> {
                try {
                    processPartition(partition);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown();
        boolean finished = executorService.awaitTermination(10, TimeUnit.MINUTES);

        if (finished) {
            Map<String, Measurement> results = mergeAllPartitionResults();
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            printResults(results);
            System.out.println("Execution completed in " + duration + " milliseconds");
        }
        else {
            System.out.println("Execution did not complete within the specified time");
        }
    }

    private static Map<String, Measurement> mergeAllPartitionResults() {
        Map<String, Measurement> results = new HashMap<>();
        for (Map<String, Measurement> partitionResults : partitionResultsQueue) {
            for (Map.Entry<String, Measurement> entry : partitionResults.entrySet()) {
                results.merge(entry.getKey(), entry.getValue(), Measurement::merge);
            }
        }
        return results;
    }

    private static void printResults(Map<String, Measurement> results) {
        results.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> System.out.println(entry.getKey() + " = " + entry.getValue()));
    }

    private static void processPartition(FilePartition partition) throws IOException {
        Map<String, Measurement> partitionResults = new HashMap<>();
        try (FileChannel channel = new RandomAccessFile(new File(FILE_PATH), "r").getChannel()) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, partition.start, partition.end - partition.start);
            StringBuilder sb = new StringBuilder(MAX_STATION_NAME_LENGTH);
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b == '\n') {
                    processLine(sb.toString(), partitionResults);
                    sb.setLength(0);
                }
                else {
                    sb.append((char) b);
                }
            }
            if (sb.length() > 0) {
                processLine(sb.toString(), partitionResults);
            }
        }
        partitionResultsQueue.add(partitionResults);
    }

    private static void processLine(String line, Map<String, Measurement> partitionResults) {
        final String[] parts = line.split(";");
        if (parts.length == 2) {
            String station = parts[0];
            double temperature = parseTemperature(parts[1]);
            partitionResults.merge(station, new Measurement(temperature), Measurement::updateTemperature);
        }
    }

    private static double parseTemperature(String tempStr) {
        boolean isNegative = tempStr.charAt(0) == '-';
        int startIndex = isNegative ? 1 : 0;
        int intValue = 0;
        int decimalValue = 0;
        boolean decimalFound = false;
        for (int i = startIndex; i < tempStr.length(); i++) {
            char c = tempStr.charAt(i);
            if (c == '.') {
                decimalFound = true;
                continue;
            }
            if (decimalFound) {
                decimalValue = c - '0';
                break;
            }
            else {
                intValue = intValue * 10 + (c - '0');
            }
        }
        return (isNegative ? -1 : 1) * (intValue + decimalValue / 10.0);
    }

    static class Measurement {
        double min, max, sum;
        int count;

        public Measurement(double temperature) {
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        public Measurement merge(Measurement other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        // Static method to merge two Measurement objects
        public static Measurement updateTemperature(Measurement m1, Measurement m2) {
            m1.min = Math.min(m1.min, m2.min);
            m1.max = Math.max(m1.max, m2.max);
            m1.sum += m2.sum;
            m1.count += m2.count;
            return m1;
        }

        @Override
        public String toString() {
            double mean = sum / count;
            return String.format("%.1f/%.1f/%.1f", min, mean, max);
        }
    }

    private static List<FilePartition> partitionFile(File file, int numberOfPartitions) throws IOException {
        long fileSize = file.length();
        long segmentSize = (fileSize + numberOfPartitions - 1) / numberOfPartitions;
        List<FilePartition> segments = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long start = 0;
            for (int i = 0; i < numberOfPartitions; i++) {
                long end = (i == numberOfPartitions - 1) ? fileSize : Math.min(fileSize, start + segmentSize);
                segments.add(new FilePartition(start, end));
                start = end;
            }
        }
        return segments;
    }

    static class FilePartition {
        long start, end;

        FilePartition(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}
