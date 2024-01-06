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

    private static final String FILE_PATH = "./measurements.txt"; // Path to your file
    private static final int MAX_STATION_NAME_LENGTH = 100;
    private static final ConcurrentHashMap<String, Measurement> results = new ConcurrentHashMap<>(10000);
    private static ThreadLocal<Map<String, Measurement>> localResults = ThreadLocal.withInitial(HashMap::new);
    private static ConcurrentLinkedQueue<Map<String, Measurement>> partitionResultsQueue = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // Start time measurement

        File file = new File(FILE_PATH);
        int processors = Runtime.getRuntime().availableProcessors();
        System.out.println("Number of processors: " + processors);
        final List<FilePartition> partitions = partitionFile(file, processors);

        ExecutorService executorService = Executors.newFixedThreadPool(processors);
        System.out.println("Number of partitions: " + partitions.size());
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

        final long endTime = System.currentTimeMillis(); // End time measurement
        final long duration = endTime - startTime; // Compute the time difference

        if (finished) {
            mergeAllPartitionResults();
            System.out.println("Execution completed in " + duration + " milliseconds");
            printResults();
        }
        else {
            System.out.println("Execution did not complete within the specified time");
        }
    }

    private static void printResults() {
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

    private static void mergeAllPartitionResults() {
        for (Map<String, Measurement> partitionResults : partitionResultsQueue) {
            for (Map.Entry<String, Measurement> entry : partitionResults.entrySet()) {
                results.merge(entry.getKey(), entry.getValue(), Measurement::merge);
            }
        }
    }

    private static void processLine(String line, Map<String, Measurement> partitionResults) {
        final String[] parts = line.split(";");
        if (parts.length == 2) {
            String station = parts[0];
            double temperature = Double.parseDouble(parts[1]);
            partitionResults.merge(station, new Measurement(temperature), (currentMeasurement, newMeasurement) -> {
                return currentMeasurement.updateTemperature(temperature);
            });
        }
    }

    // Define the Measurement class with appropriate methods for aggregation
    // This class should be thread-safe if necessary
    static class Measurement {
        double min, max, sum;
        int count;

        // Constructor for the Measurement class
        // Constructor for a new Measurement with a single temperature value
        public Measurement(double temperature) {
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        // Method to update the measurement with a new temperature
        public Measurement updateTemperature(double temperature) {
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
            sum += temperature;
            count++;
            return this;
        }

        public Measurement merge(Measurement other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        // Method to calculate mean and format output
        @Override
        public String toString() {
            double mean = sum / count;
            return String.format("%.1f/%.1f/%.1f", min, mean, max);
        }
    }

    private static List<FilePartition> partitionFile(File file, int numberOfPartitions) throws IOException {
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfPartitions;
        List<FilePartition> segments = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long start = 0;
            for (int i = 0; i < numberOfPartitions; i++) {
                long end = (i == numberOfPartitions - 1) ? fileSize : findNextLineBreak(raf, start + segmentSize);
                segments.add(new FilePartition(start, end));
                start = end;
            }
        }

        return segments;
    }

    private static long findNextLineBreak(RandomAccessFile raf, long start) throws IOException {
        raf.seek(start);
        while (raf.getFilePointer() < raf.length()) {
            if (raf.readByte() == '\n') {
                return raf.getFilePointer();
            }
        }
        return raf.length();
    }

    static class FilePartition {
        long start, end;

        FilePartition(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    private static double parseTemperature(String tempStr) {
        // Custom parsing logic for the temperature string
        // This can be optimized knowing the format is always one digit after the decimal
        return Double.parseDouble(tempStr); // Replace with a more efficient parsing if necessary
    }
}
