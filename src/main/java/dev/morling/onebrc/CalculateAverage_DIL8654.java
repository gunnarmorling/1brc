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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_DIL8654 {

    private static final String FILE_PATH = "./measurements.txt"; // Path to your file
    private static final ConcurrentHashMap<String, Measurement> results = new ConcurrentHashMap<>();

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
        try (FileChannel channel = new RandomAccessFile(new File(FILE_PATH), "r").getChannel()) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, partition.start, partition.end - partition.start);
            StringBuilder sb = new StringBuilder();

            while (buffer.hasRemaining()) {
                char c = (char) buffer.get();
                if (c == '\n') {
                    processLine(sb.toString());
                    sb.setLength(0); // Clear the builder for the next line
                } else {
                    sb.append(c);
                }
            }

            // Process any remaining data if the last line doesn't end with a newline character
            if (sb.length() > 0) {
                processLine(sb.toString());
            }
        }
    }

    private static Measurement processLine(String line) {
       final String[] parts = line.split(";");
        if (parts.length == 2) {
            String station = parts[0];
            double temperature = Double.parseDouble(parts[1]);
            return results.compute(station, (key, currentMeasurement) -> {
                if (currentMeasurement == null) {
                    return new Measurement(temperature);
                } else {
                    return currentMeasurement.updateTemperature(temperature);
                }
            });
        }
        return null;
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
}
