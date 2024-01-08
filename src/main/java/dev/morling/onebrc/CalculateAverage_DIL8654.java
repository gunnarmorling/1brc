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
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_DIL8654 {
    private static final String FILE_PATH = "./measurements.txt"; // Path to the input file containing measurement data.
    private static final int MAX_STATION_NAME_LENGTH = 100; // Maximum allowed length for station names to ensure buffer size is sufficient.

    private static final int PROCESSING_FACTOR = 2;

    // Queue to hold results from each file partition processed concurrently.
    private static final ConcurrentLinkedQueue<Map<String, Measurement>> partitionResultsQueue = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // Record start time for performance measurement.

        File file = new File(FILE_PATH); // Load the file from the specified path.
        int processors = Runtime.getRuntime().availableProcessors(); // Detect number of available CPU cores for parallel processing.
        final List<FilePartition> partitions = partitionFile(file, processors * PROCESSING_FACTOR); // Split the file into partitions based on the number of processors.

        // Executor service for managing threads equal to the number of processors.
        ExecutorService executorService = Executors.newFixedThreadPool(processors * 2);
        for (FilePartition partition : partitions) {
            executorService.submit(() -> {
                try {
                    processPartition(partition); // Process each file partition in a separate thread.
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown(); // Initiate shutdown of the executor service.
        boolean finished = executorService.awaitTermination(10, TimeUnit.MINUTES); // Wait for tasks to finish or timeout after 10 minutes.

        // If all tasks are finished, merge and print results.
        if (finished) {
            Map<String, Measurement> results = mergeAllPartitionResults(); // Combine results from all partitions.
            long endTime = System.currentTimeMillis(); // Record end time.
            long duration = endTime - startTime; // Calculate total execution time.
            printResults(results); // Print the results in a sorted order.
            System.out.println("Execution completed in " + duration + " milliseconds");
        }
        else {
            System.out.println("Execution did not complete within the specified time");
        }
    }

    // Merges the results from all file partitions into a single map.
    private static Map<String, Measurement> mergeAllPartitionResults() {
        Map<String, Measurement> results = new HashMap<>();
        for (Map<String, Measurement> partitionResults : partitionResultsQueue) {
            for (Map.Entry<String, Measurement> entry : partitionResults.entrySet()) {
                // Combine measurements for each station using the merge function of Measurement class.
                results.merge(entry.getKey(), entry.getValue(), Measurement::merge);
            }
        }
        return results;
    }

    // Prints the results to the console.
    private static void printResults(Map<String, Measurement> results) {
        results.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()) // Sort results alphabetically by station name.
                .forEach(entry -> System.out.println(entry.getKey() + " = " + entry.getValue())); // Print each station's data.
    }

    // Processes a file partition, reading and aggregating data.
    private static void processPartition(FilePartition partition) throws IOException {
        Map<String, Measurement> partitionResults = new HashMap<>();
        try (FileChannel channel = new RandomAccessFile(new File(FILE_PATH), "r").getChannel()) {
            // Map a portion of the file into memory for efficient reading.
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, partition.start, partition.end - partition.start);
            StringBuilder sb = new StringBuilder(MAX_STATION_NAME_LENGTH); // StringBuilder to accumulate line characters.
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b == '\n') {
                    // At the end of each line, process the line and reset StringBuilder.
                    processLine(sb.toString(), partitionResults);
                    sb.setLength(0);
                }
                else {
                    sb.append((char) b); // Accumulate characters in StringBuilder.
                }
            }
            // Process any remaining data in StringBuilder after the last line.
            if (sb.length() > 0) {
                processLine(sb.toString(), partitionResults);
            }
        }
        partitionResultsQueue.add(partitionResults); // Add partition results to the queue.
    }

    // Processes a line from the file, extracting station name and temperature.
    private static void processLine(String line, Map<String, Measurement> partitionResults) {
        final String[] parts = line.split(";");
        if (parts.length == 2) {
            final String station = parts[0]; // Extract station name.
            double temperature = parseTemperature(parts[1]); // Parse temperature value.
            // Merge the new measurement with existing data for the station.
            partitionResults.merge(station, new Measurement(temperature), Measurement::updateTemperature);
        }
    }

    // Parses a temperature string to a double value.
    private static double parseTemperature(String tempStr) {
        boolean isNegative = tempStr.charAt(0) == '-'; // Check if the temperature is negative.
        int startIndex = isNegative ? 1 : 0; // Determine the starting index for parsing.
        int intValue = 0; // Integer part of the temperature.
        int decimalValue = 0; // Decimal part of the temperature.
        boolean decimalFound = false; // Flag to indicate if the decimal point is found.

        // Iterate through each character in the temperature string.
        for (int i = startIndex; i < tempStr.length(); i++) {
            char c = tempStr.charAt(i);
            if (c == '.') {
                decimalFound = true; // Set flag when decimal point is found.
                continue;
            }
            if (decimalFound) {
                // Parse the decimal part.
                decimalValue = c - '0';
                break;
            }
            else {
                // Parse the integer part.
                intValue = intValue * 10 + (c - '0');
            }
        }
        // Combine integer and decimal parts to form the temperature value.
        return (isNegative ? -1 : 1) * (intValue + decimalValue / 10.0);
    }

    static class Measurement {
        double min, max, sum;
        int count;

        // Constructor to create a new Measurement object from a temperature reading.
        public Measurement(double temperature) {
            this.min = this.max = this.sum = temperature; // Initialize min, max, and sum with the first temperature reading.
            this.count = 1; // Initialize count to 1.
        }

        // Merges this Measurement with another Measurement object.
        public Measurement merge(Measurement other) {
            this.min = Math.min(this.min, other.min); // Update the minimum temperature.
            this.max = Math.max(this.max, other.max); // Update the maximum temperature.
            this.sum += other.sum; // Update the total sum of temperatures.
            this.count += other.count; // Update the total count of measurements.
            return this;
        }

        // Static method to update temperature in a Measurement.
        public static Measurement updateTemperature(Measurement m1, Measurement m2) {
            m1.min = Math.min(m1.min, m2.min); // Update the minimum temperature.
            m1.max = Math.max(m1.max, m2.max); // Update the maximum temperature.
            m1.sum += m2.sum; // Update the total sum of temperatures.
            m1.count += m2.count; // Update the total count of measurements.
            return m1;
        }

        // Returns a string representation of the Measurement.
        @Override
        public String toString() {
            double mean = sum / count; // Calculate the average temperature.
            return String.format("%.1f/%.1f/%.1f", min, mean, max); // Return formatted string with min, average, and max temperatures.
        }
    }

    // Partitions the file into segments for concurrent processing.
    private static List<FilePartition> partitionFile(File file, int numberOfPartitions) throws IOException {
        long fileSize = file.length(); // Determine the total file size.
        long segmentSize = (fileSize + numberOfPartitions - 1) / numberOfPartitions; // Calculate the size of each partition.
        List<FilePartition> segments = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long start = 0;
            for (int i = 0; i < numberOfPartitions; i++) {
                // Determine the start and end of each partition.
                long end = (i == numberOfPartitions - 1) ? fileSize : Math.min(fileSize, start + segmentSize);
                segments.add(new FilePartition(start, end)); // Add the partition to the list.
                start = end;
            }
        }
        return segments;
    }

    static class FilePartition {
        long start, end;

        // Constructor for a file partition, specifying start and end positions in the file.
        FilePartition(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}
