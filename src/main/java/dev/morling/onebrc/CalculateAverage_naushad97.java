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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

public class CalculateAverage_naushad97 {

    private static final String FILE = "./measurements.txt";
    private static final String REGEX_LINE_SPLIT = ";";
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {
        // Parallel processing with custom ThreadPool and streams API parallelism
        // Also using in-built DoubleSummaryStatistics container to calculate and hold max, min, average
        Map<String, DoubleSummaryStatistics> concurrentStations = new ConcurrentHashMap<>();

        // try with resource
        try (ForkJoinPool forkJoinPool = new ForkJoinPool(THREAD_COUNT)) {
            forkJoinPool.submit(() -> {
                doReadInParallel(concurrentStations);
            });
        }

        System.out.println(new TreeMap<>(concurrentStations));
    }

    private static void doReadInParallel(Map<String, DoubleSummaryStatistics> concurrentStations) {
        try (Stream<String> dataStreams = Files.lines(Paths.get(FILE)).parallel()) {
            dataStreams.forEach(line -> {
                // Expected data format => Monterrey;34.0
                String[] parts = line.split(REGEX_LINE_SPLIT);
                concurrentStations.computeIfAbsent(parts[0],
                        k -> getDoubleSummaryStatistics()).accept(Double.parseDouble(parts[1]));
            });
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DoubleSummaryStatistics getDoubleSummaryStatistics() {
        return new DoubleSummaryStatistics() {
            // this will get called for each line processing, hence synchronized
            public synchronized void accept(double value) {
                super.accept(value);
            }

            // should only get called when printing
            public String toString() {
                return String.format("%.1f/%.1f/%.1f", getMin(), getAverage(), getMax());
            }
        };
    }

    // TODO alternative approach to read data in chunks
    static void parallelProcessing() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(FILE))) {
            long fileSize = fileChannel.size();
        }
    }
}
