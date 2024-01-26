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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Changelog:
 * <ol>
 *      <li> 1/15/24 - initial minor changes with syntax. 100k lines - 209ms</li>
 *      <li> 1/15/24 - new algorithm without streams, new splitting method, use of BufferedReader. https://stackoverflow.com/questions/11001330/java-split-string-performances
 *          100k lines - 154ms,
 *          1m lines - 578ms</li>
 *      <li> 1/16/24 - changed to single linear pass, use indexOf instead of string tokenizer, other changes.
 *      https://stackoverflow.com/questions/13997361/string-substring-vs-string-split
 *          1m lines - 544ms</li>
 *      <li> 1/25/24 - make parseRow() functionality inline. No noticeable improvement.</li>
 *      <li> 1/26/24 - rewrite to use parallel streams to read/process rows into memory and then use multiple
 *      threads to aggregate results.
 *          1m lines - 417ms</li>
 * </ol>
 */
public class CalculateAverage_spiderpig86 {

    private static final String FILE = "./measurements2.txt";

    private record Measurement(String station, double value) { }

    private static class ResultRow {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double total = 0.0;
        private long count = 0;

        public void processValue(double val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            total += val;
            ++count;
        }

        public ResultRow merge(ResultRow other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            total += other.total;
            count += other.count;
            return this;
        }

        public double getMean() {
            return total / count;
        }

        public String toString() {
            return round(min) + "/" + round(getMean()) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    public static void main(String[] args) throws IOException {
        // TODO Remove
        long start = System.currentTimeMillis();

        // Read file in parallel
        List<Measurement> measurements = Files.readAllLines(Path.of(FILE))
                .stream().parallel()
                        .map(line -> {
                            // Substring is faster than split by a long shot
                            // https://stackoverflow.com/questions/13997361/string-substring-vs-string-split#:~:text=When%20you%20run%20this%20multiple,would%20be%20even%20more%20drastic.
                            int delimiterIndex = line.indexOf(';');
                            String station = line.substring(0, delimiterIndex);
                            double value = Double.parseDouble(line.substring(delimiterIndex + 1));
                            return new Measurement(station, value);
                        }).toList();

        int threads = Runtime.getRuntime().availableProcessors();
        try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            Map<String, ResultRow> aggregated = new ConcurrentHashMap<>();
            int chunkSize = measurements.size() / threads;

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                futures.add(CompletableFuture
                        .supplyAsync(() -> processChunk(measurements, finalI * chunkSize,
                                Math.min((finalI+1) * chunkSize, measurements.size())), executorService)
                        .thenAccept(resultMap -> {
                            resultMap.forEach((station, resultRow) -> aggregated.merge(station, resultRow, ResultRow::merge));
                        }));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .join();
            System.out.println(new TreeMap<>(aggregated));
        }

        // TODO Remove
         System.out.println("Elapsed time ms: " + (System.currentTimeMillis() - start));
    }

    private static Map<String, ResultRow> processChunk(List<Measurement> measurements, int start, int end) {
        final Map<String, ResultRow> results = new HashMap<>();
        for (int i = start; i < end; i++) {
            Measurement m = measurements.get(i);
            if (!results.containsKey(m.station)) {
                results.put(m.station, new ResultRow());
            }
            results.get(m.station).processValue(m.value);
        }
        return results;
    }
}