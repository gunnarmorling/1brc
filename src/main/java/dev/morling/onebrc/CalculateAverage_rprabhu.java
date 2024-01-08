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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_rprabhu {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static ConcurrentHashMap<String, MeasurementAggregator> map = new ConcurrentHashMap<>();

    private static final int TASK_CHUNK = 50000;

    private static class TaskExecutor implements Runnable {
        List<String> list;

        TaskExecutor(List<String> list) {
            this.list = list;
        }

        @Override
        public void run() {
            for (String str : list) {
                // System.out.println(str);
                // String[] values = str.split(";");
                int index = str.indexOf(';');
                String station = str.substring(0, index);
                double val = Double.parseDouble(str.substring(index + 1));
                // double val = Double.parseDouble(values[1]);
                MeasurementAggregator aggr = map.getOrDefault(station, new MeasurementAggregator());
                aggr.count += 1;
                aggr.sum += val;
                aggr.max = (val >= aggr.max) ? val : aggr.max; // Math.max(aggr.max, val);
                aggr.min = (val <= aggr.min) ? val : aggr.min; // Math.min(aggr.min, val);
                map.put(station, aggr);
            }

        }

    }

    private static class TaskScheduler {
        ExecutorService executor;
        List<String> list = new ArrayList<>(TASK_CHUNK);

        TaskScheduler(ExecutorService executor) {
            this.executor = executor;
        }

        void push(String line) {
            // System.out.println("adding: " + line);
            list.add(line);
            if (list.size() >= TASK_CHUNK) {
                executor.submit(new TaskExecutor(list));
                list = new ArrayList<>(TASK_CHUNK);
            }
        }

        void completeRemaining() {
            // System.out.println("Completing remaining: " + list.size());
            if (!list.isEmpty()) {
                // System.out.println("Scheduling remaining");
                executor.submit(new TaskExecutor(list));
            }
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        // ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        // ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        ExecutorService executor = Executors.newCachedThreadPool();
        TaskScheduler scheduler = new TaskScheduler(executor);
        Files.lines(Paths.get(FILE), StandardCharsets.UTF_8).forEach(line -> {
            scheduler.push(line);
        });
        scheduler.completeRemaining();
        executor.shutdown();
        executor.awaitTermination(600, TimeUnit.SECONDS);

        Map<String, ResultRow> sortedResult = new TreeMap<>();
        map.forEach(
                (key, value) -> sortedResult.put(key, new ResultRow(value.min, value.sum / value.count, value.max)));
        System.out.println(sortedResult);

    }
}
