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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class CalculateAverage_muditsaxena {

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

    static class TaskRunner<V> implements Callable<V> {

        List<String> inputList;
        String input;

        TaskRunner(List<String> taskList) {
            this.inputList = taskList;
        }

        TaskRunner(String input) {
            this.input = input;
        }

        String[] readInput(String inputTask) {
            StringBuilder stationName = new StringBuilder();
            String[] values = new String[2];
            int index = 0;
            char ch = inputTask.charAt(index);
            while (ch != ';') {
                stationName.append(ch);
                ch = inputTask.charAt(++index);
            }
            index++;

            values[0] = stationName.toString();
            values[1] = inputTask.substring(index);

            return values;
        }

        @Override
        public V call() {
            for (String inputTask : inputList) {

                if (inputTask.isEmpty()) {
                    continue;
                }

                String[] values = readInput(inputTask);
                double value = Double.parseDouble(values[1]);
                MeasurementAggregator measurementAggregator = map.getOrDefault(values[0], new MeasurementAggregator());
                measurementAggregator.count += 1;
                measurementAggregator.sum += value;
                measurementAggregator.max = Math.max(measurementAggregator.max, value);
                measurementAggregator.min = Math.min(measurementAggregator.min, value);
                map.put(values[0], measurementAggregator);
            }
            inputList = null;
            return null;
        }
    }

    static ConcurrentMap<String, MeasurementAggregator> map = new ConcurrentHashMap<>();
    static final int TASK_LIST_CAPACITY = 10000;
    // 100000 - 1:23
    // 10000 - 1:10
    // 1000 - 1:21

    // Optimising split
    // 10000 - 1:15, 1:12, 1:11
    // 1000 - 1:18, 1:23

    public static void main(String[] args) throws IOException {
        try (ExecutorService virtualThreadExecutors = Executors.newVirtualThreadPerTaskExecutor()) {

            List<String> taskList = new ArrayList<>(TASK_LIST_CAPACITY);
            List<CompletableFuture<Void>> tasks = new ArrayList<>();

            try (BufferedReader br = Files.newBufferedReader(Paths.get(FILE))) {
                String line = br.readLine();
                while (line != null) {
                    taskList.add(line);
                    if (taskList.size() >= TASK_LIST_CAPACITY) {
                        tasks.add(CompletableFuture.runAsync(new FutureTask<>(new TaskRunner<>(taskList)), virtualThreadExecutors));
                        taskList = null;
                        taskList = new ArrayList<>(TASK_LIST_CAPACITY);
                    }
                    line = br.readLine();
                }
            }

            if (!taskList.isEmpty()) {
                tasks.add(CompletableFuture.runAsync(new FutureTask<>(new TaskRunner<>(taskList)), virtualThreadExecutors));
            }

            for (CompletableFuture<Void> task : tasks) {
                if (task != null) {
                    task.join();
                }
            }

            Map<String, ResultRow> resultRowMap = new TreeMap<>();
            map.forEach((key, value) -> resultRowMap.put(key, new ResultRow(value.min, value.sum / value.count, value.max)));
            System.out.println(resultRowMap);
        }
    }
}
