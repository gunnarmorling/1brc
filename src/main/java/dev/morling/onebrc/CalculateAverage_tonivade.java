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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CalculateAverage_tonivade {

    private static final String FILE = "./measurements.txt";

    private static record ResultRow(double min, double mean, double max) {

        @Override
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

        void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        ResultRow finish() {
            return new ResultRow(
                    this.min, (Math.round(this.sum * 10.0) / 10.0) / this.count, this.max);
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, Processor> map = new ConcurrentHashMap<>();

        Files.lines(Paths.get(FILE))
                .parallel()
                .forEach(line -> {
                    var index = line.indexOf(';');
                    map.computeIfAbsent(line.substring(0, index), Processor::new).add(line.substring(index + 1));
                });

        Map<String, ResultRow> measurements = map.values().stream()
                .collect(Collectors.toMap(Processor::getStation, Processor::finish, (a, b) -> {
                    throw new IllegalStateException();
                }, TreeMap::new));

        System.out.println(measurements);
    }

    static final class Processor implements Runnable {

        private final String station;
        private final Thread thread;

        private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);
        private final MeasurementAggregator aggregator = new MeasurementAggregator();
        private final AtomicBoolean stop = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();

        public Processor(String station) {
            this.station = station;
            this.thread = Thread.ofVirtual().name(station).start(this);
        }

        public String getStation() {
            return station;
        }

        public MeasurementAggregator getAggregator() {
            return aggregator;
        }

        public void add(String measurement) {
            try {
                queue.put(measurement);
            }
            catch (InterruptedException e) {
                // ignore
            }
        }

        @Override
        public void run() {
            while (!stop.get()) {
                try {
                    aggregator.add(Double.parseDouble(queue.take()));
                }
                catch (InterruptedException e) {
                    // ignore
                }
            }
            // drain queue
            queue.stream().map(Double::parseDouble).forEach(aggregator::add);
            finished.set(true);
        }

        public ResultRow finish() {
            stop.set(true);
            thread.interrupt();
            while (!finished.get()) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    // ignore
                }
            }
            return aggregator.finish();
        }
    }
}
