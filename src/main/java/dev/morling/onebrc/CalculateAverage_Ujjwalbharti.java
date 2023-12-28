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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_Ujjwalbharti {

    private static final String FILE = "./measurements.txt";
    private static final List<Map<String, MeasurementAggregator>> results = new CopyOnWriteArrayList<>();

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
    }

    ;

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static class FileReaderCallable implements Callable<Void> {
        private final Path filePath;
        private final long startPos;
        private final long endPos;

        public FileReaderCallable(Path filePath, long startPos, long endPos) {
            this.filePath = filePath;
            this.startPos = startPos;
            this.endPos = endPos;
        }

        @Override
        public Void call() {

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                channel.position(startPos);
                ByteBuffer buffer = ByteBuffer.allocate((int) (endPos - startPos + 1));
                channel.read(buffer);
                String chunk = new String(buffer.array());
                String[] chunkLines = chunk.split("\n");

                Collector<Measurement, ?, MeasurementAggregator> collector = Collector.of(
                        MeasurementAggregator::new,
                        (a, m) -> {
                            a.min = Math.min(a.min, m.value());
                            a.max = Math.max(a.max, m.value());
                            a.sum += m.value();
                            a.count++;
                        },
                        (agg1, agg2) -> {
                            var res = new MeasurementAggregator();
                            res.min = Math.min(agg1.min, agg2.min);
                            res.max = Math.max(agg1.max, agg2.max);
                            res.sum = agg1.sum + agg2.sum;
                            res.count = agg1.count + agg2.count;

                            return res;
                        });

                Map<String, MeasurementAggregator> result = Arrays.stream(chunkLines)
                        .map(line -> new Measurement(line.split(";")))
                        .collect(groupingBy(Measurement::station, collector));

                results.add(result);

            }
            catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static long calculateEndPosition(Path path, long startPos, long chunkSize) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            if (startPos >= channel.size()) {
                return -1;
            }

            long currentPos = startPos + chunkSize;
            if (currentPos >= channel.size()) {
                currentPos = channel.size() - 1;
            }

            channel.position(currentPos);
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int readBytes = channel.read(buffer);

            if (readBytes > 0) {
                for (int i = 0; i < readBytes; i++) {
                    if (buffer.get(i) == '\n') {
                        break;
                    }
                    currentPos++;
                }
            }

            return currentPos;
        }
    }

    public static void main(String[] args) throws IOException {
        Path path = Paths.get(FILE);
        long fileSize = Files.size(path);
        long chunkSize = 1000 * 1000;
        int nThread = (int) (fileSize / chunkSize);
        var database = new HashMap<String, MeasurementAggregator>();
        try (ExecutorService customExecutor = Executors.newFixedThreadPool(275)) {
            var futures = new ArrayList<CompletableFuture<Void>>();
            long startPos = 0;
            for (int i = 0; i <= nThread; i++) {
                long endPos = calculateEndPosition(path, startPos, chunkSize);
                if (endPos == -1) {
                    break;
                }
                long finalStartPos = startPos;
                futures.add(CompletableFuture.runAsync(() -> new FileReaderCallable(path, finalStartPos, endPos).call(), customExecutor));
                startPos = endPos + 1;
            }
            CompletableFuture<Void> allOfFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allOfFuture.get();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }

        for (var map : results) {
            for (String key : map.keySet()) {
                if (database.containsKey(key)) {
                    MeasurementAggregator mag1 = database.get(key);
                    MeasurementAggregator mag2 = map.get(key);
                    mag1.min = Math.min(mag1.min, mag2.min);
                    mag1.max = Math.max(mag1.max, mag2.max);
                    mag1.sum = mag1.sum + mag2.sum;
                    mag1.count = mag1.count + mag2.count;
                    database.put(key, mag1);
                }
                else {
                    database.put(key, map.get(key));
                }
            }
        }

        var measurements = new TreeMap<String, ResultRow>();

        for (String key : database.keySet()) {
            MeasurementAggregator mag = database.get(key);
            measurements.put(key, new ResultRow(mag.min, mag.sum / mag.count, mag.max));
        }

        System.out.println(measurements);
    }
}
