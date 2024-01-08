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

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.*;

public class CalculateAverage_dalogax {

    private static final int CHUNK_SIZE = 1024 * 1024 * 1000;
    private static final int QUEUE_CAPACITY = 10; // adjust as needed

    public static void main(String[] args) throws Exception {
        Map<String, DoubleSummaryStatistics> cityStatsMap = new HashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        Future<Void> readerFuture = executor.submit(() -> {
            try (RandomAccessFile file = new RandomAccessFile("./measurements.txt", "r")) {
                long position = 0;
                while (position < file.length()) {
                    byte[] chunk = new byte[CHUNK_SIZE];
                    file.seek(position);
                    int bytesRead = file.read(chunk);
                    if (bytesRead < CHUNK_SIZE && bytesRead != -1) {
                        queue.put(Arrays.copyOf(chunk, bytesRead));
                        break;
                    }
                    int backtrack = 0;
                    while (chunk[bytesRead - 1 - backtrack] != '\n') {
                        backtrack++;
                    }
                    queue.put(Arrays.copyOf(chunk, bytesRead - backtrack));
                    position += bytesRead - backtrack;
                }
            }
            return null;
        });

        List<Future<Void>> processorFutures = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() - 1; i++) {
            processorFutures.add(executor.submit(() -> {
                while (true) {
                    byte[] chunk = queue.take();
                    if (chunk.length == 0) {
                        break;
                    }
                    processChunk(chunk, cityStatsMap);
                }
                return null;
            }));
        }

        readerFuture.get();

        for (int i = 0; i < Runtime.getRuntime().availableProcessors() - 1; i++) {
            queue.put(new byte[0]);
        }

        for (Future<Void> future : processorFutures) {
            future.get();
        }

        executor.shutdown();

        StringJoiner result = new StringJoiner(", ", "{", "}");
        synchronized (cityStatsMap) {
            cityStatsMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> result.add(entry.getKey() + "="
                            + String.format("%.1f", entry.getValue().getMin()) + "/"
                            + String.format("%.1f", entry.getValue().getAverage()) + "/"
                            + String.format("%.1f", entry.getValue().getMax())));
        }
    
        System.out.println(result.toString());
        executor.shutdownNow();
        System.exit(0);
    }

    private static byte[] leftover;
    private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static Future<Void> processChunk(byte[] chunk, Map<String, DoubleSummaryStatistics> cityStatsMap) {
        return executor.submit(() -> {
            byte[] bytes;
            synchronized (CalculateAverage_dalogax.class) {
                bytes = Arrays.copyOf(chunk, chunk.length);
                if (leftover != null) {
                    byte[] combined = new byte[leftover.length + bytes.length];
                    System.arraycopy(leftover, 0, combined, 0, leftover.length);
                    System.arraycopy(bytes, 0, combined, leftover.length, bytes.length);
                    bytes = combined;
                    leftover = null;
                }
            }
            int start = 0;
            for (int end = 0; end < bytes.length; end++) {
                if (bytes[end] == '\n') {
                    String line = new String(bytes, start, end - start);
                    processLine(line, cityStatsMap);
                    start = end + 1;
                }
            }
            if (start < bytes.length) {
                leftover = new byte[bytes.length - start];
                System.arraycopy(bytes, start, leftover, 0, bytes.length - start);
            }
            return null;
        });
    }

    private static void processLine(String line, Map<String, DoubleSummaryStatistics> cityStatsMap) {
        int index = line.lastIndexOf(';');
        if (index != -1) {
            String city = line.substring(0, index);
            double value = Double.parseDouble(line.substring(index + 1));
            synchronized (cityStatsMap) {
                cityStatsMap.computeIfAbsent(city, k -> new DoubleSummaryStatistics())
                        .accept(value);
            }
        }
    }
}