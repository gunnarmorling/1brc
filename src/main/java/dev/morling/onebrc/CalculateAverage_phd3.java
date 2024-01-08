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

import static java.nio.charset.StandardCharsets.*;
import static java.util.stream.Collectors.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class CalculateAverage_phd3 {

    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final String FILE = "./measurements.txt";
    private static final long FILE_SIZE = new File(FILE).length();
    private static final int CHUNK_SIZE = 65536 * 1024;
    private static final int PADDING = 512;
    private static final double[] POWERS_OF_10 = IntStream.range(0, 6).mapToDouble(x -> Math.pow(10.0, x)).toArray();

    private static final Map<String, AggregationInfo> globalMap = new ConcurrentHashMap<>();

    private record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    public static ResultRow resultRow(AggregationInfo aggregationInfo) {
        return new ResultRow(aggregationInfo.min, aggregationInfo.sum / aggregationInfo.count, aggregationInfo.max);
    }

    public static void main(String[] args) throws Exception {
        long fileLength = new File(FILE).length();
        int numChunks = (int) Math.ceil(fileLength * 1.0 / CHUNK_SIZE);
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        BufferDataProvider provider = new RandomAccessBasedProvider(FILE, FILE_SIZE);
        for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            executorService.submit(new Aggregator(chunkIndex, provider));
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);

        Map<String, ResultRow> measurements = new TreeMap<>(globalMap.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> resultRow(e.getValue()))));

        System.out.println(measurements);
    }

    private static class AggregationInfo {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum;
        long count;

        public AggregationInfo update(AggregationInfo update) {
            this.count += update.count;
            this.sum += update.sum;
            if (this.max < update.max) {
                this.max = update.max;
            }
            if (this.min > update.min) {
                this.min = update.min;
            }
            return this;
        }

        public AggregationInfo update(double value) {
            this.count++;
            this.sum += value;
            if (this.max < value) {
                this.max = value;
            }
            if (this.min > value) {
                this.min = value;
            }
            return this;
        }
    }

    private interface BufferDataProvider {
        int read(byte[] buffer, long offset) throws Exception;
    }

    private static class RandomAccessBasedProvider implements BufferDataProvider {
        private final String filePath;
        private final long fileSize;

        RandomAccessBasedProvider(String filePath, long fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        @Override
        public int read(byte[] buffer, long offset) throws Exception {
            RandomAccessFile file = null;
            try {
                file = new RandomAccessFile(filePath, "r");
                file.seek(offset);
                return file.read(buffer);
            }
            finally {
                if (file != null) {
                    file.close();
                }
            }
        }
    }

    private static class Aggregator implements Runnable {
        private final long startByte;
        private final BufferDataProvider dataProvider;

        public Aggregator(long chunkIndex, BufferDataProvider dataProvider) {
            this.startByte = chunkIndex * CHUNK_SIZE;
            this.dataProvider = dataProvider;
        }

        @Override
        public void run() {
            try {
                // offset for the last byte to be processed (excluded)
                long endByte = Math.min(startByte + CHUNK_SIZE, FILE_SIZE);
                // read a little more than needed to cover next entry if needed
                long bufferSize = endByte - startByte + ((endByte == FILE_SIZE) ? 0 : PADDING);
                byte[] buffer = new byte[(int) bufferSize];
                int bytes = dataProvider.read(buffer, startByte);
                // Partial aggregation to avoid accessing global concurrent map for every entry
                Map<String, AggregationInfo> updated = processBuffer(
                        buffer, startByte == 0, endByte - startByte);
                // Full aggregation with global map
                updated.entrySet().forEach(entry -> {
                    globalMap.compute(entry.getKey(), (k, v) -> {
                        if (v == null) {
                            return entry.getValue();
                        }
                        return v.update(entry.getValue());
                    });
                });
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        private static Map<String, AggregationInfo> processBuffer(byte[] buffer, boolean isFileStart, long nextChunkStart) {
            int start = 0;
            // Move to the next entry after '\n'. Don't do this if we're at the start of
            // the file to avoid missing first entry.
            if (!isFileStart) {
                while (buffer[start] != '\n') {
                    start++;
                }
                start += 1;
            }

            // local map for this thread, don't need thread safety
            Map<String, AggregationInfo> chunkMap = new HashMap<>();
            while (true) {
                LineInfo lineInfo = getNextLine(buffer, start);
                String key = new String(buffer, start, lineInfo.semicolonIndex - start);
                double value = parseDouble(buffer, lineInfo.semicolonIndex + 1, lineInfo.nextStart - 1);
                update(chunkMap, key, value);

                if ((lineInfo.nextStart > nextChunkStart) || (lineInfo.nextStart >= buffer.length)) {
                    // we are already at a point where the next line will be processed in the next chunk,
                    // so the job is done here
                    break;
                }

                start = lineInfo.nextStart();
            }
            return chunkMap;
        }

        private static double parseDouble(byte[] bytes, int offset, int end) {
            boolean negative = (bytes[offset] == '-');
            int current = negative ? offset + 1 : offset;
            int preFloat = 0;
            while (current < end && bytes[current] != '.') {
                preFloat = (preFloat * 10) + (bytes[current++] - '0');
            }
            current++;
            int postFloatStart = current;
            int postFloat = 0;
            while (current < end) {
                postFloat = (postFloat * 10) + (bytes[current++] - '0');
            }

            return (preFloat + ((postFloat) / POWERS_OF_10[end - postFloatStart])) * (negative ? -1 : 1);
        }

        private static void update(Map<String, AggregationInfo> state, String key, double value) {
            AggregationInfo info = state.computeIfAbsent(key, k -> new AggregationInfo());
            info.update(value);
        }

        // identifies indexes of the next ';' and '\n', which will be used to get entry key and value from line
        private static LineInfo getNextLine(byte[] buffer, int start) {
            // caller guarantees that the access is in bounds, so no index check
            while (buffer[start] != ';') {
                start++;
            }
            int semicolonIndex = start;
            // caller guarantees that the access is in bounds, so no index check
            while (buffer[start] != '\n') {
                start++;
            }
            return new LineInfo(semicolonIndex, start + 1);
        }
    }

    private record LineInfo(int semicolonIndex, int nextStart) {
    }
}
