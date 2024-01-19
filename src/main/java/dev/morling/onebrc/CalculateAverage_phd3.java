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

import static java.util.stream.Collectors.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_phd3 {

    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final String FILE = "./measurements.txt";
    private static final long FILE_SIZE = new File(FILE).length();
    // A chunk is a unit for processing, the file will be divided in chunks of the following size
    private static final int CHUNK_SIZE = 65536 * 1024;
    // Read a little more data into the buffer to finish processing current line
    private static final int PADDING = 512;
    // Minor : Precompute powers to avoid recalculating while parsing doubles (temperatures)
    private static final double[] POWERS_OF_10 = IntStream.range(0, 6).mapToDouble(x -> Math.pow(10.0, x)).toArray();

    /**
     * A Utility to print aggregated information in the desired format
     */
    private record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    public static ResultRow resultRow(AggregationInfo aggregationInfo) {
        return new ResultRow(aggregationInfo.min, (Math.round(aggregationInfo.sum * 10.0) / 10.0) / (aggregationInfo.count), aggregationInfo.max);
    }

    public static void main(String[] args) throws Exception {
        long fileLength = new File(FILE).length();
        int numChunks = (int) Math.ceil(fileLength * 1.0 / CHUNK_SIZE);
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        BufferDataProvider provider = new RandomAccessBasedProvider(FILE, FILE_SIZE);
        List<Future<LinearProbingHashMap>> futures = new ArrayList<>();
        // Process chunks in parallel
        for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            futures.add(executorService.submit(new Aggregator(chunkIndex, provider)));
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);

        Map<String, AggregationInfo> info = futures.stream().map(f -> {
            try {
                return f.get();
            }
            catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        })
                .map(LinearProbingHashMap::toMap)
                .flatMap(map -> map.entrySet().stream())
                .sequential()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, AggregationInfo::update));

        Map<String, ResultRow> measurements = new TreeMap<>(info.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> resultRow(e.getValue()))));

        System.out.println(measurements);
    }

    /**
     * Stores required running aggregation information to be able to compute min/max/average at the end
     */
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

    /**
     * uses RandomAccessFile seek and read APIs to load data into a buffer.
     */
    private static class RandomAccessBasedProvider implements BufferDataProvider {
        private final String filePath;

        RandomAccessBasedProvider(String filePath, long fileSize) {
            this.filePath = filePath;
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

    /**
     * Task to processes a chunk of file and return a custom linear probing hashmap for performance
     */
    private static class Aggregator implements Callable<LinearProbingHashMap> {
        private final long startByte;
        private final BufferDataProvider dataProvider;

        public Aggregator(long chunkIndex, BufferDataProvider dataProvider) {
            this.startByte = chunkIndex * CHUNK_SIZE;
            this.dataProvider = dataProvider;
        }

        @Override
        public LinearProbingHashMap call() {
            try {
                // offset for the last byte to be processed (excluded)
                long endByte = Math.min(startByte + CHUNK_SIZE, FILE_SIZE);
                // read a little more than needed to cover next entry if needed
                long bufferSize = endByte - startByte + ((endByte == FILE_SIZE) ? 0 : PADDING);
                byte[] buffer = new byte[(int) bufferSize];
                int bytes = dataProvider.read(buffer, startByte);
                // Partial aggregation in a hashmap
                return processBuffer(buffer, startByte == 0, endByte - startByte);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        private static LinearProbingHashMap processBuffer(byte[] buffer, boolean isFileStart, long nextChunkStart) {
            int start = 0;
            // Move to the next entry after '\n'. Don't do this if we're at the start of
            // the file to avoid missing first entry.
            if (!isFileStart) {
                while (buffer[start] != '\n') {
                    start++;
                }
                start += 1;
            }

            LinearProbingHashMap chunkLocalMap = new LinearProbingHashMap();
            while (true) {
                LineInfo lineInfo = getNextLine(buffer, start);
                byte[] keyBytes = new byte[lineInfo.semicolonIndex - start];
                System.arraycopy(buffer, start, keyBytes, 0, keyBytes.length);
                double value = parseDouble(buffer, lineInfo.semicolonIndex + 1, lineInfo.nextStart - 1);
                // Update aggregated value for the given key with the new line
                AggregationInfo info = chunkLocalMap.get(keyBytes, lineInfo.keyHash);
                info.update(value);

                if ((lineInfo.nextStart > nextChunkStart) || (lineInfo.nextStart >= buffer.length)) {
                    // we are already at a point where the next line will be processed in the next chunk,
                    // so the job is done here
                    break;
                }

                start = lineInfo.nextStart();
            }
            return chunkLocalMap;
        }

        /**
         * Converts bytes to double value without intermediate string conversion, faster than Double.parseDouble.
         */
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

        /**
         * Identifies indexes of the next ';' and '\n', which will be used to get entry key and value from line. Also
         * computes the hash value for the key while iterating.
         */
        private static LineInfo getNextLine(byte[] buffer, int start) {
            // caller guarantees that the access is in bounds, so no index check
            int hash = 0;
            while (buffer[start] != ';') {
                start++;
                hash = hash * 31 + buffer[start];
            }
            // The following is just to further reduce the probability of collisions
            hash = hash ^ (hash << 16);
            int semicolonIndex = start;
            // caller guarantees that the access is in bounds, so no index check
            while (buffer[start] != '\n') {
                start++;
            }
            return new LineInfo(semicolonIndex, start + 1, hash);
        }
    }

    private record LineInfo(int semicolonIndex, int nextStart, int keyHash) {
    }

    /**
     * A simple map with pre-configured fixed bucket count. With 2^13 buckets and current hash function, seeing 4
     * collisions which is not too bad. Every bucket is implemented with a linked list. The map is NOT thread safe.
     */
    private static class LinearProbingHashMap {
        private final static int BUCKET_COUNT = 8191;
        private final Node[] buckets;

        LinearProbingHashMap() {
            this.buckets = new Node[BUCKET_COUNT];
        }

        /**
         * Given a key, returns the current value of AggregationInfo. If not present, creates a new empty node at the
         * front of the bucket
         */
        public AggregationInfo get(byte[] key, int keyHash) {
            // find bucket index through bitwise AND, works for bucketCount = (2^p - 1)
            int bucketIndex = BUCKET_COUNT & keyHash;
            Node current = buckets[bucketIndex];
            while (current != null) {
                if (Arrays.equals(current.entry.key(), key)) {
                    return current.entry.aggregationInfo();
                }
                current = current.next;
            }

            // Entry does not exist, so add a new node in the linked list
            AggregationInfo newInfo = new AggregationInfo();
            KeyValuePair pair = new KeyValuePair(key, keyHash, newInfo);
            Node newNode = new Node(pair, buckets[bucketIndex]);
            buckets[bucketIndex] = newNode;
            return newNode.entry.aggregationInfo();
        }

        /**
         * A helper to convert to Java's hash map to build the final aggregation after partial aggregations
         */
        private Map<String, AggregationInfo> toMap() {
            Map<String, AggregationInfo> map = new HashMap<>();
            for (Node bucket : buckets) {
                while (bucket != null) {
                    map.put(new String(bucket.entry.key, StandardCharsets.UTF_8), bucket.entry.aggregationInfo());
                    bucket = bucket.next;
                }
            }
            return map;
        }
    }

    /**
     * Linked List node to implement a bucket of custom hash map
     */
    private static class Node {
        KeyValuePair entry;
        Node next;

        public Node(KeyValuePair entry, Node next) {
            this.entry = entry;
            this.next = next;
        }
    }

    /**
     * a wrapper class to store information needed for storing a measurement information in the hashmap
     */
    private record KeyValuePair(byte[] key, int keyHash, AggregationInfo aggregationInfo) {
    }
}
