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

import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Approach:
 * - Use memory-mapped file to speed up loading data into memory
 * - Partition data, compute aggregates for partitions in parallel, and finally combine results from all partitions
 * - Apply SIMD instructions for computing min/max/sum aggregates
 * - Use Shorts for storing aggregates of partitions, so we maximize the SIMD parallelism
 */
public class CalculateAverage_flippingbits {

    private static final String FILE = "./measurements.txt";

    private static final long CHUNK_SIZE = 100 * 1024 * 1024; // 100 MB

    private static final int SIMD_LANE_LENGTH = ShortVector.SPECIES_MAX.length();

    public static void main(String[] args) throws IOException {
        try (var file = new RandomAccessFile(FILE, "r")) {
            // Calculate chunk boundaries
            long[][] chunkBoundaries = getChunkBoundaries(file);
            // Process chunks
            var result = Arrays.asList(chunkBoundaries).stream()
                    .map(chunk -> {
                        try {
                            return processChunk(chunk[0], chunk[1]);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .parallel()
                    .reduce((firstMap, secondMap) -> {
                        for (var entry : secondMap.entrySet()) {
                            PartitionAggregate firstAggregate = firstMap.get(entry.getKey());
                            if (firstAggregate == null) {
                                firstMap.put(entry.getKey(), entry.getValue());
                            }
                            else {
                                firstAggregate.mergeWith(entry.getValue());
                            }
                        }
                        return firstMap;
                    })
                    .map(hashMap -> new TreeMap(hashMap)).get();

            System.out.println(result);
        }
    }

    private static long[][] getChunkBoundaries(RandomAccessFile file) throws IOException {
        var fileSize = file.length();
        // Split file into chunks, so we can work around the size limitation of channels
        var chunks = (int) (fileSize / CHUNK_SIZE);

        long[][] chunkBoundaries = new long[chunks + 1][2];
        var endPointer = 0L;

        for (var i = 0; i <= chunks; i++) {
            // Start of chunk
            chunkBoundaries[i][0] = Math.min(Math.max(endPointer, i * CHUNK_SIZE), fileSize);

            // Seek end of chunk, limited by the end of the file
            file.seek(Math.min(chunkBoundaries[i][0] + CHUNK_SIZE - 1, fileSize));

            // Extend chunk until end of line or file
            while (true) {
                var character = file.read();
                if (character == '\n' || character == -1) {
                    break;
                }
            }

            // End of chunk
            endPointer = file.getFilePointer();
            chunkBoundaries[i][1] = endPointer;
        }

        return chunkBoundaries;
    }

    private static Map<String, PartitionAggregate> processChunk(long startOfChunk, long endOfChunk)
            throws IOException {
        Map<String, PartitionAggregate> stationAggregates = new HashMap<>(10_000);
        byte[] byteChunk = new byte[(int) (endOfChunk - startOfChunk)];
        try (var file = new RandomAccessFile(FILE, "r")) {
            file.seek(startOfChunk);
            file.read(byteChunk);
            var i = 0;
            while (i < byteChunk.length) {
                final var startPosStation = i;

                // read station name
                while (byteChunk[i] != ';') {
                    i++;
                }
                var station = new String(Arrays.copyOfRange(byteChunk, startPosStation, i));
                i++;

                // read measurement
                final var startPosMeasurement = i;
                while (byteChunk[i] != '\n') {
                    i++;
                }

                var measurement = Arrays.copyOfRange(byteChunk, startPosMeasurement, i);
                var aggregate = stationAggregates.getOrDefault(station, new PartitionAggregate());
                aggregate.addMeasurementAndComputeAggregate(measurement);
                stationAggregates.put(station, aggregate);
                i++;
            }
            stationAggregates.values().forEach(PartitionAggregate::aggregateRemainingMeasurements);
        }

        return stationAggregates;
    }

    private static class PartitionAggregate {
        final short[] lane = new short[SIMD_LANE_LENGTH * 2];
        // Assume that we do not have more than Integer.MAX_VALUE measurements for the same station per partition
        int count = 0;
        long sum = 0;
        short min = Short.MAX_VALUE;
        short max = Short.MIN_VALUE;

        public void addMeasurementAndComputeAggregate(byte[] measurementBytes) {
            // Parse measurement and exploit that we know the format of the floating-point values
            var measurement = measurementBytes[measurementBytes.length - 1] - '0';
            var digits = 1;
            for (var i = measurementBytes.length - 3; i > 0; i--) {
                var num = measurementBytes[i] - '0';
                measurement = measurement + (num * (int) Math.pow(10, digits));
                digits++;
            }

            // Check if measurement is negative
            if (measurementBytes[0] == '-') {
                measurement = measurement * -1;
            }
            else {
                var num = measurementBytes[0] - '0';
                measurement = measurement + (num * (int) Math.pow(10, digits));
            }

            // Add measurement to buffer, which is later processed by SIMD instructions
            lane[count % lane.length] = (short) measurement;
            count++;

            // Once lane is full, use SIMD instructions to calculate aggregates
            if (count % lane.length == 0) {
                var firstVector = ShortVector.fromArray(ShortVector.SPECIES_MAX, lane, 0);
                var secondVector = ShortVector.fromArray(ShortVector.SPECIES_MAX, lane, SIMD_LANE_LENGTH);

                var simdMin = firstVector.min(secondVector).reduceLanes(VectorOperators.MIN);
                min = (short) Math.min(min, simdMin);

                var simdMax = firstVector.max(secondVector).reduceLanes(VectorOperators.MAX);
                max = (short) Math.max(max, simdMax);

                sum += firstVector.add(secondVector).reduceLanes(VectorOperators.ADD);
            }
        }

        public void aggregateRemainingMeasurements() {
            for (var i = 0; i < count % lane.length; i++) {
                var measurement = lane[i];
                min = (short) Math.min(min, measurement);
                max = (short) Math.max(max, measurement);
                sum += measurement;
            }
        }

        public void mergeWith(PartitionAggregate otherAggregate) {
            min = (short) Math.min(min, otherAggregate.min);
            max = (short) Math.max(max, otherAggregate.max);
            count = count + otherAggregate.count;
            sum = sum + otherAggregate.sum;
        }

        public String toString() {
            return String.format(
                    Locale.US,
                    "%.1f/%.1f/%.1f",
                    (min / 10.0),
                    (sum / 10.0) / count,
                    (max / 10.0));
        }
    }
}
