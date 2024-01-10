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
import java.nio.charset.StandardCharsets;
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

    private static final long CHUNK_SIZE = 10 * 1024 * 1024; // 10 MB

    private static final int SIMD_LANE_LENGTH = ShortVector.SPECIES_MAX.length();

    private static final int MAX_STATION_NAME_LENGTH = 100;

    public static void main(String[] args) throws IOException {
        var result = Arrays.asList(getSegments()).stream()
                .map(segment -> {
                    try {
                        return processSegment(segment[0], segment[1]);
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
                .map(TreeMap::new).get();

        System.out.println(result);
    }

    private static long[][] getSegments() throws IOException {
        try (var file = new RandomAccessFile(FILE, "r")) {
            var fileSize = file.length();
            // Split file into segments, so we can work around the size limitation of channels
            var numSegments = (int) (fileSize / CHUNK_SIZE);

            var boundaries = new long[numSegments + 1][2];
            var endPointer = 0L;

            for (var i = 0; i < numSegments; i++) {
                // Start of segment
                boundaries[i][0] = Math.min(Math.max(endPointer, i * CHUNK_SIZE), fileSize);

                // Seek end of segment, limited by the end of the file
                file.seek(Math.min(boundaries[i][0] + CHUNK_SIZE - 1, fileSize));

                // Extend segment until end of line or file
                while (file.read() != '\n') {
                }

                // End of segment
                endPointer = file.getFilePointer();
                boundaries[i][1] = endPointer;
            }

            boundaries[numSegments][0] = Math.max(endPointer, numSegments * CHUNK_SIZE);
            boundaries[numSegments][1] = fileSize;

            return boundaries;
        }
    }

    private static Map<String, PartitionAggregate> processSegment(long startOfSegment, long endOfSegment)
            throws IOException {
        Map<String, PartitionAggregate> stationAggregates = new HashMap<>(50_000);
        var byteChunk = new byte[(int) (endOfSegment - startOfSegment)];
        var stationBuffer = new byte[MAX_STATION_NAME_LENGTH];
        try (var file = new RandomAccessFile(FILE, "r")) {
            file.seek(startOfSegment);
            file.read(byteChunk);
            var i = 0;
            while (i < byteChunk.length) {
                // Station name has at least one byte
                stationBuffer[0] = byteChunk[i];
                i++;
                // Read station name
                var j = 1;
                while (byteChunk[i] != ';') {
                    stationBuffer[j] = byteChunk[i];
                    j++;
                    i++;
                }
                var station = new String(stationBuffer, 0, j, StandardCharsets.UTF_8);
                i++;

                // Read measurement
                var isNegative = byteChunk[i] == '-';
                var measurement = 0;
                if (isNegative) {
                    i++;
                    while (byteChunk[i] != '.') {
                        measurement = measurement * 10 + byteChunk[i] - '0';
                        i++;
                    }
                    measurement = (measurement * 10 + byteChunk[i + 1] - '0') * -1;
                }
                else {
                    while (byteChunk[i] != '.') {
                        measurement = measurement * 10 + byteChunk[i] - '0';
                        i++;
                    }
                    measurement = measurement * 10 + byteChunk[i + 1] - '0';
                }

                // Update aggregate
                var aggregate = stationAggregates.computeIfAbsent(station, x -> new PartitionAggregate());
                aggregate.addMeasurementAndComputeAggregate((short) measurement);
                i += 3;
            }
            stationAggregates.values().forEach(PartitionAggregate::aggregateRemainingMeasurements);
        }

        return stationAggregates;
    }

    private static class PartitionAggregate {
        final short[] doubleLane = new short[SIMD_LANE_LENGTH * 2];
        // Assume that we do not have more than Integer.MAX_VALUE measurements for the same station per partition
        int count = 0;
        long sum = 0;
        short min = Short.MAX_VALUE;
        short max = Short.MIN_VALUE;

        public void addMeasurementAndComputeAggregate(short measurement) {
            // Add measurement to buffer, which is later processed by SIMD instructions
            doubleLane[count % doubleLane.length] = measurement;
            count++;

            // Once lane is full, use SIMD instructions to calculate aggregates
            if (count % doubleLane.length == 0) {
                var firstVector = ShortVector.fromArray(ShortVector.SPECIES_MAX, doubleLane, 0);
                var secondVector = ShortVector.fromArray(ShortVector.SPECIES_MAX, doubleLane, SIMD_LANE_LENGTH);

                var simdMin = firstVector.min(secondVector).reduceLanes(VectorOperators.MIN);
                min = (short) Math.min(min, simdMin);

                var simdMax = firstVector.max(secondVector).reduceLanes(VectorOperators.MAX);
                max = (short) Math.max(max, simdMax);

                sum += firstVector.add(secondVector).reduceLanes(VectorOperators.ADD);
            }
        }

        public void aggregateRemainingMeasurements() {
            for (var i = 0; i < count % doubleLane.length; i++) {
                var measurement = doubleLane[i];
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
                    ((sum / 10.0) / count),
                    (max / 10.0));
        }
    }
}
