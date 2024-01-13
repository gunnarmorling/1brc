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

    private static final int NUM_STATIONS = 10_000;

    private static final int FASTER_HASH_MAP_CAPACITY = 200_000;

    public static void main(String[] args) throws IOException {
        var result = Arrays.asList(getSegments()).parallelStream()
                .map(segment -> {
                    try {
                        return processSegment(segment[0], segment[1]);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .reduce(FasterHashMap::mergeWith)
                .get();

        var sortedMap = new TreeMap<String, Station>();
        for (Station station : result.getValues()) {
            sortedMap.put(station.getParsedName(), station);
        }

        System.out.println(sortedMap);
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

    private static FasterHashMap processSegment(long startOfSegment, long endOfSegment)
            throws IOException {
        var fasterHashMap = new FasterHashMap();
        var byteChunk = new byte[(int) (endOfSegment - startOfSegment)];
        var nameBuffer = new byte[MAX_STATION_NAME_LENGTH];
        try (var file = new RandomAccessFile(FILE, "r")) {
            file.seek(startOfSegment);
            file.read(byteChunk);
            var i = 0;
            while (i < byteChunk.length) {
                // Station name has at least one byte
                nameBuffer[0] = byteChunk[i++];
                // Read station name
                var nameLength = 1;
                var nameHash = 0;
                while (byteChunk[i] != ';') {
                    nameHash = nameHash * 31 + byteChunk[i];
                    nameBuffer[nameLength++] = byteChunk[i++];
                }
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
                fasterHashMap.addEntry(nameHash, nameBuffer, nameLength, (short) measurement);
                i += 3;
            }
        }

        for (Station station : fasterHashMap.getValues()) {
            station.aggregateRemainingMeasurements();
        }

        return fasterHashMap;
    }

    private static class Station {
        final short[] measurements = new short[SIMD_LANE_LENGTH * 2];
        // Assume that we do not have more than Integer.MAX_VALUE measurements for the same station per partition
        int count = 1;
        long sum = 0;
        short min = Short.MAX_VALUE;
        short max = Short.MIN_VALUE;
        final byte[] name;
        final int nameLength;
        final int nameHash;

        public Station(int nameLength, byte[] name, int nameHash, short measurement) {
            this.nameLength = nameLength;
            this.name = name.clone();
            this.nameHash = nameHash;
            measurements[0] = measurement;
        }

        public String getParsedName() {
            return new String(name, 0, nameLength, StandardCharsets.UTF_8);
        }

        public void addMeasurementAndComputeAggregate(short measurement) {
            // Add measurement to buffer, which is later processed by SIMD instructions
            measurements[count % measurements.length] = measurement;
            count++;

            // Once lane is full, use SIMD instructions to calculate aggregates
            if (count % measurements.length == 0) {
                var firstVector = ShortVector.fromArray(ShortVector.SPECIES_MAX, measurements, 0);
                var secondVector = ShortVector.fromArray(ShortVector.SPECIES_MAX, measurements, SIMD_LANE_LENGTH);

                var simdMin = firstVector.min(secondVector).reduceLanes(VectorOperators.MIN);
                min = (short) Math.min(min, simdMin);

                var simdMax = firstVector.max(secondVector).reduceLanes(VectorOperators.MAX);
                max = (short) Math.max(max, simdMax);

                sum += firstVector.add(secondVector).reduceLanes(VectorOperators.ADD);
            }
        }

        public void aggregateRemainingMeasurements() {
            for (var i = 0; i < count % measurements.length; i++) {
                var measurement = measurements[i];
                min = (short) Math.min(min, measurement);
                max = (short) Math.max(max, measurement);
                sum += measurement;
            }
        }

        public void mergeWith(Station otherAggregate) {
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

    /**
     * Use two arrays for implementing the hash map:
     * - The array `entries` holds the map values, in our case instances of the class Station.
     * - The array `offsets` maps hashes of the keys to indexes in the `entries` array.
     *
     * We create `offsets` with a much larger capacity than `entries`, so we minimize collisions.
     */
    private static class FasterHashMap {
        // Using 16-bit integers (shorts) for offsets supports up to 2^15 (=32,767) entries
        // If you need to store more entries, consider replacing short with int
        short[] offsets = new short[FASTER_HASH_MAP_CAPACITY];
        Station[] entries = new Station[NUM_STATIONS + 1];
        int slotsInUse = 0;

        private boolean arraysAreEqual(byte[] a, byte[] b, int lengthA, int lengthB) {
            if (lengthA != lengthB) {
                return false;
            }
            for (var i = 0; i < lengthA; i++) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }

        private int getOffsetIdx(int nameHash, byte[] name, int nameLength) {
            var offsetIdx = nameHash & (offsets.length - 1);
            var offset = offsets[offsetIdx];

            while (offset != 0 && !arraysAreEqual(entries[offset].name, name, entries[offset].nameLength, nameLength)) {
                offsetIdx = (offsetIdx + 1) % offsets.length;
                offset = offsets[offsetIdx];
            }

            return offsetIdx;
        }

        public void addEntry(int nameHash, byte[] name, int nameLength, short measurement) {
            var offsetIdx = getOffsetIdx(nameHash, name, nameLength);
            var offset = offsets[offsetIdx];
            if (offset == 0) {
                slotsInUse++;
                entries[slotsInUse] = new Station(nameLength, name, nameHash, measurement);
                offsets[offsetIdx] = (short) slotsInUse;
            }
            else {
                entries[offset].addMeasurementAndComputeAggregate(measurement);
            }
        }

        public FasterHashMap mergeWith(FasterHashMap otherMap) {
            for (Station station : otherMap.getValues()) {
                var offsetIdx = getOffsetIdx(station.nameHash, station.name, station.nameLength);
                var offset = offsets[offsetIdx];
                if (offset == 0) {
                    slotsInUse++;
                    entries[slotsInUse] = station;
                    offsets[offsetIdx] = (short) slotsInUse;
                }
                else {
                    entries[offset].mergeWith(station);
                }
            }
            return this;
        }

        public List<Station> getValues() {
            return Arrays.asList(entries).subList(1, slotsInUse + 1);
        }
    }
}
