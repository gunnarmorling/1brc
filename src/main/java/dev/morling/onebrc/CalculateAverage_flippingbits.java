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

import sun.misc.Unsafe;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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

    private static final long MINIMUM_FILE_SIZE_PARTITIONING = 10 * 1024 * 1024; // 10 MB

    private static final int SIMD_LANE_LENGTH = ShortVector.SPECIES_MAX.length();

    private static final int NUM_STATIONS = 10_000;

    private static final int HASH_MAP_OFFSET_CAPACITY = 200_000;

    private static final Unsafe UNSAFE = initUnsafe();

    private static int HASH_PRIME_NUMBER = 31;

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

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
        for (Station station : result.getEntries()) {
            sortedMap.put(station.getName(), station);
        }

        System.out.println(sortedMap);
    }

    private static long[][] getSegments() throws IOException {
        try (var file = new RandomAccessFile(FILE, "r")) {
            var channel = file.getChannel();

            var fileSize = channel.size();
            var startAddress = channel
                    .map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global())
                    .address();

            // Split file into segments, so we can work around the size limitation of channels
            var numSegments = (fileSize > MINIMUM_FILE_SIZE_PARTITIONING)
                    ? Runtime.getRuntime().availableProcessors()
                    : 1;
            var segmentSize = fileSize / numSegments;

            var boundaries = new long[numSegments][2];
            var endPointer = startAddress;

            for (var i = 0; i < numSegments - 1; i++) {
                // Start of segment
                boundaries[i][0] = endPointer;

                // Extend segment until end of line or file
                endPointer = endPointer + segmentSize;
                while (UNSAFE.getByte(endPointer) != '\n') {
                    endPointer++;
                }

                // End of segment
                boundaries[i][1] = endPointer++;
            }

            boundaries[numSegments - 1][0] = endPointer;
            boundaries[numSegments - 1][1] = startAddress + fileSize;

            return boundaries;
        }
    }

    private static FasterHashMap processSegment(long startOfSegment, long endOfSegment) throws IOException {
        var fasterHashMap = new FasterHashMap();
        for (var i = startOfSegment; i < endOfSegment; i += 3) {
            // Read station name
            int nameHash = UNSAFE.getByte(i);
            final var nameStartAddress = i++;
            var character = UNSAFE.getByte(i);
            while (character != ';') {
                nameHash = nameHash * HASH_PRIME_NUMBER + character;
                i++;
                character = UNSAFE.getByte(i);
            }
            var nameLength = (int) (i - nameStartAddress);
            i++;

            // Read measurement
            var isNegative = UNSAFE.getByte(i) == '-';
            var measurement = 0;
            if (isNegative) {
                i++;
                character = UNSAFE.getByte(i);
                while (character != '.') {
                    measurement = measurement * 10 + character - '0';
                    i++;
                    character = UNSAFE.getByte(i);
                }
                measurement = (measurement * 10 + UNSAFE.getByte(i + 1) - '0') * -1;
            }
            else {
                character = UNSAFE.getByte(i);
                while (character != '.') {
                    measurement = measurement * 10 + character - '0';
                    i++;
                    character = UNSAFE.getByte(i);
                }
                measurement = measurement * 10 + UNSAFE.getByte(i + 1) - '0';
            }

            fasterHashMap.addEntry(nameHash, nameLength, nameStartAddress, (short) measurement);
        }

        for (Station station : fasterHashMap.getEntries()) {
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
        final long nameAddress;
        final int nameLength;
        final int nameHash;

        public Station(int nameHash, int nameLength, long nameAddress, short measurement) {
            this.nameHash = nameHash;
            this.nameLength = nameLength;
            this.nameAddress = nameAddress;
            measurements[0] = measurement;
        }

        public String getName() {
            byte[] name = new byte[nameLength];
            UNSAFE.copyMemory(null, nameAddress, name, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);
            return new String(name, StandardCharsets.UTF_8);
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

        public void mergeWith(Station otherStation) {
            min = (short) Math.min(min, otherStation.min);
            max = (short) Math.max(max, otherStation.max);
            count = count + otherStation.count;
            sum = sum + otherStation.sum;
        }

        public boolean nameEquals(long otherNameAddress) {
            var swarLimit = (nameLength / Long.BYTES) * Long.BYTES;
            var i = 0;
            for (; i < swarLimit; i += Long.BYTES) {
                if (UNSAFE.getLong(nameAddress + i) != UNSAFE.getLong(otherNameAddress + i)) {
                    return false;
                }
            }
            for (; i < nameLength; i++) {
                if (UNSAFE.getByte(nameAddress + i) != UNSAFE.getByte(otherNameAddress + i)) {
                    return false;
                }
            }
            return true;
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
        short[] offsets = new short[HASH_MAP_OFFSET_CAPACITY];
        Station[] entries = new Station[NUM_STATIONS + 1];
        int slotsInUse = 0;

        private int getOffsetIdx(int nameHash, int nameLength, long nameAddress) {
            var offsetIdx = nameHash & (offsets.length - 1);
            var offset = offsets[offsetIdx];

            while (offset != 0 &&
                    (nameLength != entries[offset].nameLength || !entries[offset].nameEquals(nameAddress))) {
                offsetIdx = (offsetIdx + 1) % offsets.length;
                offset = offsets[offsetIdx];
            }

            return offsetIdx;
        }

        public void addEntry(int nameHash, int nameLength, long nameAddress, short measurement) {
            var offsetIdx = getOffsetIdx(nameHash, nameLength, nameAddress);
            var offset = offsets[offsetIdx];

            if (offset == 0) {
                slotsInUse++;
                entries[slotsInUse] = new Station(nameHash, nameLength, nameAddress, measurement);
                offsets[offsetIdx] = (short) slotsInUse;
            }
            else {
                entries[offset].addMeasurementAndComputeAggregate(measurement);
            }
        }

        public FasterHashMap mergeWith(FasterHashMap otherMap) {
            for (Station station : otherMap.getEntries()) {
                var offsetIdx = getOffsetIdx(station.nameHash, station.nameLength, station.nameAddress);
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

        public List<Station> getEntries() {
            return Arrays.asList(entries).subList(1, slotsInUse + 1);
        }
    }
}
