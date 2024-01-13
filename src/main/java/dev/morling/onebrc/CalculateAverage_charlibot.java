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

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_charlibot {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int MAP_CAPACITY = 16384; // Need at least 10,000 so 2^14 = 16384. Might need 2^15 = 32768.

    public static void main(String[] args) throws Exception {
        memoryMap();
    }

    // Copied from Roy van Rijn's code
    // branchless max (unprecise for large numbers, but good enough)
    static int max(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    // branchless min (unprecise for large numbers, but good enough)
    static int min(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
    }

    static class Measurement {
        int min;
        int max;
        int sum;
        int count;

        Measurement(int value) {
            min = value;
            max = value;
            sum = value;
            count = 1;
        }

        @Override
        public String toString() {
            double minD = (double) min / 10;
            double maxD = (double) max / 10;
            double meanD = (double) sum / 10 / count;
            return round(minD) + "/" + round(meanD) + "/" + round(maxD);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class MeasurementMap3 {

        final Measurement[] measurements;
        final byte[][] cities;

        final int capacity = MAP_CAPACITY;

        MeasurementMap3() {
            measurements = new Measurement[capacity];
            cities = new byte[capacity][128]; // 100 bytes for the city. Round up to nearest power of 2.
        }

        public void insert(long fromAddress, long toAddress, int hashcode, int value) {
            int index = hashcode & (capacity - 1); // same trick as in hashmap. This is the same as (% capacity).
            tryInsert(index, fromAddress, toAddress, value);
        }

        private void tryInsert(int mapIndex, long fromAddress, long toAddress, int value) {
            byte length = (byte) (toAddress - fromAddress);
            outer: while (true) {
                byte[] cityArray = cities[mapIndex];
                Measurement jas = measurements[mapIndex];
                if (jas != null) {
                    if (cityArray[0] == length) {
                        int i = 0;
                        while (i < length) {
                            byte b = UNSAFE.getByte(fromAddress + i);
                            if (b != cityArray[i + 1]) {
                                mapIndex = (mapIndex + 1) & (capacity - 1);
                                continue outer;
                            }
                            i++;
                        }
                        jas.min = min(value, jas.min);
                        jas.max = max(value, jas.max);
                        jas.sum += value;
                        jas.count += 1;
                        break;
                    }
                    else {
                        mapIndex = (mapIndex + 1) & (capacity - 1);
                    }
                }
                else {
                    // just insert
                    int i = 0;
                    cityArray[0] = length;
                    while (i < length) {
                        byte b = UNSAFE.getByte(fromAddress + i);
                        cityArray[i + 1] = b;
                        i++;
                    }
                    measurements[mapIndex] = new Measurement(value);
                    break;

                }
            }
        }

        public HashMap<String, Measurement> toMap() {
            HashMap<String, Measurement> hashMap = new HashMap<>();
            for (int mapIndex = 0; mapIndex < cities.length; mapIndex++) {
                byte[] cityArray = cities[mapIndex];
                Measurement measurement = measurements[mapIndex];
                if (measurement != null) {
                    int length = cityArray[0];
                    String city = new String(cityArray, 1, length, StandardCharsets.UTF_8);
                    hashMap.put(city, measurement);
                }
            }
            return hashMap;
        }

        public Set<Map.Entry<String, Measurement>> entrySet() {
            return toMap().entrySet();
        }
    }

    public static long[] getChunks(int numChunks) throws Exception {
        long[] chunks = new long[numChunks + 1];
        try (FileChannel fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long sizeOfChunk = fileSize / numChunks;
            var address = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = address;
            for (int processIdx = 1; processIdx < numChunks; processIdx++) {
                long chunkAddress = processIdx * sizeOfChunk + address;
                while (UNSAFE.getByte(chunkAddress) != '\n') {
                    chunkAddress++;
                }
                chunkAddress++;
                chunks[processIdx] = chunkAddress;
            }
            chunks[numChunks] = address + fileSize;
        }
        return chunks;
    }

    public static void memoryMap() throws Exception {
        int numProcessors = Runtime.getRuntime().availableProcessors();
        long[] chunks = getChunks(numProcessors);
        try (ExecutorService executorService = Executors.newWorkStealingPool(numProcessors)) {
            Future[] results = new Future[numProcessors];
            for (int processIdx = 0; processIdx < numProcessors; processIdx++) {
                int finalProcessIdx = processIdx;
                Future<HashMap<String, Measurement>> future = executorService.submit(() -> {
                    long chunkIdx = chunks[finalProcessIdx];
                    long chunkEnd = chunks[finalProcessIdx + 1];
                    MeasurementMap3 measurements = new MeasurementMap3();
                    while (chunkIdx < chunkEnd) {
                        long cityStart = chunkIdx;
                        byte b;
                        int hashcode = 0;
                        while ((b = UNSAFE.getByte(chunkIdx)) != ';') {
                            hashcode = 31 * hashcode + b;
                            chunkIdx++;
                        }
                        long cityEnd = chunkIdx;
                        chunkIdx++;
                        int multiplier = 1;
                        b = UNSAFE.getByte(chunkIdx);
                        if (b == '-') {
                            multiplier = -1;
                            chunkIdx++;
                        }
                        int value = 0;
                        while ((b = UNSAFE.getByte(chunkIdx)) != '\n') {
                            if (b != '.') {
                                value = (value * 10) + (b - '0');
                            }
                            chunkIdx++;
                        }
                        value = value * multiplier;
                        measurements.insert(cityStart, cityEnd, hashcode, value);
                        chunkIdx++;
                    }
                    return measurements.toMap();
                });
                results[processIdx] = future;
            }
            final HashMap<String, Measurement> measurements = new HashMap<>();
            for (Future f : results) {
                HashMap<String, Measurement> m = (HashMap<String, Measurement>) f.get();
                m.forEach((city, measurement) -> {
                    measurements.merge(city, measurement, (oldValue, newValue) -> {
                        Measurement mmm = new Measurement(0);
                        mmm.min = Math.min(oldValue.min, newValue.min);
                        mmm.max = Math.max(oldValue.max, newValue.max);
                        mmm.sum = oldValue.sum + newValue.sum;
                        mmm.count = oldValue.count + newValue.count;
                        return mmm;
                    });
                });
            }
            System.out.print("{");
            System.out.print(
                    measurements.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
            System.out.println("}");
        }
    }
}
