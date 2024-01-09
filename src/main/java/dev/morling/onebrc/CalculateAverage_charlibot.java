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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_charlibot {

    private static final String FILE = "./measurements.txt";

    private static final int BUFFER_SIZE = 1024 * 1024 * 10;

    private static final int MAP_CAPACITY = 16384; // Need at least 10,000 so 2^14 = 16384. Might need 2^15 = 32768.

    public static void main(String[] args) throws Exception {
        multiThreadedReadingDoItAll();
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

    static int hashArraySlice(byte[] array, int offset, int length) {
        int hashcode = 0;
        for (int i = offset; i < offset + length; i++) {
            hashcode = 31 * hashcode + array[i];
        }
        // Not sure the below actually helps much?
        // hashcode = hashcode >>> 16; // Do the same trick as-in hashmap since we're using power of 2
        return hashcode;
    }

    static class MeasurementMap {

        final int[][] map;
        final int capacity = MAP_CAPACITY;

        final int numIntsToStoreCity = 25; // stores up to 100 characters.
        int minPos = numIntsToStoreCity;
        int maxPos = numIntsToStoreCity + 1;
        int sumPos = numIntsToStoreCity + 2;
        int countPos = numIntsToStoreCity + 3;

        MeasurementMap() {
            map = new int[capacity][numIntsToStoreCity + 4]; // length of string and then the city encoded cast bytes to int. then min, max, sum, count,
        }

        public void insert(byte[] array, int offset, int length, int value) {
            int hashcode = hashArraySlice(array, offset, length);
            int index = hashcode & (capacity - 1); // same trick as in hashmap. This is the same as (% capacity).
            tryInsert(index, array, offset, length, value);
        }

        private void tryInsert(int mapIndex, byte[] array, int offset, int length, int value) {
            outer: while (true) {
                int[] jas = map[mapIndex];
                if (jas[0] == 0) {
                    // just insert
                    int i = 0;
                    int jasIndex = -1;
                    while (i < length) {
                        byte b = array[i + offset];
                        // i & 3 is the same as i % 4
                        if ((i & 3) == 0) { // when at i=0,4,8,12 then
                            jasIndex++;
                        }
                        jas[jasIndex] = jas[jasIndex] | ((b & 0xFF) << (8 * (i & 3)));
                        i++;
                    }
                    jas[minPos] = value;
                    jas[maxPos] = value;
                    jas[sumPos] = value;
                    jas[countPos] = 1;
                    break;
                }
                else {
                    int i = 0;
                    int jasIndex = -1;
                    while (i < length) {
                        byte b = array[i + offset];
                        if ((i & 3) == 0) { // when at i=0,4,8,12,... then
                            jasIndex++;
                        }
                        byte inJas = (byte) (jas[jasIndex] >>> (8 * (i & 3)));
                        if (b != inJas) {
                            mapIndex = (mapIndex + 1) & (capacity - 1);
                            continue outer;
                        }
                        i++;
                    }
                    jas[minPos] = min(value, jas[minPos]);
                    jas[maxPos] = max(value, jas[maxPos]);
                    jas[sumPos] += value;
                    jas[countPos] += 1;
                    break;
                }
            }
        }

        public HashMap<String, Measurement> toMap() {
            HashMap<String, Measurement> hashMap = new HashMap<>();
            for (int[] jas : map) {
                if (jas[0] != 0) {
                    int jasIndex = 0;
                    byte[] array = new byte[numIntsToStoreCity * 4];
                    while (jasIndex < numIntsToStoreCity) {
                        int tmp = jas[jasIndex];
                        array[jasIndex * 4] = (byte) tmp;
                        array[jasIndex * 4 + 1] = (byte) (tmp >>> 8);
                        array[jasIndex * 4 + 2] = (byte) (tmp >>> 16);
                        array[jasIndex * 4 + 3] = (byte) (tmp >>> 24);
                        jasIndex++;
                    }
                    int length = array.length;
                    for (int i = 0; i < array.length; i++) {
                        if (array[i] == 0) {
                            length = i;
                            break;
                        }
                    }
                    String city = new String(array, 0, length, StandardCharsets.UTF_8);
                    Measurement m = new Measurement(0);
                    m.min = jas[minPos];
                    m.max = jas[maxPos];
                    m.sum = jas[sumPos];
                    m.count = jas[countPos];
                    hashMap.put(city, m);
                }
            }
            return hashMap;
        }

        public Set<Map.Entry<String, Measurement>> entrySet() {
            return toMap().entrySet();
        }
    }

    public static void multiThreadedReadingDoItAll() throws Exception {
        File file = Path.of(FILE).toFile();
        long length = file.length();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        long chunkToRead = length / numProcessors;

        // make life easier by spending a bit of time up front to find line breaks around the chunks
        final long[] startPositions = new long[numProcessors + 1];
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] buffer = new byte[256];
            for (int processIdx = 1; processIdx < numProcessors; processIdx++) {
                long initialSeekPoint = processIdx * chunkToRead;
                raf.seek(initialSeekPoint);
                int bytesRead = raf.read(buffer);
                // if (bytesRead != buffer.length) {
                // throw new Exception("Actual read is not same as requested. " + bytesRead);
                // }
                int i = 0;
                while (buffer[i] != '\n') {
                    i++;
                }
                initialSeekPoint += (i + 1);
                startPositions[processIdx] = initialSeekPoint;
            }
            startPositions[numProcessors] = length;
        }

        try (ExecutorService executorService = Executors.newWorkStealingPool(numProcessors)) {
            Future[] results = new Future[numProcessors];
            for (int processIdx = 0; processIdx < numProcessors; processIdx++) {
                long seekPoint = startPositions[processIdx];
                long bytesToRead = startPositions[processIdx + 1] - startPositions[processIdx];
                Future<HashMap<String, Measurement>> future = executorService.submit(() -> {
                    MeasurementMap measurements = new MeasurementMap();
                    try (FileInputStream fis = new FileInputStream(file)) {
                        long actualSkipped = fis.skip(seekPoint);
                        if (actualSkipped != seekPoint) {
                            throw new Exception("Uho oh");
                        }
                        byte[] buffer = new byte[BUFFER_SIZE];
                        long totalBytesRead = 0;
                        int bytesRead;
                        int currentCityLength = 0;
                        while ((bytesRead = fis.read(buffer, currentCityLength, buffer.length - currentCityLength)) != -1) {
                            totalBytesRead -= currentCityLength; // avoid double counting. There must be a better way !
                            if (totalBytesRead >= bytesToRead && currentCityLength == 0) {
                                // we have read everything we intend to and there is no city in the buffer to finish processing
                                return measurements.toMap();
                            }
                            int i = 0;
                            int cityIndexStart = 0;
                            int cityLength;
                            int multiplier = 1;
                            int value = 0;
                            while (i < bytesRead + currentCityLength) {
                                if (totalBytesRead >= bytesToRead) {
                                    // we have read everything we intend to for this chunk
                                    return measurements.toMap();
                                }
                                if (buffer[i] == ';') {
                                    cityLength = i - cityIndexStart;
                                    i++;
                                    totalBytesRead++;
                                    if (i == bytesRead + currentCityLength) {
                                        System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                        bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                        currentCityLength = cityLength;
                                        cityIndexStart = 0;
                                        i = cityLength;
                                    }
                                    if (buffer[i] == '-') {
                                        multiplier = -1;
                                        i++;
                                        totalBytesRead++;
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    while (buffer[i] != '\n') {
                                        if (buffer[i] != '.') {
                                            value = (value * 10) + (buffer[i] - '0');
                                        }
                                        i++;
                                        totalBytesRead++;
                                        if (i == bytesRead + currentCityLength) {
                                            System.arraycopy(buffer, cityIndexStart, buffer, 0, cityLength);
                                            bytesRead = fis.read(buffer, cityLength, buffer.length - cityLength);
                                            currentCityLength = cityLength;
                                            cityIndexStart = 0;
                                            i = cityLength;
                                        }
                                    }
                                    value = value * multiplier; // is boolean check faster?
                                    measurements.insert(buffer, cityIndexStart, cityLength, value);
                                    if (totalBytesRead >= bytesToRead) {
                                        return measurements.toMap();
                                    }
                                    // buffer[i] == \n so go one more
                                    cityIndexStart = i + 1;
                                    value = 0;
                                    multiplier = 1;
                                }
                                i++;
                                totalBytesRead++;
                            }
                            currentCityLength = buffer.length - cityIndexStart;
                            System.arraycopy(buffer, cityIndexStart, buffer, 0, currentCityLength);
                        }
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
