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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class CalculateAverage_Smoofie {

    private static final String FILE = "./measurements.txt";
    private static final Unsafe unsafe = getUnsafe();

    private static class MeasurementAggregator {
        private int min = -1000;
        private int max = 1000;
        private long sum = 0;
        private int count = 0;

        @Override
        public String toString() {
            return ((double) min) / 10 + "/" + round(sum / 10.0 / count) + "/" + ((double) max) / 10;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static final class CountResult {
        private final long cityHashTableAddress;
        private final long countsAddress;
        private int cityIdCounter;
        private long nextCollisionAddress;

        private CountResult(

                            // cityId|cityLength|cityNameAddress|nextElementAddress|cityCountsAddress
                            long cityHashTableAddress,
                            long countsAddress,
                            int cityIdCounter,
                            long nextCollisionAddress) {
            this.cityHashTableAddress = cityHashTableAddress;
            this.countsAddress = countsAddress;
            this.cityIdCounter = cityIdCounter;
            this.nextCollisionAddress = nextCollisionAddress;
        }

    }

    private static int hash(long cityNameAddress, short cityLength) {
        if (cityLength < 17) {
            long[] city = new long[2];
            unsafe.copyMemory(null, cityNameAddress, city, Unsafe.ARRAY_LONG_BASE_OFFSET, cityLength);
            long hash = city[0] ^ (city[1] >> 1);
            int foldedHash = (int) (hash ^ (hash >>> 31));
            return (foldedHash & foldedHash >>> 15) & 0xffff;
        }
        else {
            long[] city = new long[cityLength >> 3 + 1];
            unsafe.copyMemory(null, cityNameAddress, city, Unsafe.ARRAY_LONG_BASE_OFFSET, cityLength);

            long hash = city[0];
            for (int i = 1; i < city.length; i++) {
                hash ^= city[i];
            }

            int foldedHash = (int) (hash ^ (hash >>> 30));
            return (foldedHash & foldedHash >>> 15) & 0xffff;
        }
    }

    private static Unsafe getUnsafe() {
        try {
            var field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static long locateSemicolon(long input) {
        long semiXor = input ^ 0x3B3B3B3B3B3B3B3BL;
        return (semiXor - 0x0101010101010101L) & ~semiXor & 0x8080808080808080L;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        var numberOfThreads = Runtime.getRuntime().availableProcessors();
        var executorService = Executors.newFixedThreadPool(numberOfThreads);
        var resultMap = new TreeMap<String, MeasurementAggregator>();
        var subCountResults = new CountResult[numberOfThreads];

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(FILE, "r");
                FileChannel fileChannel = randomAccessFile.getChannel()) {

            long fileSize = randomAccessFile.length();
            if (fileSize < numberOfThreads * 1024) {
                numberOfThreads = fileSize < 1024 ? 1 : (int) (fileSize / 1024);
            }
            long chunkSize = fileSize / numberOfThreads;

            long inputFileAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            final long[] inputFileMemoryOffsets = new long[numberOfThreads + 1];
            inputFileMemoryOffsets[0] = inputFileAddress;
            inputFileMemoryOffsets[numberOfThreads] = inputFileAddress + fileSize;
            for (long i = inputFileAddress + chunkSize, index = 1; index < numberOfThreads; i += chunkSize, index++) {
                while (unsafe.getByte(i++) != '\n')
                    ;
                inputFileMemoryOffsets[(int) index] = i;
            }

            for (int i = 0; i < numberOfThreads; i++) {
                final long start = inputFileMemoryOffsets[i];
                final long end = inputFileMemoryOffsets[i + 1];

                final int threadIndex = i;
                executorService.execute(() -> {
                    var cityHashTableAddress = unsafe.allocateMemory(75536 * 32);
                    unsafe.setMemory(cityHashTableAddress, 75536 * 32, (byte) 0);
                    long nextCollisionAddress = cityHashTableAddress + (65536 << 5);

                    var countsAddress = unsafe.allocateMemory(10000 * 2 * 1000 * 4);
                    int cityId;
                    int temperature = 0;
                    int cityIdCounter = 0;
                    long position = start;
                    byte c;
                    long input;
                    long inputSemicolon;
                    int cityHash;
                    long hashAddress;
                    short cityLength;
                    long cityStart;
                    long temperatureAddress;
                    while (position < end) {
                        cityStart = position;
                        input = unsafe.getLong(position);
                        inputSemicolon = locateSemicolon(input);
                        if (inputSemicolon == 0) {
                            position += 8;
                            input = unsafe.getLong(position);
                            inputSemicolon = locateSemicolon(input);

                            if (inputSemicolon == 0) {
                                // probably not gonna happen very often
                                while (inputSemicolon == 0) {
                                    position += 8;
                                    input = unsafe.getLong(position);
                                    inputSemicolon = locateSemicolon(input);
                                }
                            }
                        }
                        position += Long.numberOfTrailingZeros(inputSemicolon) >> 3;

                        cityLength = (short) (position - cityStart);

                        cityHash = hash(cityStart, cityLength);
                        hashAddress = cityHashTableAddress + ((long) cityHash << 5);
                        cityId = -1;
                        outer: for (;;) {
                            if (cityLength != unsafe.getShort(hashAddress + 4)) {
                                if (unsafe.getShort(hashAddress + 4) == 0) {
                                    // new hash slot init
                                    cityId = cityIdCounter++;
                                    unsafe.setMemory(countsAddress + cityId * 8000, 8000, (byte) 0);
                                    unsafe.putInt(hashAddress, cityId);
                                    unsafe.putShort(hashAddress + 4, cityLength);
                                    unsafe.putLong(hashAddress + 6, cityStart);
                                    unsafe.putLong(hashAddress + 22, countsAddress + cityId * 8000);
                                    break;
                                }
                                if (unsafe.getLong(hashAddress + 14) != 0) {
                                    hashAddress = unsafe.getLong(hashAddress + 14);
                                    continue;
                                }
                                break;
                            }
                            long cityNameAddress = unsafe.getLong(hashAddress + 6);
                            int j;
                            for (j = 0; j < cityLength >> 3 << 3; j += 8) {
                                if (unsafe.getLong(cityStart + j) != unsafe.getLong(cityNameAddress + j)) {
                                    if (unsafe.getLong(hashAddress + 14) != 0) {
                                        hashAddress = unsafe.getLong(hashAddress + 14);
                                        continue outer;
                                    }
                                    break outer;
                                }
                            }
                            if (j < cityLength) {
                                if ((unsafe.getLong(cityStart + j) << ((0x8 - cityLength & 0x7) << 3)) != (unsafe
                                        .getLong(cityNameAddress + j) << ((0x8 - cityLength & 0x7) << 3))) {
                                    if (unsafe.getLong(hashAddress + 14) != 0) {
                                        hashAddress = unsafe.getLong(hashAddress + 14);
                                        continue;
                                    }
                                    break;
                                }
                            }
                            cityId = unsafe.getInt(hashAddress);
                            break;
                        }

                        if (cityId == -1) {
                            // collision
                            cityId = cityIdCounter++;
                            unsafe.setMemory(countsAddress + cityId * 8000, 8000, (byte) 0);
                            unsafe.putLong(hashAddress + 14, nextCollisionAddress);
                            hashAddress = nextCollisionAddress;
                            nextCollisionAddress += 32;
                            unsafe.putInt(hashAddress, cityId);
                            unsafe.putShort(hashAddress + 4, cityLength);
                            unsafe.putLong(hashAddress + 6, cityStart);
                            unsafe.putLong(hashAddress + 22, countsAddress + cityId * 8000);
                        }

                        position++; // skip semicolon

                        // long inputDecimalPoint = locateDecimalPoint(unsafe.getLong(position));
                        // position += (Long.numberOfTrailingZeros(inputDecimalPoint) >> 3) + 3;

                        temperature = 0;
                        c = unsafe.getByte(position++);
                        if (c == '-') {
                            while ((c = unsafe.getByte(position++)) != '\n') {
                                if (c != '.') {
                                    temperature = temperature * 10 + (c ^ 0x30);
                                }
                            }
                            temperatureAddress = unsafe.getLong(hashAddress + 22) + (1000 + temperature) * 4;
                            unsafe.putInt(temperatureAddress, unsafe.getInt(temperatureAddress) + 1);
                        }
                        else {
                            temperature = c - '0';
                            while ((c = unsafe.getByte(position++)) != '\n') {
                                if (c != '.') {
                                    temperature = temperature * 10 + (c ^ 0x30);
                                }
                            }

                            temperatureAddress = unsafe.getLong(hashAddress + 22) + temperature * 4;
                            unsafe.putInt(temperatureAddress, unsafe.getInt(temperatureAddress) + 1);
                        }
                    }
                    subCountResults[threadIndex] = new CountResult(cityHashTableAddress, countsAddress, cityIdCounter, nextCollisionAddress);
                });
            }

            executorService.shutdown();
            executorService.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS);

            // aggregate results 1..n to 0
            var subCountA = subCountResults[0];
            for (int r = 1; r < numberOfThreads; r++) {
                CountResult subCountB = subCountResults[r];
                for (int i = 0; i < 65536; i++) {
                    long bHashAddress = subCountB.cityHashTableAddress + ((long) i << 5);
                    if (unsafe.getShort(bHashAddress + 4) == 0) {
                        continue;
                    }
                    long aHashAddress = subCountA.cityHashTableAddress + ((long) i << 5);
                    // check if a initialized
                    if (unsafe.getShort(aHashAddress + 4) == 0) {
                        // new hash slot init
                        for (long addressA = aHashAddress, addressB = bHashAddress; addressB != 0;) {
                            unsafe.putInt(addressA, subCountA.cityIdCounter++);
                            unsafe.putShort(addressA + 4, unsafe.getShort(addressB + 4));
                            unsafe.putLong(addressA + 6, unsafe.getLong(addressB + 6));
                            addressB = unsafe.getLong(addressB + 14);
                            if (addressB != 0) {
                                unsafe.putLong(addressA + 14, subCountA.nextCollisionAddress);
                                addressA = subCountA.nextCollisionAddress;
                                subCountA.nextCollisionAddress += 32;
                            }
                        }
                    }
                    else {
                        // check to copy collision list too
                        outerB: for (long addressB = bHashAddress; addressB != 0; addressB = unsafe.getLong(addressB + 14)) {
                            short cityLength = unsafe.getShort(addressB + 4);
                            long cityNameAddress = unsafe.getLong(addressB + 6);
                            // compare to each city in A slot
                            outerA: for (long aAddress = aHashAddress; aAddress != 0; aAddress = unsafe.getLong(aAddress + 14)) {
                                if (unsafe.getShort(aAddress + 4) == cityLength) {
                                    long aCityNameAddress = unsafe.getLong(aAddress + 6);
                                    int j;
                                    for (j = 0; j < cityLength >> 3 << 3; j += 8) {
                                        if (unsafe.getLong(cityNameAddress + j) != unsafe.getLong(aCityNameAddress + j)) {
                                            // nope, not the same, try next
                                            continue outerA;
                                        }
                                    }
                                    if (j == cityLength ||
                                            (unsafe.getLong(cityNameAddress + j) << ((0x8 - cityLength & 0x7) << 3)) == (unsafe
                                                    .getLong(aCityNameAddress + j) << ((0x8 - cityLength & 0x7) << 3))) {
                                        // found the same city, continue with next city in B slot
                                        continue outerB;
                                    }
                                }
                            }
                            // city not found in A slot, add it. It's a collision too
                            long addressA = aHashAddress;
                            while (unsafe.getLong(addressA + 14) != 0) {
                                addressA = unsafe.getLong(addressA + 14);
                            }
                            unsafe.putLong(addressA + 14, subCountA.nextCollisionAddress);
                            addressA = subCountA.nextCollisionAddress;
                            subCountA.nextCollisionAddress += 32;

                            unsafe.putInt(addressA, subCountA.cityIdCounter++);
                            unsafe.putShort(addressA + 4, cityLength);
                            unsafe.putLong(addressA + 6, cityNameAddress);
                        }
                    }
                }

                int[] cityIdMap = new int[10000];
                for (int i = 0; i < 10000; i++) {
                    cityIdMap[i] = -1;
                }

                for (int i = 0; i < 65536; i++) {
                    long bHashAddress = subCountB.cityHashTableAddress + ((long) i << 5);
                    long aHashAddress = subCountA.cityHashTableAddress + ((long) i << 5);
                    if (unsafe.getShort(aHashAddress + 4) == 0) {
                        continue;
                    }
                    // for each city in A slot
                    outerA: for (long aAddress = aHashAddress; aAddress != 0; aAddress = unsafe.getLong(aAddress + 14)) {
                        short cityLength = unsafe.getShort(aAddress + 4);
                        long cityNameAddress = unsafe.getLong(aAddress + 6);
                        int cityIdA = unsafe.getInt(aAddress);
                        // compare to each city in B slot
                        outer: for (long bAddress = bHashAddress; bAddress != 0; bAddress = unsafe.getLong(bAddress + 14)) {
                            if (unsafe.getShort(bAddress + 4) == cityLength) {
                                long bCityNameAddress = unsafe.getLong(bAddress + 6);
                                int j;
                                for (j = 0; j < cityLength >> 3 << 3; j += 8) {
                                    if (unsafe.getLong(cityNameAddress + j) != unsafe.getLong(bCityNameAddress + j)) {
                                        // nope, not the same, try next
                                        continue outer;
                                    }
                                }
                                if (j == cityLength ||
                                        (unsafe.getLong(cityNameAddress + j) << ((0x8 - cityLength & 0x7) << 3)) == (unsafe
                                                .getLong(bCityNameAddress + j) << ((0x8 - cityLength & 0x7) << 3))) {
                                    cityIdMap[cityIdA] = unsafe.getInt(bAddress);
                                    // found the same city, continue with next city in A slot
                                    continue outerA;
                                }
                            }
                        }
                    }
                }

                for (int i = 0; i < subCountA.cityIdCounter; i++) {
                    int cityId2 = cityIdMap[i];
                    if (cityId2 != -1) {
                        for (int j = 0; j < 2; j++) {
                            for (int k = 0; k < 1000; k++) {
                                unsafe.putInt(subCountA.countsAddress + i * 8000 + j * 4000 + k * 4,
                                        unsafe.getInt(subCountA.countsAddress + i * 8000 + j * 4000 + k * 4) +
                                                unsafe.getInt(subCountB.countsAddress + cityId2 * 8000 + j * 4000 + k * 4));
                            }
                        }
                    }
                }
            }

            var countResult = subCountResults[0];
            var reverseCityIds = new String[10000];
            for (int i = 0; i < 65536; i++) {
                long resultHashAddress = countResult.cityHashTableAddress + ((long) i << 5);
                if (unsafe.getShort(resultHashAddress + 4) != 0) {
                    for (long address = resultHashAddress; address != 0; address = unsafe.getLong(address + 14)) {
                        int cityId = unsafe.getInt(address);
                        int cityLength = unsafe.getShort(address + 4);
                        long cityNameAddress = unsafe.getLong(address + 6);
                        byte[] cityBytes = new byte[cityLength];
                        unsafe.copyMemory(null, cityNameAddress, cityBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, cityLength);
                        reverseCityIds[cityId] = new String(cityBytes, StandardCharsets.UTF_8);
                    }
                }
            }

            // count result as stream
            IntStream.range(0, 10000).parallel().forEach(cityId -> {
                var cityName = reverseCityIds[cityId];
                if (cityName == null) {
                    return;
                }
                var cityAddress = countResult.countsAddress + cityId * 8000;
                var cityResult = new MeasurementAggregator();
                for (int i = 999; i > -1; i--) {
                    if (unsafe.getInt(cityAddress + 4000 + i * 4) > 0) {
                        cityResult.min = -i;
                        break;
                    }
                }
                if (cityResult.min == -1000) {
                    for (int i = 0; i < 1000; i++) {
                        if (unsafe.getInt(cityAddress + i * 4) > 0) {
                            cityResult.min = i;
                            break;
                        }
                    }
                }
                for (int i = 999; i > -1; i--) {
                    if (unsafe.getInt(cityAddress + i * 4) > 0) {
                        cityResult.max = i;
                        break;
                    }
                }
                if (cityResult.max == 1000) {
                    for (int i = 0; i < 1000; i++) {
                        if (unsafe.getInt(cityAddress + 4000 + i * 4) > 0) {
                            cityResult.max = -i;
                            break;
                        }
                    }
                }
                for (int i = 0; i < 1000; i++) {
                    cityResult.sum += ((long) unsafe.getInt(cityAddress + i * 4)) * i;
                    cityResult.sum -= ((long) unsafe.getInt(cityAddress + 4000 + i * 4)) * i;
                    cityResult.count += unsafe.getInt(cityAddress + i * 4);
                    cityResult.count += unsafe.getInt(cityAddress + 4000 + i * 4);
                }
                synchronized (resultMap) {
                    resultMap.put(cityName, cityResult);
                }
            });

            System.out.println(resultMap);
        }
    }
}
