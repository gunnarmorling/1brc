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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_ericxiao {

    private static final String FILE = "./measurements.txt";

    private static class Station {
        private int min;
        private int max;
        private long sum;
        private int count;

        private Station(int temp) {
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        public void setMeasurement(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public void mergeStation(Station station) {
            this.min = Math.min(this.min, station.min);
            this.max = Math.max(this.max, station.max);
            this.sum += station.sum;
            this.count += station.count;
        }

        public String toString() {
            return round(min / 10.0) + "/" + round((double) this.sum / this.count / 10.0) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

    }

    static class ProcessFileMap implements Callable<Map<String, double[]>> {
        private long readStart;
        private long readEnd;
        private boolean lastRead;
        private boolean firstRead;
        byte[] keyBytes = new byte[100];
        byte[] valueBytes = new byte[100];

        private static final Unsafe UNSAFE = initUnsafe();

        public ProcessFileMap(long readStart, long readEnd, boolean firstRead, boolean lastRead) {
            this.readStart = readStart;
            this.readEnd = readEnd;
            this.lastRead = lastRead;
            this.firstRead = firstRead;
        }

        private final HashMap<String, double[]> hashMap = new HashMap<>();

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

        public void add(long keyStart, long keyEnd, long valueEnd) {
            int length = (int) (keyEnd - keyStart);
            UNSAFE.copyMemory(null, keyStart, keyBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
            String key = new String(keyBytes, 0, length, StandardCharsets.UTF_8);
            long valueStart = keyEnd + 1;
            length = (int) (valueEnd - valueStart);
            UNSAFE.copyMemory(null, valueStart, valueBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
            double value = Double.parseDouble(new String(valueBytes, 0, length, StandardCharsets.UTF_8));

            hashMap.compute(key, (_, v) -> {
                if (v == null) {
                    return new double[]{ value, value, value, 1 };
                }
                else {
                    v[0] = Math.min(v[0], value);
                    v[1] = Math.max(v[1], value);
                    v[2] = v[2] + value;
                    v[3] = v[3] + 1;
                    return v;
                }
            });
        }

        private static long delimiterMask(long word, long delimiter) {
            long mask = word ^ delimiter;
            return (mask - 0x0101010101010101L) & (~mask & 0x8080808080808080L);
        }

        public Map<String, double[]> call() {
            return readMemory(readStart, readEnd);
        }

        private Map<String, double[]> readMemory(long startAddress, long endAddress) {
            int packedBytes = 0;
            final long singleSemiColonPattern = 0x3BL;
            final long semiColonPattern = 0x3B3B3B3B3B3B3B3BL;
            final long singleNewLinePattern = 0x0AL;
            final long newLinePattern = 0x0A0A0A0A0A0A0A0AL;
            long keyEndAddress;
            long valueEndAddress;

            long word;
            long mask;

            long byteStart = startAddress;

            // TODO: We might need to consider memory alignment here. we need to be on a 8 byte boundary.

            // We need to skip to the first \n so that we skip partial data. The partial data will be picked up by the previous thread.
            if (!firstRead) {
                while (UNSAFE.getByte(byteStart++) != '\n')
                    ;
            }

            long keyStartAddress = byteStart;

            byteStart = (byteStart + 7) & ~7;

            final int vectorLoops = (int) (endAddress - byteStart) / 8;

            word = UNSAFE.getLong(byteStart);
            packedBytes += 1;

            while (true) {
                mask = delimiterMask(word, semiColonPattern);
                while (mask == 0 && packedBytes < vectorLoops) {
                    packedBytes += 1;
                    byteStart += 8;
                    word = UNSAFE.getLong(byteStart);
                    mask = delimiterMask(word, semiColonPattern);
                }

                if (packedBytes == vectorLoops)
                    break;

                keyEndAddress = byteStart + (Long.numberOfTrailingZeros(mask) / 8);

                // Once we find the semicolon we remove it from the word
                // so that we can find multiple semicolons in the same word.
                word ^= singleSemiColonPattern << (Long.numberOfTrailingZeros(mask) + 1 - 8);

                // The new line pattern could be located in the same byte we found the key in.
                // so we need to check if the value is in this byte.
                mask = delimiterMask(word, newLinePattern);

                while (mask == 0 && packedBytes < vectorLoops) {
                    packedBytes += 1;
                    byteStart += 8;
                    word = UNSAFE.getLong(byteStart);
                    mask = delimiterMask(word, newLinePattern);
                }

                if (packedBytes == vectorLoops)
                    break;

                valueEndAddress = byteStart + (Long.numberOfTrailingZeros(mask) / 8);
                add(keyStartAddress, keyEndAddress, valueEndAddress);
                keyStartAddress = valueEndAddress + 1;

                // TODO: We might be able to do better here by using popcount on the mask
                // and then shifting the mask till it is zero.

                // Same as before, we remove the newline character so we don't match it again.
                word ^= singleNewLinePattern << (Long.numberOfTrailingZeros(mask) + 1 - 8);
            }

            // We do scalar reads here for the remaining values.
            byteStart = keyStartAddress;

            while (byteStart < endAddress) {
                byte value = UNSAFE.getByte(byteStart);
                if (value == ';') {
                    keyEndAddress = byteStart;
                    while (UNSAFE.getByte(++byteStart) != '\n')
                        ;
                    valueEndAddress = byteStart;
                    add(keyStartAddress, keyEndAddress, valueEndAddress);
                    keyStartAddress = valueEndAddress + 1;
                }
                else {
                    byteStart++;
                }
            }

            // we need to do one more read here so that we overlap with the next chunk.
            if (!lastRead) {
                byteStart = keyStartAddress;
                while (UNSAFE.getByte(++byteStart) != ';')
                    ;
                keyEndAddress = byteStart;
                while (UNSAFE.getByte(++byteStart) != '\n')
                    ;
                valueEndAddress = byteStart;
                add(keyStartAddress, keyEndAddress, valueEndAddress);
            }

            return hashMap;
        }
    }

    public static void main(String[] args) throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors() - 1; // Use the number of available processors
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Callable<Map<String, double[]>>> callableTasks = new ArrayList<>();
        Path filePath = Path.of(FILE);

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath, EnumSet.of(StandardOpenOption.READ))) {
            MemorySegment fileMap = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());

            long readStart = fileMap.address();
            long fileSize = fileChannel.size();
            long readLength = (fileSize / numThreads);

            callableTasks.add(new ProcessFileMap(readStart, readStart + readLength, true, false));
            readStart += readLength;

            for (int i = 1; i < numThreads - 1; ++i) {
                ProcessFileMap callableTask = new ProcessFileMap(readStart, readStart + readLength, false, false);
                readStart += readLength;
                callableTasks.add(callableTask);
            }

            callableTasks.add(new ProcessFileMap(readStart, readStart + readLength, false, true));

            List<Map<String, double[]>> results = new ArrayList<>();
            try {
                List<Future<Map<String, double[]>>> futures = executorService.invokeAll(callableTasks);
                for (Future<Map<String, double[]>> future : futures) {
                    try {
                        results.add(future.get());
                    }
                    catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                executorService.shutdown();
                // fileChannel.close();
                Map<String, double[]> mapA = results.getFirst();
                for (int i = 1; i < numThreads; ++i) {
                    results.get(i).forEach((station, stationMeasurements) -> {
                        if (mapA.containsKey(station)) {
                            double[] measurements = mapA.get(station);
                            measurements[0] = Math.min(measurements[0], stationMeasurements[0]);
                            measurements[1] = Math.max(measurements[1], stationMeasurements[1]);
                            measurements[2] = measurements[2] + stationMeasurements[2];
                            measurements[3] = measurements[3] + stationMeasurements[3];
                        }
                        else {
                            mapA.put(station, stationMeasurements);
                        }
                    });
                }
                // print key and values
                int counter = 1;
                System.out.println("{");
                for (Map.Entry<String, double[]> entry : mapA.entrySet()) {
                    double[] measurements = entry.getValue();
                    System.out.print(entry.getKey() + "=" + measurements[0] + "/" + String.format("%.1f", measurements[2] / measurements[3]) + "/" + measurements[1]);
                    if (counter++ < mapA.size())
                        System.out.print(", ");
                }
                System.out.print("}");
            }
        }
    }
}