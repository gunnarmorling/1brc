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

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_artpar {
    public static final int N_THREADS = 8;
    private static final String FILE = "./measurements.txt";
    private static final Map<Integer, String> nameStringMap = new ConcurrentHashMap<>(1024 * 1024);
    private static final Map<Integer, String> tempStringMap = new ConcurrentHashMap<>(1024 * 1024);
    private static final Map<Integer, Double> hashToDouble = new ConcurrentHashMap<>(1024 * 1024);
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_MAX;
    final int VECTOR_SIZE = 512;
    final int VECTOR_SIZE_1 = VECTOR_SIZE - 1;
    final int SIZE = 1024 * 128;

    public CalculateAverage_artpar() throws IOException {
        long start = Instant.now().toEpochMilli();
        Path measurementFile = Paths.get(FILE);
        long fileSize = Files.size(measurementFile);

        long expectedChunkSize = fileSize / N_THREADS;

        ExecutorService threadPool = Executors.newFixedThreadPool(N_THREADS);

        long chunkStartPosition = 0;
        FileInputStream fis = new FileInputStream(measurementFile.toFile());
        List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
        long bytesReadCurrent = 0;

        try (FileChannel fileChannel = FileChannel.open(measurementFile, StandardOpenOption.READ)) {
            for (int i = 0; i < N_THREADS; i++) {

                long chunkSize = expectedChunkSize;
                chunkSize = fis.skip(chunkSize);

                bytesReadCurrent += chunkSize;
                while (((char) fis.read()) != '\n' && bytesReadCurrent < fileSize) {
                    chunkSize++;
                    bytesReadCurrent++;
                }

                // System.out.println("[" + chunkStartPosition + "] - [" + (chunkStartPosition + chunkSize) + " bytes");
                if (chunkStartPosition + chunkSize >= fileSize) {
                    chunkSize = fileSize - chunkStartPosition;
                }

                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStartPosition,
                        chunkSize);

                ReaderRunnable readerRunnable = new ReaderRunnable(mappedByteBuffer,
                        chunkStartPosition, chunkSize);
                Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
                futures.add(future);
                chunkStartPosition = chunkStartPosition + chunkSize + 1;
            }
        }
        fis.close();

        Map<String, MeasurementAggregator> globalMap = futures.parallelStream()
                .flatMap(future -> {
                    try {
                        return future.get().entrySet().stream();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toConcurrentMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        MeasurementAggregator::combine));

        Map<String, ResultRow> results = globalMap.entrySet().stream().parallel()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().finish()));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        System.out.println(measurements);
        // long end = Instant.now().toEpochMilli();
        // System.out.println((end - start) / 1000);

    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_artpar();
    }

    public static int parseDouble(byte[] str, int length) {

        boolean negative = false;

        int start = 0;
        int decimalIndex = -1;
        int result = 0;

        // Check for negative numbers
        if (str[0] == '-') {
            negative = true;
            start++;
        }

        // Parse each character
        for (int i = start; i < length; i++) {
            byte c = str[i];

            if (c == '.') {
                continue;
            }
            else {
                result = result * 10 + (c - '0');
            }
        }

        // Adjust for the decimal point
        // if (decimalIndex != -1) {
        // result /= 10;
        // }

        return negative ? -result : result;
    }

    public static int hashCode(byte[] array, int length) {
        if (array == null || length == 0) {
            return 0;
        }

        int result = 1;
        int i = 0;

        // Loop unrolling for every 4 elements
        for (; i <= length - 4; i += 4) {
            result = result
                    + (array[i] * 31 * 31 * 31)
                    + (array[i + 1] * 31 * 31)
                    + (array[i + 2] * 31)
                    + array[i + 3];
        }

        // Process remaining elements
        for (; i < length; i++) {
            result = result * 31 + array[i];
        }

        return result;
    }

    private record Measurement(String station, double value) {
    }

    private record ResultRow(double min, double mean, double max, long count, double sum) {
        public String toString() {
            return round(min / 10) + "/" + round(mean / 10) + "/" + round(max / 10);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
        private String name;

        public MeasurementAggregator() {
        }

        public MeasurementAggregator(double min, double max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        synchronized MeasurementAggregator combine(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        synchronized MeasurementAggregator combine(double otherMin, double otherMax, double otherSum, long otherCount) {
            min = Math.min(min, otherMin);
            max = Math.max(max, otherMax);
            sum += otherSum;
            count += otherCount;
            return this;
        }

        ResultRow finish() {
            double mean = (count > 0) ? sum / count : 0;
            return new ResultRow(min, mean, max, count, sum);
        }
    }

    private class ReaderRunnable {
        private final MappedByteBuffer mappedByteBuffer;
        private final long chunkStartPosition;
        private final long chunkSize;
        StationNameMap stationNameMap = new StationNameMap();
        // double[][] stationValueMap = new double[SIZE][];

        private ReaderRunnable(MappedByteBuffer mappedByteBuffer, long chunkStartPosition, long chunkSize) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.chunkStartPosition = chunkStartPosition;
            this.chunkSize = chunkSize;
        }

        public Map<String, MeasurementAggregator> run() {
            long start = Date.from(Instant.now()).getTime();
            int totalBytesRead = 0;

            // ByteBuffer nameBuffer = ByteBuffer.allocate(128);
            byte[] rawBuffer = new byte[128];
            int bufferIndex = 0;
            StationName matchedStation = null;
            boolean readUntilSemiColon = true;

            int MAPPED_BYTE_BUFFER_SIZE = 1024;
            byte[] mappedBytes = new byte[MAPPED_BYTE_BUFFER_SIZE];
            int i1;
            while (mappedByteBuffer.hasRemaining()) {
                int remaining = mappedByteBuffer.remaining();
                mappedByteBuffer.get(mappedBytes, 0, Math.min(remaining, MAPPED_BYTE_BUFFER_SIZE));
                i1 = 0;
                while (i1 < remaining && i1 < MAPPED_BYTE_BUFFER_SIZE) {
                    byte b = mappedBytes[i1];
                    i1++;
                    totalBytesRead++;
                    if (readUntilSemiColon) {
                        if (b != ';') {
                            rawBuffer[bufferIndex] = b;
                            bufferIndex++;
                            continue;
                        }
                        else {
                            readUntilSemiColon = false;
                            // Integer bufferHash = hashCode(rawBuffer, bufferIndex);
                            // int finalBufferIndex = bufferIndex;
                            matchedStation = stationNameMap.getOrCreate(rawBuffer, bufferIndex);
                            // matchedStation = new String(rawBuffer, 0, bufferIndex, StandardCharsets.UTF_8);
                            bufferIndex = 0;
                            continue;
                        }
                    }

                    if (b != '\n') {
                        rawBuffer[bufferIndex] = b;
                        bufferIndex++;
                    }
                    else {
                        readUntilSemiColon = true;
                        // String tempValue = new String(rawBuffer, 0, bufferIndex, StandardCharsets.UTF_8);

                        // int tempValueHashCode = tempValue.hashCode();
                        // if (!hashToDouble.containsKey(tempValueHashCode)) {
                        // hashToDouble.put(tempValueHashCode, parseDouble(tempValue));
                        // }
                        int doubleValue = parseDouble(rawBuffer, bufferIndex);
                        bufferIndex = 0;

                        // Measurement measurement = new Measurement(matchedStation, doubleValue);
                        int[] array = matchedStation.values;
                        int index = matchedStation.count;
                        array[index] = doubleValue;
                        if (index == VECTOR_SIZE_1) {

                            int i = 0;
                            double min = Double.POSITIVE_INFINITY;
                            double max = Double.NEGATIVE_INFINITY;
                            double sum = 0;
                            long count = 0;
                            for (; i < SPECIES.loopBound(array.length); i += SPECIES.length()) {
                                // Vector operations
                                IntVector vector = IntVector.fromArray(SPECIES, array, i);
                                min = Math.min(min, vector.reduceLanes(VectorOperators.MIN));
                                max = Math.max(max, vector.reduceLanes(VectorOperators.MAX));
                                sum += vector.reduceLanes(VectorOperators.ADD);
                                count += vector.length();
                            }

                            for (; i < array.length; i++) {
                                min = Math.min(min, array[i]);
                                max = Math.max(max, array[i]);
                                sum += array[i];
                                count++;
                            }

                            matchedStation.measurementAggregator.combine(min, max, sum, count);

                            matchedStation.count = 0;
                            continue;
                        }
                        matchedStation.count++;
                    }
                }

            }

            VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;

            Arrays.stream(stationNameMap.names)
                    .filter(Objects::nonNull)
                    .parallel()
                    .forEach(stationName -> {
                        int count = stationName.count;
                        if (count < 1) {
                            return;
                        }
                        else if (count == 1) {
                            int[] array = stationName.values;
                            double val = array[0];
                            MeasurementAggregator ma = new MeasurementAggregator(val, val, val, 1);
                            stationName.measurementAggregator.combine(ma);
                        }
                        else {
                            int[] array = stationName.values;
                            int[] subArray = new int[count];
                            System.arraycopy(array, 0, subArray, 0, count);
                            // Creating a DoubleVector from the array
                            // System.out.println("Create vector from [" + count + "] -> " + subArray.length);

                            int i = 0;
                            double min = Double.POSITIVE_INFINITY;
                            double max = Double.NEGATIVE_INFINITY;
                            double sum = 0;
                            long subCount = 0;

                            for (; i < SPECIES.loopBound(subArray.length); i += SPECIES.length()) {
                                // Vector operations
                                IntVector vector = IntVector.fromArray(SPECIES, subArray, i);
                                min = Math.min(min, vector.reduceLanes(VectorOperators.MIN));
                                max = Math.max(max, vector.reduceLanes(VectorOperators.MAX));
                                sum += vector.reduceLanes(VectorOperators.ADD);
                                subCount += vector.length();
                            }

                            for (; i < subArray.length; i++) {
                                min = Math.min(min, subArray[i]);
                                max = Math.max(max, subArray[i]);
                                sum += subArray[i];
                                subCount++;
                            }

                            MeasurementAggregator ma = new MeasurementAggregator(min, max, sum, subCount);
                            stationName.measurementAggregator.combine(ma);
                        }

                        stationName.count = 0;
                    });

            // for (StationName stationName : stationNameMap.names) {
            // if (stationName == null) {
            // continue;
            // }
            //
            //
            // }

            long end = Date.from(Instant.now()).getTime();
            // System.out.println("Took [" + ((end - start) / 1000) + "s for " + totalBytesRead / 1024 + " kb");

            return Arrays.stream(stationNameMap.names)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(e -> e.name, e -> e.measurementAggregator));
            // return groupedMeasurements;
        }

        private String getTempStringFromBufferUsingBuffer(byte[] array, int length) {

            int byteArrayHashCode = CalculateAverage_artpar.hashCode(array, length);
            if (!tempStringMap.containsKey(byteArrayHashCode)) {
                String value = new String(array, 0, length, StandardCharsets.UTF_8);
                tempStringMap.put(byteArrayHashCode, value);
                return value;
            }

            return tempStringMap.get(byteArrayHashCode);
        }
    }

    class StationName {
        public final int hash;
        private final String name;
        private final int index;
        public int count = 0;
        public int[] values = new int[VECTOR_SIZE];
        public MeasurementAggregator measurementAggregator = new MeasurementAggregator();

        public StationName(String name, int index, int hash) {
            this.name = name;
            this.index = index;
            this.hash = hash;
        }

    }

    class StationNameMap {
        int[] indexes = new int[SIZE];
        StationName[] names = new StationName[SIZE];
        int currentIndex = 0;

        public StationName getOrCreate(byte[] stationNameBytes, int length) {
            int hash = CalculateAverage_artpar.hashCode(stationNameBytes, length);
            int position = Math.abs(hash) % SIZE;
            while (indexes[position] != 0 && names[indexes[position]].hash != hash) {
                position = ++position % SIZE;
            }
            if (indexes[position] != 0) {
                return names[indexes[position]];
            }
            while (true) {
                synchronized (this) {
                    StationName stationName = new StationName(
                            new String(stationNameBytes, 0, length, StandardCharsets.UTF_8), position, hash);
                    if (indexes[position] != 0) {
                        continue;
                    }
                    indexes[position] = ++currentIndex;
                    names[indexes[position]] = stationName;
                    return stationName;
                }
            }
        }
    }

}
