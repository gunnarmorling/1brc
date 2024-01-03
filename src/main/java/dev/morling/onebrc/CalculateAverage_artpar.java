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
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
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
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    public static void main(String[] args) throws IOException {
        long start = Instant.now().toEpochMilli();
        Path measurementFile = Paths.get(FILE);
        OpenOption openOptions = StandardOpenOption.READ;

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

                ReaderRunnable readerRunnable = new ReaderRunnable(mappedByteBuffer, StandardOpenOption.READ,
                        chunkStartPosition, chunkSize);
                Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
                futures.add(future);
                chunkStartPosition = chunkStartPosition + chunkSize + 1;
            }
        }

        Map<String, MeasurementAggregator> globalMap = futures.parallelStream()
                .flatMap(future -> {
                    try {
                        return future.get().entrySet().stream();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (existing, replacement) -> {
                            existing.combine(replacement);
                            return existing;
                        }));

        Map<String, ResultRow> results = globalMap.entrySet().stream()
                .parallel().map(e -> Map.entry(e.getKey(), e.getValue().finish()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        System.out.println(measurements);
        // long end = Instant.now().toEpochMilli();
        // System.out.println((end - start) / 1000);

    }

    public static double sum(double[] array) {
        int i = 0;
        double sum = 0.0;

        // Vectorized loop
        for (; i < SPECIES.loopBound(array.length); i += SPECIES.length()) {
            var v = DoubleVector.fromArray(SPECIES, array, i);
            sum += v.reduceLanes(VectorOperators.ADD);
        }

        // Scalar loop for remaining elements
        for (; i < array.length; i++) {
            sum += array[i];
        }

        return sum;
    }

    private record Measurement(String station, double value) {
    }

    private record ResultRow(double min, double mean, double max, long count) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
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

        void add(Measurement measurement) {
            min = Math.min(min, measurement.value());
            max = Math.max(max, measurement.value());
            sum += measurement.value();
            name = measurement.station;
            count++;
        }

        MeasurementAggregator combine(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        ResultRow finish() {
            double mean = (count > 0) ? sum / count : 0;
            return new ResultRow(min, mean, max, count);
        }
    }

    private static class ReaderRunnable {
        private final MappedByteBuffer mappedByteBuffer;
        private final OpenOption openOptions;
        private final long chunkStartPosition;
        private final long chunkSize;
        private final Map<String, ResultRow> results;
        Map<String, double[]> stationValueMap = new HashMap<>();
        Map<String, Integer> stationIndexMap = new HashMap<>();

        private ReaderRunnable(MappedByteBuffer mappedByteBuffer, OpenOption openOptions, long chunkStartPosition, long chunkSize) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.openOptions = openOptions;
            this.results = new HashMap<>();
            this.chunkStartPosition = chunkStartPosition;
            this.chunkSize = chunkSize;
        }

        public Map<String, MeasurementAggregator> run() {
            long start = Date.from(Instant.now()).getTime();
            int totalBytesRead = 0;
            Map<String, MeasurementAggregator> groupedMeasurements = new HashMap<>();

            final int VECTOR_SIZE = 512;
            final int VECTOR_SIZE_1 = VECTOR_SIZE - 1;
            // ByteBuffer nameBuffer = ByteBuffer.allocate(128);
            byte[] rawBuffer = new byte[128];
            int bufferIndex = 0;
            String matchedStation = "";
            boolean readUntilSemiColon = true;

            while (mappedByteBuffer.hasRemaining()) {
                byte b = mappedByteBuffer.get();
                totalBytesRead++;
                if (readUntilSemiColon) {
                    if (b != ';') {
                        rawBuffer[bufferIndex] = b;
                        bufferIndex++;
                        continue;
                    }
                    else {
                        readUntilSemiColon = false;
                        matchedStation = new String(rawBuffer, 0, bufferIndex, StandardCharsets.UTF_8);
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
                    String tempValue = getTempStringFromBufferUsingBuffer(rawBuffer, bufferIndex);
                    bufferIndex = 0;

                    int tempValueHashCode = tempValue.hashCode();
                    if (!hashToDouble.containsKey(tempValueHashCode)) {
                        hashToDouble.put(tempValueHashCode, Double.parseDouble(tempValue));
                    }
                    double doubleValue = hashToDouble.get(tempValueHashCode);

                    // Measurement measurement = new Measurement(matchedStation, doubleValue);
                    double[] array = stationValueMap.computeIfAbsent(matchedStation, (k) -> {
                        stationIndexMap.put(k, 0);
                        return new double[VECTOR_SIZE];
                    });
                    Integer index = stationIndexMap.get(matchedStation);
                    array[index] = doubleValue;
                    if (index == VECTOR_SIZE_1) {

                        int i = 0;
                        double min = Double.POSITIVE_INFINITY;
                        double max = Double.NEGATIVE_INFINITY;
                        double sum = 0;
                        long count = 0;
                        for (; i < SPECIES.loopBound(array.length); i += SPECIES.length()) {
                            // Vector operations
                            DoubleVector vector = DoubleVector.fromArray(SPECIES, array, i);
                            min = Math.min(min, vector.reduceLanes(VectorOperators.MIN));
                            max = Math.max(max, vector.reduceLanes(VectorOperators.MAX));
                            sum += vector.reduceLanes(VectorOperators.ADD);
                            count += vector.length();
                        }

                        // MeasurementAggregator ma = new MeasurementAggregator(min, max, sum, VECTOR_SIZE);
                        // groupedMeasurements.computeIfAbsent(matchedStation, k -> new MeasurementAggregator())
                        // .combine(ma);

                        // int remainingCount = array.length - i;
                        for (; i < array.length; i++) {
                            min = Math.min(min, array[i]);
                            max = Math.max(max, array[i]);
                            sum += array[i];
                            count++;
                        }
                        MeasurementAggregator ma = new MeasurementAggregator(min, max, sum, count);
                        // System.out.println("Sum ma [" + ma + "]");
                        groupedMeasurements.computeIfAbsent(matchedStation, k -> new MeasurementAggregator())
                                .combine(ma);

                        stationIndexMap.put(matchedStation, 0);
                        continue;
                    }
                    stationIndexMap.put(matchedStation, index + 1);
                }
            }

            VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
            for (String stationName : stationIndexMap.keySet()) {

                Integer count = stationIndexMap.get(stationName);
                if (count < 1) {
                    continue;
                }
                else if (count == 1) {
                    double[] array = stationValueMap.get(stationName);
                    double val = array[0];
                    MeasurementAggregator ma = new MeasurementAggregator(val, val, val, 1);
                    groupedMeasurements.computeIfAbsent(stationName, k -> new MeasurementAggregator())
                            .combine(ma);
                }
                else {
                    double[] array = stationValueMap.get(stationName);
                    double[] subArray = new double[count];
                    System.arraycopy(array, 0, subArray, 0, count);
                    // Creating a DoubleVector from the array
                    // System.out.println("Create vector from [" + count + "] -> " + subArray.length);
                    DoubleVector doubleVector = DoubleVector.fromArray(species, subArray, 0);
                    double min = doubleVector.reduceLanes(VectorOperators.MIN);
                    double max = doubleVector.reduceLanes(VectorOperators.MAX);
                    double sum = doubleVector.reduceLanes(VectorOperators.ADD);
                    MeasurementAggregator ma = new MeasurementAggregator(min, max, sum, count);
                    groupedMeasurements.computeIfAbsent(stationName, k -> new MeasurementAggregator())
                            .combine(ma);
                }

                stationIndexMap.put(matchedStation, 0);
            }

            long end = Date.from(Instant.now()).getTime();
            // System.out.println("Took [" + ((end - start) / 1000) + "s for " + totalBytesRead / 1024 + " kb");

            return groupedMeasurements;
        }


        private int hashCode(byte[] array, int length) {
            if (array == null) {
                return 0;
            }

            int result = 1;
            for (int i = 0; i < length; i++) {
                result = 31 * result + array[i];
            }

            return result;
        }

        private String getTempStringFromBufferUsingBuffer(byte[] array, int length) {

            int byteArrayHashCode = hashCode(array, length);
            if (!tempStringMap.containsKey(byteArrayHashCode)) {
                String value = new String(array, 0, length, StandardCharsets.UTF_8);
                tempStringMap.put(byteArrayHashCode, value);
                return value;
            }

            return tempStringMap.get(byteArrayHashCode);
        }
    }
}
