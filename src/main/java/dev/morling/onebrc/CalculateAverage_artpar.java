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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_artpar {

    public static final int N_THREADS = 8;
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {

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

                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStartPosition,
                        chunkSize);
                ReaderRunnable readerRunnable = new ReaderRunnable(mappedByteBuffer, StandardOpenOption.READ,
                        chunkStartPosition, chunkSize);
                Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
                futures.add(future);
            }
        }

        Map<String, MeasurementAggregator> globalMap = new HashMap<>();
        for (Future<Map<String, MeasurementAggregator>> future : futures) {
            try {
                Map<String, MeasurementAggregator> stringMeasurementAggregatorMap = future.get();
                for (Map.Entry<String, MeasurementAggregator> entry : stringMeasurementAggregatorMap.entrySet()) {
                    if (globalMap.containsKey(entry.getKey())) {
                        MeasurementAggregator value = entry.getValue();
                        globalMap.get(entry.getKey()).combine(value);
                    }
                    else {
                        globalMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        Map<String, ResultRow> results = globalMap.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().finish()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        System.out.println(measurements);
    }

    private record Measurement(String station, double value) {
    }

    ;

    private record ResultRow(double min, double mean, double max) {
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

        void add(Measurement measurement) {
            min = Math.min(min, measurement.value());
            max = Math.max(max, measurement.value());
            sum += measurement.value();
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
            return new ResultRow(min, mean, max);
        }
    }

    private static class ReaderRunnable {
        private final MappedByteBuffer mappedByteBuffer;
        private final OpenOption openOptions;
        private final long chunkStartPosition;
        private final long chunkSize;
        private final Map<String, ResultRow> results;
        private Map<Integer, String> nameStringMap = new HashMap(1024 * 1024);
        private Map<Integer, String> tempStringMap = new HashMap(1024 * 1024);
        private Map<Integer, Double> hashToDouble = new HashMap<>(1024 * 1024);

        private ReaderRunnable(MappedByteBuffer mappedByteBuffer, OpenOption openOptions, long chunkStartPosition, long chunkSize) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.openOptions = openOptions;
            this.results = new HashMap<>();
            this.chunkStartPosition = chunkStartPosition;
            this.chunkSize = chunkSize;
        }

        public Map<String, MeasurementAggregator> run() {
            long start = Date.from(Instant.now()).getTime();
            SeekableByteChannel fileChannel = null;
            int totalBytesRead = 0;
            Map<String, MeasurementAggregator> groupedMeasurements = new HashMap<>();

            ByteBuffer nameBuffer = ByteBuffer.allocate(128);
            String matchedStation = "";
            boolean readUntilSemiColon = true;

            while (mappedByteBuffer.hasRemaining()) {
                byte b = mappedByteBuffer.get();

                if (readUntilSemiColon) {
                    if ((byte) b != ';') {
                        nameBuffer.put(b);
                        continue;
                    }
                    else {
                        readUntilSemiColon = false;
                        matchedStation = getNameStringFromBufferUsingBuffer(nameBuffer);
                        continue;
                    }
                }

                if (b != '\n') {
                    nameBuffer.put(b);
                }
                else {
                    readUntilSemiColon = true;
                    String tempValue = getTempStringFromBufferUsingBuffer(nameBuffer);

                    int tempValueHashCode = tempValue.hashCode();
                    if (!hashToDouble.containsKey(tempValueHashCode)) {
                        hashToDouble.put(tempValueHashCode, Double.parseDouble(tempValue));
                    }
                    Double doubleValue = hashToDouble.get(tempValueHashCode);

                    Measurement measurement = new Measurement(matchedStation, doubleValue);
                    groupedMeasurements.computeIfAbsent(measurement.station(), k -> new MeasurementAggregator())
                            .add(measurement);
                }
            }

            long end = Date.from(Instant.now()).getTime();
            System.out.println("Took [" + ((end - start) / 1000) + "s for " + totalBytesRead / 1024 + " kb");

            return groupedMeasurements;
        }

        private String getNameStringFromBufferUsingBuffer(ByteBuffer nameBuffer) {
            nameBuffer.flip();

            byte[] array = nameBuffer.array();
            int length = nameBuffer.limit();
            int byteArrayHashCode = hashCode(array, 0, length);
            nameBuffer.flip();
            nameBuffer.clear();

            if (!nameStringMap.containsKey(byteArrayHashCode)) {
                String value = new String(array, 0, length, StandardCharsets.UTF_8);
                nameStringMap.put(byteArrayHashCode, value);
                return value;
            }

            return nameStringMap.get(byteArrayHashCode);
        }

        private int hashCode(byte[] array, int start, int length) {
            if (array == null) {
                return 0;
            }

            int result = 1;
            for (int i = start; i < start + length; i++) {
                result = 31 * result + array[i];
            }

            return result;
        }

        private String getTempStringFromBufferUsingBuffer(ByteBuffer nameBuffer) {
            nameBuffer.flip();

            byte[] array = nameBuffer.array();
            int length = nameBuffer.limit();
            int byteArrayHashCode = hashCode(array, 0, length);
            nameBuffer.flip();
            nameBuffer.clear();

            if (!tempStringMap.containsKey(byteArrayHashCode)) {
                String value = new String(array, 0, length, StandardCharsets.UTF_8);
                tempStringMap.put(byteArrayHashCode, value);
                return value;
            }

            return tempStringMap.get(byteArrayHashCode);
        }
    }
}
