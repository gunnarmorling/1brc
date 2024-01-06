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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    // final int VECTOR_SIZE = 512;
    // final int VECTOR_SIZE_1 = VECTOR_SIZE - 1;
    final int AVERAGE_CHUNK_SIZE = 1024 * 1024;
    final int AVERAGE_CHUNK_SIZE_1 = AVERAGE_CHUNK_SIZE - 1;

    public CalculateAverage_artpar() throws IOException {
        long start = Instant.now().toEpochMilli();
        Path measurementFile = Paths.get(FILE);
        long fileSize = Files.size(measurementFile);

        // System.out.println("File size - " + fileSize);
        int expectedChunkSize = Math.toIntExact(Math.max(fileSize / N_THREADS, Integer.MAX_VALUE / 4));

        ExecutorService threadPool = Executors.newFixedThreadPool(N_THREADS);

        long chunkStartPosition = 0;
        RandomAccessFile fis = new RandomAccessFile(measurementFile.toFile(), "r");
        List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
        long bytesReadCurrent = 0;

        FileChannel fileChannel = FileChannel.open(measurementFile, StandardOpenOption.READ);
        for (int i = 0; chunkStartPosition < fileSize; i++) {

            int chunkSize = expectedChunkSize;
            chunkSize = fis.skipBytes(chunkSize);

            bytesReadCurrent += chunkSize;
            while (((char) fis.read()) != '\n' && bytesReadCurrent < fileSize) {
                chunkSize++;
                bytesReadCurrent++;
            }

            // System.out.println("[" + chunkStartPosition + "] - [" + (chunkStartPosition + chunkSize) + " bytes");
            if (chunkStartPosition + chunkSize >= fileSize) {
                chunkSize = (int) Math.min(fileSize - chunkStartPosition, Integer.MAX_VALUE);
            }
            if (chunkSize < 1) {
                break;
            }
            if (chunkSize >= Integer.MAX_VALUE) {
                chunkSize = Integer.MAX_VALUE / 8;
            }

            // MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStartPosition,
            // chunkSize);

            ReaderRunnable readerRunnable = new ReaderRunnable(chunkStartPosition, chunkSize, fileChannel);
            Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
            // System.out.println("Added future [" + chunkStartPosition + "][" + chunkSize + "]");
            futures.add(future);
            chunkStartPosition = chunkStartPosition + chunkSize + 1;
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
                }).parallel().collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue,
                        MeasurementAggregator::combine));
        fileChannel.close();

        Map<String, ResultRow> results = globalMap.entrySet().stream().parallel()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().finish()));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        PrintStream printStream = new PrintStream(new BufferedOutputStream(System.out));
        // PrintStream printStream = System.out;
        printStream.print("{");

        boolean isFirst = true;
        for (Map.Entry<String, ResultRow> stringResultRowEntry : measurements.entrySet()) {
            if (!isFirst) {
                printStream.print(", ");
                printStream.flush();
            }
            printStream.flush();
            printStream.print(stringResultRowEntry.getKey());
            printStream.flush();
            printStream.print("=");
            printStream.flush();
            stringResultRowEntry.getValue().printTo(printStream);
            printStream.flush();
            isFirst = false;
        }

        System.out.print("}\n");

        // long end = Instant.now().toEpochMilli();
        // System.out.println((end - start) / 1000);

    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_artpar();
    }

    public static int hashCode(byte[] array, int length) {

        int h = 1;
        int i = 0;
        for (; i + 7 < length; i += 8) {
            h = 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * h + 31 * 31 * 31 * 31
                    * 31 * 31 * 31 * array[i] + 31 * 31 * 31 * 31 * 31 * 31
                            * array[i + 1]
                    + 31 * 31 * 31 * 31 * 31 * array[i + 2] + 31
                            * 31 * 31 * 31 * array[i + 3]
                    + 31 * 31 * 31 * array[i + 4]
                    + 31 * 31 * array[i + 5] + 31 * array[i + 6] + array[i + 7];
        }

        for (; i + 3 < length; i += 4) {
            h = 31 * 31 * 31 * 31 * h + 31 * 31 * 31 * array[i] + 31 * 31
                    * array[i + 1] + 31 * array[i + 2] + array[i + 3];
        }
        for (; i < length; i++) {
            h = 31 * h + array[i];
        }

        return h;
    }

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min / 10) + "/" + round(mean / 10) + "/" + round(max / 10);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public void printTo(PrintStream out) {
            out.printf("%.1f/%.1f/%.1f", min / 10, mean / 10, max / 10);
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator() {
        }

        // public MeasurementAggregator(double min, double max, double sum, long count) {
        // this.min = min;
        // this.max = max;
        // this.sum = sum;
        // this.count = count;
        // }

        MeasurementAggregator combine(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        // MeasurementAggregator combine(double otherMin, double otherMax, double otherSum, long otherCount) {
        // min = Math.min(min, otherMin);
        // max = Math.max(max, otherMax);
        // sum += otherSum;
        // count += otherCount;
        // return this;
        // }

        MeasurementAggregator combine(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count += 1;
            return this;
        }

        ResultRow finish() {
            double mean = (count > 0) ? sum / count : 0;
            return new ResultRow(min, mean, max);
        }
    }

    static class StationName {
        public final int hash;
        private final byte[] nameBytes;
        // private final int index;
        public int count = 0;
        // public int[] values = new int[VECTOR_SIZE];
        public MeasurementAggregator measurementAggregator = new MeasurementAggregator();

        public StationName(byte[] nameBytes, int hash) {
            this.nameBytes = nameBytes;
            // this.index = index;
            this.hash = hash;
        }

    }

    private class ReaderRunnable {
        private final long startPosition;
        private final int chunkSize;
        private final FileChannel fileChannel;
        StationNameMap stationNameMap = new StationNameMap();

        private ReaderRunnable(long startPosition, int chunkSize, FileChannel fileChannel) {
            this.startPosition = startPosition;
            this.chunkSize = chunkSize;
            this.fileChannel = fileChannel;
        }

        public Map<String, MeasurementAggregator> run() throws IOException {
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition,
                    chunkSize);

            byte[] rawBuffer = new byte[100];
            int rawBufferReadIndex = 0;
            boolean negative;
            int result;
            int position = 0;
            byte b;
            int hash;
            while (position < chunkSize) {
                hash = 0;
                while ((rawBuffer[rawBufferReadIndex++] = mappedByteBuffer.get(position++)) != ';') {
                    hash = hash * 31 + rawBuffer[rawBufferReadIndex - 1];
                }

                negative = false;
                result = 0;

                b = mappedByteBuffer.get(position++);
                if (b == '-') {
                    byte one = mappedByteBuffer.get(position);
                    byte two = mappedByteBuffer.get(position + 1);
                    byte three = mappedByteBuffer.get(position + 2);
                    if (two == '.') {
                        result = -1 * (((one - '0') * 10 + (three - '0')));
                        position = position + 4;
                    }
                    else if (three == '.') {
                        result = -1 * ((((one - '0') * 10 * 10) + ((two - '0') * 10) + mappedByteBuffer.get(
                                position + 3)) - '0');
                        position = position + 5;
                    }
                }
                else {
                    byte two = mappedByteBuffer.get(position);
                    byte three = mappedByteBuffer.get(position + 1);
                    if (two == '.') {
                        result = (b - '0') * 10 + (three - '0');
                        position = position + 3;
                    }
                    else if (three == '.') {
                        result = ((b - '0') * 10 * 10) + ((two - '0') * 10) + mappedByteBuffer.get(position + 2) - '0';
                        position = position + 4;
                    }

                }

                stationNameMap.getOrCreate(rawBuffer, rawBufferReadIndex - 1, negative ? -result : result, hash);
                rawBufferReadIndex = 0;
            }
            return Arrays.stream(stationNameMap.names).parallel().filter(Objects::nonNull)
                    .collect(Collectors.toMap(e -> new String(e.nameBytes), e -> e.measurementAggregator));
        }
    }

    class StationNameMap {
        int[] indexes = new int[AVERAGE_CHUNK_SIZE];
        StationName[] names = new StationName[AVERAGE_CHUNK_SIZE];
        int currentIndex = 0;

        public void getOrCreate(byte[] stationNameBytes, int length, int doubleValue, int hash) {
            int position = hash & AVERAGE_CHUNK_SIZE_1;
            while (indexes[position] != 0 && names[indexes[position]].hash != hash) {
                position = ++position & AVERAGE_CHUNK_SIZE_1;
            }
            if (indexes[position] != 0) {
                StationName stationName = names[indexes[position]];
                stationName.measurementAggregator.combine(doubleValue);
                stationName.count++;
            }
            else {
                byte[] destination = new byte[length];
                System.arraycopy(stationNameBytes, 0, destination, 0, length);
                StationName stationName = new StationName(destination, hash);
                indexes[position] = ++currentIndex;
                names[indexes[position]] = stationName;

                stationName.measurementAggregator.combine(doubleValue);
                stationName.count++;
            }
        }
    }

}
