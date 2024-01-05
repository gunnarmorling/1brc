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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_isolgpus {

    public static final int HISTOGRAMS_LENGTH = 1024 * 32;
    public static final int HISTOGRAMS_MASK = HISTOGRAMS_LENGTH - 1;
    public static final int THREAD_COUNT = 8;
    private static final String FILE = "./measurements.txt";
    public static final byte SEPERATOR = 59;
    public static final byte OFFSET = 48;
    public static final byte NEGATIVE = 45;
    public static final byte DECIMAL_POINT = 46;
    public static final int MAX_CHUNK_SIZE = Integer.MAX_VALUE - 100; // bit of wiggle room
    public static final byte NEW_LINE = 10;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        File file = Paths.get(FILE).toFile();
        long length = file.length();
        long chunksCount = Math.max(THREAD_COUNT, length / MAX_CHUNK_SIZE);

        long estimatedChunkSize = length / chunksCount;

        FileChannel channel = new RandomAccessFile(file, "r").getChannel();

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        List<Future<MeasurementCollector[]>> futures = new ArrayList<>();
        for (int i = 0; i < chunksCount; i++) {
            int finalI = i;
            futures.add(executorService.submit(() -> handleChunk(channel, estimatedChunkSize * finalI, estimatedChunkSize, length)));
        }

        List<MeasurementCollector[]> measurementCollectors = new ArrayList<>();
        for (Future<MeasurementCollector[]> result : futures) {
            measurementCollectors.add(result.get());
        }

        executorService.shutdown();

        Map<String, MeasurementCollector> measurementCollectorsByCity = mergeMeasurements(measurementCollectors);
        List<MeasurementResult> results = measurementCollectorsByCity.values().stream().map(MeasurementResult::from).toList();

        System.out.println("{" + results.stream().map(MeasurementResult::toString).collect(Collectors.joining(", ")) + "}");
    }

    private static Map<String, MeasurementCollector> mergeMeasurements(List<MeasurementCollector[]> resultsFromAllChunk) {
        Map<String, MeasurementCollector> mergedResults = new TreeMap<>(Comparator.naturalOrder());

        for (int i = 0; i < HISTOGRAMS_LENGTH; i++) {
            for (MeasurementCollector[] resultFromSpecificChunk : resultsFromAllChunk) {
                MeasurementCollector measurementCollectorFromChunk = resultFromSpecificChunk[i];
                while (measurementCollectorFromChunk != null) {
                    MeasurementCollector currentMergedResult = mergedResults.get(new String(measurementCollectorFromChunk.name));
                    if (currentMergedResult == null) {
                        currentMergedResult = new MeasurementCollector(measurementCollectorFromChunk.name);
                        mergedResults.put(new String(currentMergedResult.name), currentMergedResult);
                    }
                    currentMergedResult.merge(measurementCollectorFromChunk);
                    measurementCollectorFromChunk = measurementCollectorFromChunk.link;
                }
            }
        }

        return mergedResults;
    }

    // ----n---
    private static MeasurementCollector[] handleChunk(FileChannel channel, long estimatedStart, long lengthOfChunk, long maxLengthOfFile) throws IOException {
        // -1 to see if we're starting on a brand new message
        // +200 for wiggle room to finish the final message

        long seekStart = Math.max(estimatedStart - 1, 0);
        long length = Math.min(lengthOfChunk + 200, maxLengthOfFile - seekStart);

        MappedByteBuffer r = channel.map(FileChannel.MapMode.READ_ONLY, seekStart, length);

        byte[] nameBuffer = new byte[100];
        boolean isNegative;
        byte[] valueBuffer = new byte[3];
        MeasurementCollector[] measurementCollectors = new MeasurementCollector[HISTOGRAMS_LENGTH];
        int valueIndex = 0;
        int nameBufferIndex = 0;
        boolean parsingName = true;
        long i = 0;

        // seek to the start of the next message
        if (estimatedStart != 0) {
            while (r.get() != NEW_LINE) {
                i++;
            }
            i++;
        }

        try {

            while (i <= lengthOfChunk || !parsingName) {
                byte aChar;
                if (parsingName) {

                    nameBufferIndex = buildName(r, nameBuffer, nameBufferIndex);
                    parsingName = false;
                    i += nameBufferIndex + 1;
                }
                else {
                    isNegative = (aChar = r.get()) == NEGATIVE;
                    valueIndex = readNumber(isNegative, valueBuffer, valueIndex, aChar, r);

                    byte decimalValue = r.get();

                    int value = resolveValue(valueIndex, valueBuffer, decimalValue, isNegative);
                    // new line character
                    r.get();

                    int hash = byteHashCode(nameBuffer, nameBufferIndex);
                    MeasurementCollector measurementCollector = resolveMeasurementCollector(measurementCollectors, hash, nameBuffer, nameBufferIndex);

                    measurementCollector.feed(value);
                    i += valueIndex + (isNegative ? 4 : 3);
                    valueIndex = 0;
                    nameBufferIndex = 0;
                    parsingName = true;
                }
            }

        }
        catch (BufferUnderflowException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return measurementCollectors;
    }

    private static MeasurementCollector resolveMeasurementCollector(MeasurementCollector[] measurementCollectors, int hash, byte[] nameBuffer, int nameBufferIndex) {
        MeasurementCollector measurementCollector = measurementCollectors[hash & HISTOGRAMS_MASK];
        if (measurementCollector == null) {
            measurementCollector = new MeasurementCollector(Arrays.copyOf(nameBuffer, nameBufferIndex));
            measurementCollectors[hash & HISTOGRAMS_MASK] = measurementCollector;
        }
        else {
            // collision unhappy path, try to avoid
            while (!nameEquals(measurementCollector.name, nameBuffer, nameBufferIndex)) {
                if (measurementCollector.link == null) {
                    measurementCollector.link = new MeasurementCollector(Arrays.copyOf(nameBuffer, nameBufferIndex));
                    measurementCollector = measurementCollector.link;
                    break;
                }
                else {
                    measurementCollector = measurementCollector.link;
                }

            }

        }
        return measurementCollector;
    }

    private static boolean nameEquals(byte[] existingName, byte[] newName, int nameBufferIndex) {
        if (existingName.length != nameBufferIndex) {
            return false;
        }

        for (int i = 0; i < nameBufferIndex; i++) {
            if (existingName[i] != newName[i]) {
                return false;
            }
        }
        return true;
    }

    private static int resolveValue(int valueIndex, byte[] valueBuffer, byte decimalValue, boolean isNegative) {
        int value;
        if (valueIndex == 1) {
            value = ((valueBuffer[0] - OFFSET) * 10) + (decimalValue - OFFSET);
        }
        else // it's 2 digits
        {
            value = ((valueBuffer[0] - OFFSET) * 100) + ((valueBuffer[1] - OFFSET) * 10) + (decimalValue - OFFSET);
        }

        if (isNegative) {
            value = Math.negateExact(value);
        }
        return value;
    }

    private static int readNumber(boolean isNegative, byte[] valueBuffer, int valueIndex, byte aChar, MappedByteBuffer r) {
        if (!isNegative) {
            valueBuffer[valueIndex++] = aChar;
        }

        // maybe one or two more
        while ((aChar = r.get()) != DECIMAL_POINT) {
            valueBuffer[valueIndex++] = aChar;
        }
        return valueIndex;
    }

    private static int buildName(MappedByteBuffer r, byte[] nameBuffer, int nameBufferIndex) {
        byte aChar;
        while ((aChar = r.get()) != SEPERATOR) {
            nameBuffer[nameBufferIndex++] = aChar;
        }
        return nameBufferIndex;
    }

    private static int byteHashCode(byte[] a, int length) {
        int result = 0;
        for (int i = 0; i < length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    private static class MeasurementCollector {
        private final byte[] name;
        public MeasurementCollector link;
        private long sum;
        private int count;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;

        public MeasurementCollector(byte[] name) {

            this.name = name;
        }

        public void feed(int value) {
            sum += value;
            count++;
            min = Math.min(value, min);
            max = Math.max(value, max);
        }

        public void merge(MeasurementCollector measurementCollector) {
            this.sum += measurementCollector.sum;
            this.count += measurementCollector.count;
            this.min = Math.min(measurementCollector.min, this.min);
            this.max = Math.max(measurementCollector.max, this.max);
        }
    }

    private static class MeasurementResult {
        private final String name;
        private final double mean;
        private final BigDecimal max;
        private final BigDecimal min;

        public MeasurementResult(String name, double mean, BigDecimal max, BigDecimal min) {

            this.name = name;
            this.mean = mean;
            this.max = max;
            this.min = min;
        }

        @Override
        public String toString() {
            // Abha=-24.9/18.0/61.7
            return name + "=" + min + "/" + mean + "/" + max;
        }

        public static MeasurementResult from(MeasurementCollector mc) {
            double mean = Math.round((double) mc.sum / (double) mc.count) / 10d;
            BigDecimal max = BigDecimal.valueOf(mc.max).divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP);
            BigDecimal min = BigDecimal.valueOf(mc.min).divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP);
            return new MeasurementResult(new String(mc.name), mean, max, min);
        }
    }
}
