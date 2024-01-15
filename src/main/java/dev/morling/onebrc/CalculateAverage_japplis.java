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
import java.util.*;
import java.util.concurrent.*;

/**
 * Maybe not the fastest but trying to get the most readable code for the performance.
 *
 * It allows:
 *  - pass another file as argument
 *  - the first lines can start with comments lines using '#'
 *  - the temperatures can have more than one fraction digit but it needs to be constant in the file
 *  - it does not require much RAM
 *  - Java 8 as minimal Java version
 * Assumptions
 *  - No temperatures are above 100 or below -100
 *  - the last character of the file is \n
 *
 * Changelog:
 * - First local attempt with FileReader and TreeMap: Way too long
 * - Switched to InputStream and ConcurrentHashMap: 23"
 * - Added Semaphore to avoid OOMException: 23"
 * - Replaced String with my own ByteText class: a bit slower (~10%)
 * - Replaced compute lambda call with synchronized(city.intern()): 43" (due to intern())
 * - Removed BufferedInputStream and replaced Measurement with IntSummaryStatistics (thanks davecom): still 23" but cleaner code
 * - Execute same code on 1BRC server: 41"
 * - One HashMap per thread: 17" locally (12" on 1BRC server)
 * - Read file in multiple threads if available and
 * - Changed String to (byte[]) Text with cache: 18" locally (but 8" -> 5" on laptop)
 *
 * @author Anthony Goubard - Japplis
 */
public class CalculateAverage_japplis {

    private static final String DEFAULT_MEASUREMENT_FILE = "measurements.txt";
    private static final int BUFFER_SIZE = 5 * 1024 * 1024; // 5 MB
    private static final int MAX_COMPUTE_THREADS = Runtime.getRuntime().availableProcessors();

    private int precision = -1;
    private int precisionLimitTenth;
    private long fileSize;
    private Map<Text, IntSummaryStatistics> cityMeasurementMap = new ConcurrentHashMap<>(10_000);
    private List<Byte> previousBlockLastLine = new ArrayList<>();
    private Semaphore readFileLock = new Semaphore(MAX_COMPUTE_THREADS);
    private Queue<ByteArray> bufferPool = new ConcurrentLinkedQueue<>();

    private void parseTemperatures(File measurementsFile) throws Exception {
        fileSize = measurementsFile.length();
        int blockIndex = 0;
        int totalBlocks = (int) (fileSize / BUFFER_SIZE) + 1;
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_COMPUTE_THREADS);
        List<Future> parseBlockTasks = new ArrayList<>();

        while (blockIndex < totalBlocks) {
            int availableReadThreads = Math.min(readFileLock.availablePermits(), totalBlocks - blockIndex);
            if (availableReadThreads == 0) {
                readFileLock.acquire(); // No need to loop in the 'while' if all threads are busy
                readFileLock.release();
            }
            List<Future<ByteArray>> readBlockTasks = new ArrayList<>();
            for (int i = 0; i < availableReadThreads; i++) {
                readFileLock.acquire(); // Wait if all threads are busy
                Callable<ByteArray> blockReader = readBlock(measurementsFile, blockIndex);
                Future<ByteArray> readBlockTask = threadPool.submit(blockReader);
                readBlockTasks.add(readBlockTask);
                blockIndex++;
            }
            for (Future<ByteArray> readBlockTask : readBlockTasks) {
                ByteArray buffer = readBlockTask.get();
                if (buffer.array().length > 0) {
                    int startIndex = handleSplitLine(buffer.array());
                    readFileLock.acquire(); // Wait if all threads are busy
                    Runnable blockParser = parseTemperaturesBlock(buffer, startIndex);
                    Future parseBlockTask = threadPool.submit(blockParser);
                    parseBlockTasks.add(parseBlockTask);
                }
            }
        }
        for (Future parseBlockTask : parseBlockTasks) { // Wait for all tasks to finish
            parseBlockTask.get();
        }
        threadPool.shutdownNow();
    }

    private Callable<ByteArray> readBlock(File measurementsFile, long blockIndex) {
        return () -> {
            long fileIndex = blockIndex * BUFFER_SIZE;
            if (fileIndex >= fileSize) {
                readFileLock.release();
                return new ByteArray(0);
            }
            try (InputStream measurementsFileIS = new FileInputStream(measurementsFile)) {
                if (fileIndex > 0) {
                    long skipped = measurementsFileIS.skip(fileIndex);
                    while (skipped != fileIndex) {
                        skipped += measurementsFileIS.skip(fileIndex - skipped);
                    }
                }
                long bufferSize = Math.min(BUFFER_SIZE, fileSize - fileIndex);
                ByteArray buffer = bufferSize == BUFFER_SIZE ? bufferPool.poll() : new ByteArray((int) bufferSize);
                if (buffer == null) {
                    buffer = new ByteArray(BUFFER_SIZE);
                }
                int totalRead = measurementsFileIS.read(buffer.array(), 0, (int) bufferSize);
                while (totalRead < bufferSize) {
                    byte[] extraBuffer = new byte[(int) (bufferSize - totalRead)];
                    int readCount = measurementsFileIS.read(extraBuffer);
                    System.arraycopy(extraBuffer, 0, buffer.array(), totalRead, readCount);
                    totalRead += readCount;
                }
                readFileLock.release();
                return buffer;
            }
        };
    }

    private Runnable parseTemperaturesBlock(ByteArray buffer, int startIndex) {
        Runnable countAverageRun = () -> {
            int bufferIndex = startIndex;
            Map<Text, IntSummaryStatistics> blockCityMeasurementMap = new HashMap<>(10_000);
            Map<Integer, Text> textPool = new HashMap<>(10_000);
            byte[] bufferArray = buffer.array();
            try {
                while (bufferIndex < bufferArray.length) {
                    bufferIndex = readNextLine(bufferIndex, bufferArray, blockCityMeasurementMap, textPool);
                }
            }
            catch (ArrayIndexOutOfBoundsException ex) {
                // Done reading and parsing the buffer
            }
            if (bufferArray.length == BUFFER_SIZE)
                bufferPool.add(buffer);
            mergeBlockResults(blockCityMeasurementMap);
            readFileLock.release();
        };
        return countAverageRun;
    }

    private int handleSplitLine(byte[] buffer) {
        int bufferIndex = readFirstLines(buffer);
        List<Byte> lastLine = new ArrayList<>(100); // Store the last (partial) line of the block
        int tailIndex = buffer.length;
        byte car = buffer[--tailIndex];
        while (car != '\n') {
            lastLine.add(0, car);
            car = buffer[--tailIndex];
        }
        if (previousBlockLastLine.isEmpty()) {
            previousBlockLastLine = lastLine;
            return bufferIndex;
        }
        bufferIndex = readSplitLine(buffer);
        previousBlockLastLine = lastLine;
        return bufferIndex;
    }

    private int readSplitLine(byte[] buffer) {
        int bufferIndex = 0;
        byte car = buffer[bufferIndex++];
        while (car != '\n') {
            previousBlockLastLine.add(car);
            car = buffer[bufferIndex++];
        }
        previousBlockLastLine.add((byte) '\n');
        byte[] splitLineBytes = new byte[previousBlockLastLine.size()];
        for (int i = 0; i < splitLineBytes.length; i++) {
            splitLineBytes[i] = previousBlockLastLine.get(i);
        }
        readNextLine(0, splitLineBytes, cityMeasurementMap, new HashMap<>());
        return bufferIndex;
    }

    private int readFirstLines(byte[] buffer) {
        if (precision >= 0)
            return 0; // not the first lines of the file
        int bufferIndex = 0;
        while (buffer[bufferIndex] == '#') { // read comments (like in weather_stations.csv)
            while (buffer[bufferIndex++] != '\n') {
            }
        }
        int startIndex = bufferIndex;
        int dotPos = bufferIndex;
        byte car = buffer[bufferIndex++];
        while (car != '\n') {
            if (car == '.') {
                dotPos = bufferIndex;
            }
            car = buffer[bufferIndex++];
        }
        precision = bufferIndex - dotPos - 1;
        int precisionLimit = (int) Math.pow(10, precision);
        precisionLimitTenth = precisionLimit * 10;
        return startIndex;
    }

    private int readNextLine(int bufferIndex, byte[] buffer, Map<Text, IntSummaryStatistics> blockCityMeasurementMap, Map<Integer, Text> textPool) {
        int startLineIndex = bufferIndex;
        while (buffer[bufferIndex] != (byte) ';') {
            bufferIndex++;
        }
        // String city = new String(buffer, startLineIndex, bufferIndex - startLineIndex, StandardCharsets.UTF_8);
        Text city = Text.getByteText(buffer, startLineIndex, bufferIndex - startLineIndex, textPool);
        bufferIndex++; // skip ';'
        int temperature = readTemperature(buffer, bufferIndex);
        bufferIndex += precision + 3; // digit, dot and CR
        if (temperature < 0) {
            bufferIndex++;
        }
        if (temperature <= -precisionLimitTenth || temperature >= precisionLimitTenth) {
            bufferIndex++;
        }
        addTemperature(city, temperature, blockCityMeasurementMap);
        return bufferIndex;
    }

    private int readTemperature(byte[] buffer, int bufferIndex) {
        boolean negative = buffer[bufferIndex] == (byte) '-';
        if (negative) {
            bufferIndex++;
        }
        byte digit = buffer[bufferIndex++];
        int temperature = 0;
        while (digit != (byte) '\n') {
            temperature = temperature * 10 + (digit - (byte) '0');
            digit = buffer[bufferIndex++];
            if (digit == (byte) '.') { // Skip '.'
                digit = buffer[bufferIndex++];
            }
        }
        if (negative) {
            temperature = -temperature;
        }
        return temperature;
    }

    private void addTemperature(Text city, int temperature, Map<Text, IntSummaryStatistics> blockCityMeasurementMap) {
        IntSummaryStatistics measurement = blockCityMeasurementMap.get(city);
        if (measurement == null) {
            measurement = new IntSummaryStatistics();
            blockCityMeasurementMap.put(city, measurement);
        }
        measurement.accept(temperature);
    }

    private void mergeBlockResults(Map<Text, IntSummaryStatistics> blockCityMeasurementMap) {
        blockCityMeasurementMap.forEach((city, measurement) -> {
            cityMeasurementMap.compute(city, (town, currentMeasurement) -> {
                if (currentMeasurement == null) {
                    return measurement;
                }
                currentMeasurement.combine(measurement);
                return currentMeasurement;
            });
        });
    }

    private void printTemperatureStatsByCity() {
        Set<Text> sortedCities = new TreeSet<>(cityMeasurementMap.keySet());
        StringBuilder result = new StringBuilder(cityMeasurementMap.size() * 40);
        result.append('{');
        sortedCities.forEach(city -> {
            IntSummaryStatistics measurement = cityMeasurementMap.get(city);
            result.append(city);
            result.append(getTemperatureStats(measurement));
        });
        if (!sortedCities.isEmpty()) {
            result.delete(result.length() - 2, result.length());
        }
        result.append('}');
        String temperaturesByCity = result.toString();
        System.out.println(temperaturesByCity);
    }

    private String getTemperatureStats(IntSummaryStatistics measurement) {
        StringBuilder stats = new StringBuilder(19);
        stats.append('=');
        appendTemperature(stats, measurement.getMin());
        stats.append('/');
        int average = (int) Math.round(measurement.getAverage());
        appendTemperature(stats, average);
        stats.append('/');
        appendTemperature(stats, measurement.getMax());
        stats.append(", ");
        return stats.toString();
    }

    private void appendTemperature(StringBuilder resultBuilder, int temperature) {
        String temperatureAsText = String.valueOf(temperature);
        int minCharacters = precision + (temperature < 0 ? 2 : 1);
        for (int i = temperatureAsText.length(); i < minCharacters; i++) {
            temperatureAsText = temperature < 0 ? "-0" + temperatureAsText.substring(1) : "0" + temperatureAsText;
        }
        int dotPosition = temperatureAsText.length() - precision;
        resultBuilder.append(temperatureAsText.substring(0, dotPosition));
        resultBuilder.append('.');
        resultBuilder.append(temperatureAsText.substring(dotPosition));
    }

    public static final void main(String... args) throws Exception {
        CalculateAverage_japplis cityTemperaturesCalculator = new CalculateAverage_japplis();
        String measurementFile = args.length == 1 ? args[0] : DEFAULT_MEASUREMENT_FILE;
        cityTemperaturesCalculator.parseTemperatures(new File(measurementFile));
        cityTemperaturesCalculator.printTemperatureStatsByCity();
    }

    private class ByteArray {

        private byte[] array;

        private ByteArray(int size) {
            array = new byte[size];
        }

        private byte[] array() {
            return array;
        }
    }

    private static class Text implements Comparable<Text> {

        private final byte[] textBytes;
        private final int hash;
        private String text;

        private Text(byte[] buffer, int startIndex, int length, int hash) {
            textBytes = new byte[length];
            this.hash = hash;
            System.arraycopy(buffer, startIndex, textBytes, 0, length);
        }

        private static Text getByteText(byte[] buffer, int startIndex, int length, Map<Integer, Text> textPool) {
            int hash = hashCode(buffer, startIndex, length);
            Text textFromPool = textPool.get(hash);
            if (textFromPool == null || !Arrays.equals(buffer, startIndex, startIndex + length, textFromPool.textBytes, 0, length)) {
                Text newText = new Text(buffer, startIndex, length, hash);
                textPool.put(hash, newText);
                return newText;
            }
            return textFromPool;
        }

        private static int hashCode(byte[] buffer, int startIndex, int length) {
            int hash = 31;
            int endIndex = startIndex + length;
            for (int i = startIndex; i < endIndex; i++) {
                hash = 31 * hash + buffer[i];
            }
            return hash;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            return other != null &&
                    hashCode() == other.hashCode() &&
                    other instanceof Text &&
                    Arrays.equals(textBytes, ((Text) other).textBytes);
        }

        @Override
        public int compareTo(Text other) {
            return toString().compareTo(other.toString());
        }

        @Override
        public String toString() {
            if (text == null) {
                text = new String(textBytes, StandardCharsets.UTF_8);
            }
            return text;
        }
    }
}