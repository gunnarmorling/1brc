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
 * - One HashMap per thread: 17" locally
 *
 * @author Anthony Goubard - Japplis
 */
public class CalculateAverage_japplis {

    private static final String DEFAULT_MEASUREMENT_FILE = "measurements.txt";
    private static final int BUFFER_SIZE = 5 * 1024 * 1024; // 5 MB
    private static final int MAX_COMPUTE_THREADS = Runtime.getRuntime().availableProcessors();

    private int precision = -1;
    private int precisionLimitTenth;

    private Map<String, IntSummaryStatistics> cityMeasurementMap = new ConcurrentHashMap<>();
    private List<Byte> previousBlockLastLine = new ArrayList<>();

    private Semaphore readFileLock = new Semaphore(MAX_COMPUTE_THREADS);

    private void parseTemperatures(File measurementsFile) throws Exception {
        try (InputStream measurementsFileIS = new FileInputStream(measurementsFile)) {
            int readCount = BUFFER_SIZE;
            ExecutorService threadPool = Executors.newFixedThreadPool(MAX_COMPUTE_THREADS);
            List<Future> parseBlockTasks = new ArrayList<>();
            while (readCount > 0) {
                byte[] buffer = new byte[BUFFER_SIZE];
                readCount = measurementsFileIS.read(buffer);
                if (readCount > 0) {
                    readFileLock.acquire(); // Wait if all threads are busy

                    // Process the block in a thread while the main thread continues to read the file
                    Future parseBlockTask = threadPool.submit(parseTemperaturesBlock(buffer, readCount));
                    parseBlockTasks.add(parseBlockTask);
                }
            }
            for (Future parseBlockTask : parseBlockTasks) // Wait for all tasks to finish
                parseBlockTask.get();
            threadPool.shutdownNow();
        }
    }

    private Runnable parseTemperaturesBlock(byte[] buffer, int readCount) {
        int startIndex = handleSplitLine(buffer, readCount);
        Runnable countAverageRun = () -> {
            int bufferIndex = startIndex;
            Map<String, IntSummaryStatistics> blockCityMeasurementMap = new HashMap<>();
            try {
                while (bufferIndex < readCount) {
                    bufferIndex = readNextLine(bufferIndex, buffer, blockCityMeasurementMap);
                }
            }
            catch (ArrayIndexOutOfBoundsException ex) {
                // Done reading and parsing the buffer
            }
            mergeBlockResults(blockCityMeasurementMap);
            readFileLock.release();
        };
        return countAverageRun;
    }

    private int handleSplitLine(byte[] buffer, int readCount) {
        int bufferIndex = readFirstLines(buffer);
        List<Byte> lastLine = new ArrayList<>(); // Store the last (partial) line of the block
        int tailIndex = readCount;
        if (tailIndex == buffer.length) {
            byte car = buffer[--tailIndex];
            while (car != '\n') {
                lastLine.add(0, car);
                car = buffer[--tailIndex];
            }
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
        readNextLine(0, splitLineBytes, cityMeasurementMap);
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
            if (car == '.')
                dotPos = bufferIndex;
            car = buffer[bufferIndex++];
        }
        precision = bufferIndex - dotPos - 1;
        int precisionLimit = (int) Math.pow(10, precision);
        precisionLimitTenth = precisionLimit * 10;
        return startIndex;
    }

    private int readNextLine(int bufferIndex, byte[] buffer, Map<String, IntSummaryStatistics> blockCityMeasurementMap) {
        int startLineIndex = bufferIndex;
        while (buffer[bufferIndex] != ';')
            bufferIndex++;
        String city = new String(buffer, startLineIndex, bufferIndex - startLineIndex, StandardCharsets.UTF_8);
        bufferIndex++; // skip ';'
        int temperature = readTemperature(buffer, bufferIndex);
        bufferIndex += precision + 3; // digit, dot and CR
        if (temperature < 0)
            bufferIndex++;
        if (temperature <= -precisionLimitTenth || temperature >= precisionLimitTenth)
            bufferIndex++;
        addTemperature(city, temperature, blockCityMeasurementMap);
        return bufferIndex;
    }

    private int readTemperature(byte[] text, int measurementIndex) {
        boolean negative = text[measurementIndex] == '-';
        if (negative)
            measurementIndex++;
        byte digitChar = text[measurementIndex++];
        int temperature = 0;
        while (digitChar != '\n') {
            temperature = temperature * 10 + (digitChar - '0');
            digitChar = text[measurementIndex++];
            if (digitChar == '.')
                digitChar = text[measurementIndex++];
        }
        if (negative)
            temperature = -temperature;
        return temperature;
    }

    private void addTemperature(String city, int temperature, Map<String, IntSummaryStatistics> blockCityMeasurementMap) {
        IntSummaryStatistics measurement = blockCityMeasurementMap.get(city);
        if (measurement == null) {
            measurement = new IntSummaryStatistics();
            blockCityMeasurementMap.put(city, measurement);
        }
        measurement.accept(temperature);
    }

    private void mergeBlockResults(Map<String, IntSummaryStatistics> blockCityMeasurementMap) {
        blockCityMeasurementMap.forEach((city, measurement) -> {
            IntSummaryStatistics oldMeasurement = cityMeasurementMap.putIfAbsent(city, measurement);
            if (oldMeasurement != null)
                oldMeasurement.combine(measurement);
        });
    }

    private void printTemperatureStatsByCity() {
        Set<String> sortedCities = new TreeSet<>(cityMeasurementMap.keySet());
        StringBuilder result = new StringBuilder(cityMeasurementMap.size() * 40);
        result.append('{');
        sortedCities.forEach(city -> {
            IntSummaryStatistics measurement = cityMeasurementMap.get(city);
            result.append(city);
            result.append(getTemperatureStats(measurement));
        });
        result.delete(result.length() - 2, result.length());
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
        resultBuilder.append(temperatureAsText.substring(0, temperatureAsText.length() - precision));
        resultBuilder.append('.');
        resultBuilder.append(temperatureAsText.substring(temperatureAsText.length() - precision));
    }

    public static final void main(String... args) throws Exception {
        CalculateAverage_japplis cityTemperaturesCalculator = new CalculateAverage_japplis();
        String measurementFile = args.length == 1 ? args[0] : DEFAULT_MEASUREMENT_FILE;
        cityTemperaturesCalculator.parseTemperatures(new File(measurementFile));
        cityTemperaturesCalculator.printTemperatureStatsByCity();
    }
}
