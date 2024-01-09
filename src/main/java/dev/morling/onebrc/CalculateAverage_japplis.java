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

/**
 * Maybe not the fastest but trying to get the most readable code for the performance.
 *
 * It allows:
 *  - pass another file as argument
 *  - the first lines can start with comments lines using '#'
 *  - the temperatures can have more than one fraction digit but it needs to be constant in the file
 *  - it does not require much RAM
 * Assumptions
 *  - No temperatures are above 100 or below -100
 *  - the last character of the file is \n
 *
 * @author Anthony Goubard - Japplis
 */
public class CalculateAverage_japplis {

    private static final String DEFAULT_MEASUREMENT_FILE = "measurements.txt";
    private static final int BUFFER_SIZE = 5 * 1024 * 1024; // 5 MB
    private static final int MAX_COMPUTE_THREADS = Runtime.getRuntime().availableProcessors();

    private int precision = -1;
    private int precisionLimitTenth;

    private Map<String, Measurement> cityMeasurementMap = new ConcurrentHashMap<>();
    private List<Byte> previousBlockLastLine = new ArrayList<>();

    private Semaphore readFileLock = new Semaphore(MAX_COMPUTE_THREADS);

    private void parseTemperatures(Path measurementsFile) throws Exception {
        try (InputStream measurementsFileIS = new FileInputStream(measurementsFile.toFile());
                BufferedInputStream measurementsBufferIS = new BufferedInputStream(measurementsFileIS, BUFFER_SIZE)) {
            int readCount = BUFFER_SIZE;
            ExecutorService threadPool = Executors.newFixedThreadPool(MAX_COMPUTE_THREADS);
            List<Future> parseBlockTasks = new ArrayList<>();
            while (readCount > 0) {
                byte[] buffer = new byte[BUFFER_SIZE];
                readCount = measurementsBufferIS.read(buffer);
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
            try {
                while (bufferIndex < readCount) {
                    bufferIndex = readNextLine(bufferIndex, buffer);
                }
            }
            catch (ArrayIndexOutOfBoundsException ex) {
                // Done reading and parsing the buffer
            }
            readFileLock.release();
        };
        return countAverageRun;
    }

    private int handleSplitLine(byte[] buffer, int readCount) {
        int bufferIndex = readFirstLines(buffer);
        List<Byte> lastLine = new ArrayList<>();
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
        readNextLine(0, splitLineBytes);
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

    private int readNextLine(int bufferIndex, byte[] buffer) {
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
        addTemperature(city, temperature);
        return bufferIndex;
    }

    int readTemperature(byte[] text, int measurementIndex) {
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

    private void addTemperature(String city, int temperature) {
        cityMeasurementMap.compute(city, (c, m) -> {
            if (m == null)
                return new Measurement(temperature);
            m.add(temperature);
            return m;
        });
    }

    private void printTemperatureStatsByCity() {
        Set<String> sortedCities = new TreeSet<>(cityMeasurementMap.keySet());
        StringBuilder result = new StringBuilder(cityMeasurementMap.size() * 40);
        result.append('{');
        sortedCities.forEach(city -> {
            Measurement measurement = cityMeasurementMap.get(city);
            result.append(city);
            result.append(getTemperatureStats(measurement));
        });
        result.delete(result.length() - 2, result.length());
        result.append('}');
        String temperaturesByCity = result.toString();
        System.out.println(temperaturesByCity);
    }

    private String getTemperatureStats(Measurement measurement) {
        StringBuilder stats = new StringBuilder(19);
        stats.append('=');
        appendTemperature(stats, measurement.min);
        stats.append('/');
        int average = (int) (Math.round(measurement.total / (double) measurement.count));
        appendTemperature(stats, average);
        stats.append('/');
        appendTemperature(stats, measurement.max);
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
        CalculateAverage_japplis averageCalculator = new CalculateAverage_japplis();
        String measurementFile = args.length == 1 ? args[0] : DEFAULT_MEASUREMENT_FILE;
        averageCalculator.parseTemperatures(Path.of(measurementFile));
        averageCalculator.printTemperatureStatsByCity();
    }

    private class Measurement {

        private int min;
        private int max;
        private long total;
        private int count = 1;

        // The initial measurement
        private Measurement(int value) {
            min = value;
            max = value;
            total = value;
        }

        private void add(int temperature) {
            if (temperature < min)
                min = temperature;
            if (temperature > max)
                max = temperature;
            total += temperature;
            count++;
        }
    }
}
