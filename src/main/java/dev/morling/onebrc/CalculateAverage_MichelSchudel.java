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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_MichelSchudel {

    private static final Map<String, double[]> allMap = new ConcurrentHashMap<>();

    private static final String FILE_NAME = "measurements.txt";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();

        Stream<String> lines = Files.lines(Paths.get(FILE_NAME));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            lines.parallel().forEach(CalculateAverage_MichelSchudel::processLine);
        }

        long end = System.currentTimeMillis() - start;
        System.out.println("processing took " + end + "ms.");

        printResult();
    }

    private static void printResult() {
        var keySet = allMap.keySet().stream().sorted().toList();
        var list = new ArrayList<String>();
        keySet.forEach(key -> {
            var recording = allMap.get(key);
            var text = key + "=" + recording[0] + "/" + (Math.round((recording[1] / recording[3]) * 10.0) / 10.0) + "/" + recording[2];
            list.add(text);
        });
        System.out.println(list.stream().collect(Collectors.joining(",", "{", "}")));
    }

    private static void processLine(String line) {
        Object[] measurement = parseLine(line);
        allMap.compute((String)measurement[0], (k, v) -> {
            var temp = (double)measurement[1];
            if (v == null) return createTemperateCalculation(temp);
            else return computeTemperatureCalculation(temp, v);
        });
    }

    private static double[] createTemperateCalculation(double temperature) {
        return new double[]{
                temperature,
                temperature,
                temperature,
                1};
    }

    private static double[] computeTemperatureCalculation(double temperature, double[] existingCalculation) {

        existingCalculation[0] = Math.min(existingCalculation[0], temperature);
        existingCalculation[1] = existingCalculation[1] + temperature;
        existingCalculation[2] = Math.max(existingCalculation[2], temperature);
        existingCalculation[3] = existingCalculation[3] + 1;
        return existingCalculation;
    }

    private static Object[] parseLine(String line) {
        int index = line.indexOf(";");
        return new Object[] {line.substring(0, index), Double.parseDouble(line.substring(index + 1))};
    }

}