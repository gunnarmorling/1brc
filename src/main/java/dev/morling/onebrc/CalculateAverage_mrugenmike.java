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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class CalculateAverage_mrugenmike {

    private static final String FILE = "./measurements.txt";

    private record ResultForACity(double min, double max, double sum, double count) {
        public String toString() {
            return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
//        try (final Stream<String> lines = Files.lines(Path.of(FILE), StandardCharsets.UTF_8)) {
//            final ConcurrentSkipListMap<String, double[]> result = new ConcurrentSkipListMap<>();
//            lines.parallel().forEach((line) -> processSingleLine(result, line));
//            System.out.println(result);
//        }
        /* bufferedreader*/
        final ConcurrentSkipListMap<String, double[]> result = new ConcurrentSkipListMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FILE))) {
            br.lines().parallel().forEach(line -> processSingleLine(result, line));
            System.out.println(new TreeMap<>(result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, (e) -> new ResultForACity(e.getValue()[0], e.getValue()[1], e.getValue()[2], e.getValue()[3])))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(STR."Seconds=\{(System.currentTimeMillis() - start) / 1000}");
    }

    private static void processSingleLine(ConcurrentSkipListMap<String, double[]> result, String line) {
        System.out.println("line" + line);
        final int semiColonIndex = line.indexOf(';');
        final String city = line.substring(0, semiColonIndex).intern();
        final double currentMeasurement = Double.parseDouble(line.substring(semiColonIndex + 1).trim());
        result.compute(city, (String cityName, double[] previousValue) -> {
            if (previousValue == null) {
                BigDecimal sum = new BigDecimal(currentMeasurement).setScale(1, RoundingMode.HALF_UP);
                return new double[]{currentMeasurement, currentMeasurement, sum.doubleValue(), 1};
            }
            BigDecimal calculatedSum = BigDecimal.valueOf(previousValue[2]).add(new BigDecimal(currentMeasurement)).setScale(1, RoundingMode.HALF_UP);
            return new double[]{Math.min(currentMeasurement, previousValue[0]), Math.max(currentMeasurement, previousValue[1]), calculatedSum.doubleValue(), previousValue[3] + 1};
        });
    }
}
