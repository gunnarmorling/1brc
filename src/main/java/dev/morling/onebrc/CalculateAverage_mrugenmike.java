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
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class CalculateAverage_mrugenmike {

    private static final String FILE = "./measurements.txt";

    private record ResultForACity(String city, double min, double max, double count, double sum) {
        public String toString() {
            return round(min) + "/" + round(mean()) + "/" + round(max);
        }

        private double mean() {
            return sum / count;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        try (final Stream<String> lines = Files.lines(Path.of(FILE), StandardCharsets.UTF_8)) {
            final ConcurrentHashMap<String, ResultForACity> intermediateResult = new ConcurrentHashMap<>();
            lines.parallel().forEach((line) -> {
                final int semiColonIndex = line.indexOf(';');
                final String city = line.substring(0, semiColonIndex).intern();
                final double reading = Double.parseDouble(line.substring(semiColonIndex + 1).trim());
                intermediateResult.computeIfAbsent(city, cityName -> new ResultForACity(cityName, reading, reading, 1, reading));

                intermediateResult.computeIfPresent(city,
                        (key, value) -> new ResultForACity(key, Math.min(reading, value.min), Math.min(reading, value.max), value.count + 1, value.sum + reading));
            });
            TreeMap<String, ResultForACity> finalResult = new TreeMap<>(intermediateResult);
            System.out.println(finalResult);
        }
    }
}
