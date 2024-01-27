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

import static java.lang.Math.round;
import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;

public class CalculateAverage_jeevjyot {

    public static final String MEAUREMENT_FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, tempMeasurement> result = new ConcurrentHashMap<>();
        Files.lines(Path.of(MEAUREMENT_FILE))
                .parallel()
                .forEach(s -> {
                    var separatorIndex = s.indexOf(";");
                    var stationName = s.substring(0, separatorIndex);
                    var temp = s.substring(separatorIndex + 1);
                    result.computeIfAbsent(stationName, d -> new tempMeasurement(parseDoubleFast(temp)))
                            .recordTemp(parseDoubleFast(temp));
                });

        TreeMap<String, tempMeasurement> sortedStats = new TreeMap<>(result);
        System.out.println(sortedStats);
    }

    public static double parseDoubleFast(String str) {
        // Simple implementation - can be improved with more error checking and support for different formats
        boolean negative = false;
        double result = 0;
        int length = str.length();
        int i = 0;
        if (str.charAt(0) == '-') {
            negative = true;
            i++;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c == '.') {
                int divisor = 1;
                for (i++; i < length; i++) {
                    result += (double) (str.charAt(i) - '0') / (divisor *= 10);
                }
                break;
            }
            result = result * 10 + (c - '0');
        }
        return negative ? -result : result;
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    public static class tempMeasurement {
        double minTemp;
        double maxTemp;
        double sum;
        int count;

        public tempMeasurement(double temString) {
            this.minTemp = temString;
            this.maxTemp = temString;
            this.sum = 0.0;
            this.count = 0;
        }

        public synchronized void recordTemp(Double temp) {
            this.minTemp = Math.min(minTemp, temp);
            this.maxTemp = Math.max(maxTemp, temp);
            sum += temp;
            count++;
        }

        double getAverage() {
            return round(sum) / count;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", round(minTemp), round(getAverage()), round(maxTemp));
        }
    }
}
