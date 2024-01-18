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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the solution from GitHut Copilot Chat with the help of Antonio Goncalves (prompting and guiding, but trying not to change code directly on my own, always using Copilot).
 * <p>
 * List of prompts that has been used:
 * <p>
 * =============
 * =============
 * =============
 * v1 - 73603 ms
 * You are entering The One Billion Row Challenge (1BRC) which is an exploration of how far modern Java can be pushed for aggregating one billion rows from a text file. Grab all the (virtual) threads, reach out to SIMD, optimize the GC, or pull any other trick, and create the fastest implementation for solving this task!
 * The text file contains temperature values for a range of weather stations. Each row is one measurement in the format <string: station name>;<double: measurement>, with the measurement value having exactly one fractional digit. The following delimited with --- shows ten rows as an example:
 * ---
 * Hamburg;12.0
 * Bulawayo;8.9
 * Palembang;38.8
 * St. John's;15.2
 * Cracow;12.6
 * Bridgetown;26.9
 * Istanbul;6.2
 * Roseau;34.4
 * Conakry;31.2
 * Istanbul;23.0
 * ---
 * You have to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like the result below delimited by --- (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit). Notice the curly braces:
 * ---
 * {Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
 * ---
 * You must use Java 21.
 * Create an algorithm in any way you see fit including parallelizing the computation, using the (incubating) Vector API, memory-mapping different sections of the file concurrently, using AppCDS, GraalVM, CRaC, etc. for speeding up the application start-up, choosing and tuning the garbage collector, and much more.
 * No external library dependencies may be used.
 * =============
 * =============
 * =============
 * (Here I had to chat with Copilot about formatting the output, there were commas missing, the curly brackets were also missed)
 * =============
 * =============
 * =============
 * v2 - 71831 ms
 * Being written in Java 21, please use records instead of classes for Measurement.
 * =============
 * =============
 * =============
 * v3 - 69333 ms
 * If the temperatures are small numbers, why use double? Can't you use another datatype ?
 * <p>
 * The profiler mentions that this line of code has very bad performance. Can you refactor it so it has better performance:
 * ---
 * String[] parts = line.split(";")
 * ---
 * <p>
 * There is a maximum of 10000 unique station names. Can you optimize the code taking this into account?
 * =============
 * =============
 * =============
 * v4 - 56417 ms
 * Which parameters can I pass to the JVM to make it run faster ?
 * Which GC can I use and what is the most optimized to run CalculateAverage ?
 */
public class CalculateAverage_agoncal {

    private static final String FILE = "./measurements.txt";

    record Measurement(String station, double temperature) {
    }

    static class StationStats {
        double min;
        double max;
        double sum;
        int count;

        public StationStats(double temperature) {
            this.min = temperature;
            this.max = temperature;
            this.sum = 0;
            this.count = 0;
        }

        synchronized void update(double temperature) {
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
            sum += temperature;
            count++;
        }

        double getAverage() {
            return round(sum) / count;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", round(min), round(getAverage()), round(max));
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, StationStats> stats = new ConcurrentHashMap<>(10_000);
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(FILE))) {
            reader.lines().parallel().forEach(line -> {
                int separatorIndex = line.indexOf(';');
                String station = line.substring(0, separatorIndex);
                String temperature = line.substring(separatorIndex + 1);
                Measurement m = new Measurement(station, Double.parseDouble(temperature));
                stats.computeIfAbsent(m.station, k -> new StationStats(m.temperature)).update(m.temperature);
            });
        }

        TreeMap<String, StationStats> sortedStats = new TreeMap<>(stats);
        Iterator<Map.Entry<String, StationStats>> iterator = sortedStats.entrySet().iterator();
        System.out.print("{");
        while (iterator.hasNext()) {
            Map.Entry<String, StationStats> entry = iterator.next();
            StationStats s = entry.getValue();
            if (iterator.hasNext()) {
                System.out.printf("%s=%s, ", entry.getKey(), s.toString());
            }
            else {
                System.out.printf("%s=%s", entry.getKey(), s.toString());
            }
        }
        System.out.println("}");
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }
}
