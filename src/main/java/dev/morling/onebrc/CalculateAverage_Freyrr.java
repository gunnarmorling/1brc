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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;



public class CalculateAverage_Freyrr {
    private static final String FILE = "./measurements.txt";
    public static void main(String[] args) {
        
        try {
            Map<String, TemperatureStats> cityStats = processFile(FILE);
            printCityStats(cityStats);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static Map<String, TemperatureStats> processFile(String filePath) throws Exception {
        Map<String, TemperatureStats> cityStats = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length == 2) {
                    String city = parts[0];
                    double temperature = Double.parseDouble(parts[1]);

                    TemperatureStats stats = cityStats.getOrDefault(city, new TemperatureStats());
                    stats.update(temperature);
                    cityStats.put(city, stats);
                }
            }
        }

        return cityStats;
    }

    private static void printCityStats(Map<String, TemperatureStats> cityStats) {
        for (Map.Entry<String, TemperatureStats> entry : cityStats.entrySet()) {
            String city = entry.getKey();
            TemperatureStats stats = entry.getValue();
            System.out.println(city +"=" + round(stats.getMinTemperature() / 10.0) +
                                     "/" + round(stats.getMaxTemperature() / 10.0) +
                                     "/" + round(stats.getAverageTemperature() / 10.0));
        }
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    static class TemperatureStats {
        private double minTemperature = Double.MAX_VALUE;
        private double maxTemperature = Double.MIN_VALUE;
        private double totalTemperature = 0.0;
        private int count = 0;

        public void update(double temperature) {
            minTemperature = Math.min(minTemperature, temperature);
            maxTemperature = Math.max(maxTemperature, temperature);
            totalTemperature += temperature;
            count++;
        }

        public double getMinTemperature() {
            return minTemperature;
        }

        public double getMaxTemperature() {
            return maxTemperature;
        }

        public double getAverageTemperature() {
            return count > 0 ? totalTemperature / count : 0.0;
        }
    }

    static class UnsafeUtils {
        private static final Unsafe unsafe;

        static {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = (Unsafe) field.get(null);
            } catch (Exception e) {
                throw new RuntimeException("Failed to obtain Unsafe instance", e);
            }
        }

        static double getDouble(Object obj, long offset) {
            return unsafe.getDouble(obj, offset);
        }

        static void putDouble(Object obj, long offset, double value) {
            unsafe.putDouble(obj, offset, value);
        }
    }
}
