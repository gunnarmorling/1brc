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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_varunsankethi {

    static Map<String, DataPoints> dataPointsMapConc = new ConcurrentSkipListMap<>();
    static String pathToFile = "./measurements.txt";

    public static void main(String[] args) throws IOException {

        System.out.println(LocalDateTime.now());
        Files.newBufferedReader(Paths.get(pathToFile)).lines().parallel().forEach(line -> calculateMeasurements(line));

        for (DataPoints dp : dataPointsMapConc.values()) {
            dp.mean = Math.round((dp.sum / dp.count) * 10.0) / 10.0;
        }
        System.out.println(dataPointsMapConc);
        System.out.println(LocalDateTime.now());
    }

    private static void calculateMeasurements(String line) {

        int indexOfSplit = line.indexOf(";");
        double newVal = Double.parseDouble(line.substring(indexOfSplit + 1));
        String city = line.substring(0, indexOfSplit);
        DataPoints dataPoints = null;

        if ((dataPoints = dataPointsMapConc.get(city)) == null) {
            dataPointsMapConc.put(city, new DataPoints(newVal, newVal, newVal));
        }
        else {

            dataPoints.count = dataPoints.count + 1;
            dataPoints.min = Math.min(dataPoints.min, newVal);
            dataPoints.sum = dataPoints.sum + newVal;
            dataPoints.max = Math.max(dataPoints.max, newVal);

        }
    }

    private static class DataPoints {

        double min = Double.MAX_VALUE;
        double mean = 0;
        double max = Double.MIN_VALUE;
        double count = 1;

        double sum = 0;

        public DataPoints(double min, double mean, double max) {

            this.max = max;
            this.mean = mean;
            this.min = min;
        }

        @Override
        public String toString() {
            return this.min + "/" + this.mean + "/" + this.max;
        }
    }
}
