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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentMap;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class CalculateAverage_ivanklaric {
    private static final String FILE = "measurements.txt";

    record CityTemps (double min, double max, double sum, long count) {
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double num) {
            return Math.round(num * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        Stream<String> lines = Files.lines(Paths.get(FILE));
        ConcurrentMap<String, CityTemps> cityStats = new ConcurrentSkipListMap<>();

        lines.parallel().forEach(line -> {
            int splitterLoc = line.indexOf(';');
            double temp = Double.parseDouble(line.substring(splitterLoc + 1, line.length()));

            cityStats.merge(line.substring(0, splitterLoc), new CityTemps(temp, temp, temp, 1),
                    (oldValue, defaultValue) -> {
                        return new CityTemps(Math.min(oldValue.min, temp), Math.max(oldValue.max, temp), oldValue.sum + temp, oldValue.count + 1);
                    });
        });
        System.out.println(cityStats);
    }
}
