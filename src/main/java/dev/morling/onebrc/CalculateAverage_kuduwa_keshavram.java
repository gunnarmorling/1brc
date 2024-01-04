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

import org.radughiorma.Arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CalculateAverage_kuduwa_keshavram {

  private record Measurement(double min, double max, double sum, long count) {

    Measurement(double initialMeasurement) {
      this(initialMeasurement, initialMeasurement, initialMeasurement, 1);
    }

    public static Measurement combineWith(Measurement m1, Measurement m2) {
      return new Measurement(
          m1.min < m2.min ? m1.min : m2.min,
          m1.max > m2.max ? m1.max : m2.max,
          m1.sum + m2.sum,
          m1.count + m2.count);
    }

    public String toString() {
      return round(min) + "/" + round(sum / count) + "/" + round(max);
    }

    private double round(double value) {
      return Math.round(value * 10.0) / 10.0;
    }
  }

    public static void main(String[] args) throws IOException {
        // long before = System.currentTimeMillis();
        Map<String, Measurement> resultMap = new ConcurrentHashMap<>();
        Files.lines(Arguments.measurmentsPath(args))
                .parallel()
                .forEach(
                        line -> {
                            int pivot = line.indexOf(";");
                            String key = line.substring(0, pivot);
                            Measurement measured = new Measurement(Double.parseDouble(line.substring(pivot + 1)));
                            Measurement existingMeasurement = resultMap.get(key);
                            if (existingMeasurement != null) {
                                resultMap.put(key, Measurement.combineWith(existingMeasurement, measured));
                            }
                            else {
                                resultMap.put(key, measured);
                            }
                        });
        System.out.print("{");
        System.out.print(
                resultMap.entrySet().stream()
                        .map(Object::toString)
                        .sorted()
                        .collect(Collectors.joining(", ")));
        System.out.println("}");
        // System.out.println("Took: " + (System.currentTimeMillis() - before));
    }
}
