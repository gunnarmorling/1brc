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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class CalculateAverage_alesj {
    public static void main(String[] args) throws Exception {
        Map<String, DoubleSummaryStatistics> stations = new ConcurrentHashMap<>();
        try (Stream<String> stream = Files.lines(Paths.get("./measurements.txt")).parallel()) {
            stream.forEach(line -> {
                String[] split = line.split(";");
                stations.computeIfAbsent(split[0], k -> new DoubleSummaryStatistics() {
                    public synchronized void accept(double value) {
                        super.accept(value);
                    }

                    public String toString() {
                        return String.format("%.1f/%.1f/%.1f", getMin(), getAverage(), getMax());
                    }
                })
                        .accept(Double.parseDouble(split[1]));
            });
        }
        System.out.println(new TreeMap<>(stations));
    }
}
