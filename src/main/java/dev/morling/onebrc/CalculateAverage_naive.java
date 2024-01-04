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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class CalculateAverage_naive {

    record Result(double min, double max, double sum, long count) {
    }

    public static void main(String[] args) throws FileNotFoundException {
        long start = System.currentTimeMillis();
        var results = new BufferedReader(new FileReader(Arguments.measurmentsFilename(args)))
                .lines()
                .map(l -> l.split(";"))
                .collect(Collectors.toMap(
                        parts -> parts[0],
                        parts -> {
                            double temperature = Double.parseDouble(parts[1]);
                            return new Result(temperature, temperature, temperature, 1);
                        },
                        (oldResult, newResult) -> {
                            double min = Math.min(oldResult.min, newResult.min);
                            double max = Math.max(oldResult.max, newResult.max);
                            double sum = oldResult.sum + newResult.sum;
                            long count = oldResult.count + newResult.count;
                            return new Result(min, max, sum, count);
                        }, ConcurrentSkipListMap::new));
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(results);
    }
}
