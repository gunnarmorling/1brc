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

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class CalculateAverage_twobiers {

    private static final String FILE = "./measurements.txt";
    private static final FastAveragingCollector FAST_AVERAGING_COLLECTOR = new FastAveragingCollector();
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_256;

    private static class FastAveragingCollector implements Collector<String[], double[], Double> {
        @Override
        public Supplier<double[]> supplier() {
            return () -> new double[4];
        }

        @Override
        public BiConsumer<double[], String[]> accumulator() {
            return (a, t) -> {
                double val = fastParseDouble(t[1]);
                sumWithCompensation(a, val);
                a[2]++;
                a[3] += val;
            };
        }

        @Override
        public BinaryOperator<double[]> combiner() {
            return (a, b) -> {
                sumWithCompensation(a, b[0]);
                // Subtract compensation bits
                sumWithCompensation(a, -b[1]);
                a[2] += b[2];
                a[3] += b[3];
                return a;
            };
        }

        @Override
        public Function<double[], Double> finisher() {
            return a -> (a[2] == 0) ? 0.0d : Math.round(((a[0] + a[1]) / a[2]) * 10.0) / 10.0;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }

    public static void main(String[] args) throws IOException {
        TreeMap<String, Double> measurements = Files.lines(Paths.get(FILE))
                .parallel()
                .map(l -> fastSplit(l))
                .collect(
                        groupingBy(
                                m -> m[0],
                                TreeMap::new,
                                FAST_AVERAGING_COLLECTOR));

        System.out.println(measurements);
    }

    private static String[] fastSplit(String str) {
        var splitArray = new String[2];
        var chars = str.toCharArray();

        int i = 0;
        for (char c : chars) {
            if (c == ';') {
                splitArray[0] = new String(Arrays.copyOfRange(chars, 0, i));
                break;
            }
            i++;
        }

        splitArray[1] = new String(Arrays.copyOfRange(chars, i + 1, chars.length));
        return splitArray;
    }

    private static Double fastParseDouble(String str) {
        long value = 0;
        int exp = 0;
        boolean negative = false;
        int decimalPlaces = Integer.MIN_VALUE;
        var chars = str.toCharArray();
        for (char ch : chars) {
            if (ch >= '0' && ch <= '9') {
                value = value * 10 + (ch - '0');
                decimalPlaces++;
            }
            else if (ch == '-') {
                negative = true;
            }
            else if (ch == '.') {
                decimalPlaces = 0;
            }
        }

        return asDouble(value, exp, negative, decimalPlaces);
    }

    private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
        if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
            if (value < Long.MAX_VALUE / (1L << 32)) {
                exp -= 32;
                value <<= 32;
            }
            if (value < Long.MAX_VALUE / (1L << 16)) {
                exp -= 16;
                value <<= 16;
            }
            if (value < Long.MAX_VALUE / (1L << 8)) {
                exp -= 8;
                value <<= 8;
            }
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
            }
        }
        for (; decimalPlaces > 0; decimalPlaces--) {
            exp--;
            long mod = value % 5;
            value /= 5;
            int modDiv = 1;
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
                modDiv <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
                modDiv <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
                modDiv <<= 1;
            }
            if (decimalPlaces > 1) {
                value += modDiv * mod / 5;
            }
            else {
                value += (modDiv * mod + 4) / 5;
            }
        }
        final double d = Math.scalb((double) value, exp);
        return negative ? -d : d;
    }

    private static double[] sumWithCompensation(double[] intermediateSum, double value) {
        double tmp = value - intermediateSum[1];
        double sum = intermediateSum[0];
        double velvel = sum + tmp; // Little wolf of rounding error
        intermediateSum[1] = (velvel - sum) - tmp;
        intermediateSum[0] = velvel;
        return intermediateSum;
    }

}
