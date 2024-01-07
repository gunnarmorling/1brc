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

import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public class CalculateAverage_fragmede {

    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./yaku.txt";// "./measurements.txt";

    private static record Measurement(String station, int value) {
		public static Measurement of(String[] parts) {
			//System.out.printf("string: %s!\n", parts[1]);
			boolean negative = false;
			int idx = 0;
			if (parts[1].charAt(0) == '-') {
				negative = true;
				idx = 1;
			}
			int digit = (parts[1].charAt(idx)-48) * 10;
			int digit2 = 0;
			if (parts[1].charAt(idx+1) != '.') {
				// two digit temperature
				digit = (digit) * 10;
				//System.out.println(parts[1].charAt(idx+1));
				digit2 = (parts[1].charAt(idx+1)-48)*10;
				idx++;
			}
			// if (parts[1].charAt(idx+2) != ".") {
			// 	abort();
			// }
			int frac = parts[1].charAt(idx+2)-48;
			//System.out.printf("parts, %d:%d:%d\n", digit, digit2, frac);
			int value = digit + digit2 + frac;
			if (negative) {
				value = -value;
			}
			//System.out.printf("end value: %d\n", value);
			return new Measurement(parts[0], value);
		}
	}

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return min + "/" + round(mean) + "/" + max;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min / 10.0, agg.sum / 10.0 / agg.count, agg.max / 10.0);
                });

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(args[0]))
                .map(l -> Measurement.of(l.split(";")))
                .collect(groupingBy(Measurement::station, collector)));

        System.out.println(measurements);
    }
}
