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
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.TreeMap;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingByConcurrent;

// gunnar morling - 2:10
// roy van rijn -   1:01
//                  0:53

public class CalculateAverage_ddimtirov {
    public record InputLine(String station, int value) {

        public static InputLine fromLine(String line) {
            int endOfText = line.indexOf(";");

            String station = line.substring(0, endOfText);

            int startOfWhole = endOfText + 1;
            int sign;
            if (line.charAt(startOfWhole) == '-') {
                sign = -1;
                startOfWhole++;
            } else {
                sign = 1;
            }

            int endOfWhole = line.lastIndexOf(".");
            var whole = unsafeParsePositiveInt(line, startOfWhole, endOfWhole);
            var decimal = unsafeParsePositiveInt(line,endOfWhole+1, line.length());
            int fixpoint10 = (whole * 10  + decimal) * sign;

            return new InputLine(station, fixpoint10);
        }

        static int unsafeParsePositiveInt(String s, int start, int end) {
            int acc = 0;
            for (int i = start; i<end; i++) {
                if (acc != 0) acc *= 10;
                char c = s.charAt(i);
                var v = c - '0';
                assert v>=0 && v<=9 : String.format("Character '%s', value %,d", c, v);
                acc += v;
            }
            return acc;
        }
    }

    private static class OutputMetrics {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum;
        private long count;

        @SuppressWarnings("ManualMinMaxCalculation")
        public OutputMetrics combine(OutputMetrics o) {
            var r = new OutputMetrics();
            r.min = min < o.min ? min : o.min;
            r.max = max > o.max ? max : o.max;
            r.sum = sum + o.sum;
            r.count = count + o.count;
            return r;
        }

        public void accumulate(InputLine m) {
            if (m.value < min) min = m.value;
            if (m.value > max) max = m.value;
            sum += m.value;
            count++;
        }

        @Override
        public String toString() {
            var min = this.min / 10.0;
            var mean = Math.round(this.sum / (double) count) / 10.0;
            var max = this.max / 10.0;
            return min + "/" + mean + "/" + max;
        }
    }

    public static void main(String[] args) throws IOException {
//         var start = Instant.now();
        Instant start = null;
        var path = Path.of("./measurements.txt");
        var buffer = 8192 * 20;

        // Files.lines() is optimized for files that can be indexed by int
        // For larger files it falls back to buffered reader, which we now
        // use directly to be able to tweak the buffer size.
        try (var reader = new BufferedReader(new InputStreamReader(Files.newInputStream(path)), buffer)) {
            var stationsToMetrics = reader.lines()
                    .map(InputLine::fromLine)
                    .parallel()
                    .collect(groupingByConcurrent(InputLine::station, Collector.of(
                            OutputMetrics::new,
                            OutputMetrics::accumulate,
                            OutputMetrics::combine,
                            OutputMetrics::toString
                    )));
            System.out.println(new TreeMap<>(stationsToMetrics));
            assert Files.readAllLines(Path.of("expected_result.txt")).getFirst().equals(new TreeMap<>(stationsToMetrics).toString());
        }

        //noinspection ConstantValue
        if (start!=null) System.err.println(Duration.between(start, Instant.now()));
    }

}
