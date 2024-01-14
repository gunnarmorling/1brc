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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class CalculateAverage_javamak {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    ;

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

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
                agg -> new ResultRow(agg.min, agg.sum / agg.count, agg.max));

        var path = Paths.get(FILE);

        var a = calcChunks(path).entrySet().parallelStream()
                .flatMap(entry -> getLinesFromFile(path, entry)) // read file for each chunk and get the lines
                .map(l -> new Measurement(l.split(";")))// convert each line to measurement object
                .collect(groupingBy(Measurement::station, collector));
        Map<String, ResultRow> measurements = new TreeMap<>(a);

        System.out.println(measurements);
    }

    private static Stream<String> getLinesFromFile(Path path, Map.Entry<Long, Long> entry) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(entry.getKey());
            ByteBuffer buffer = ByteBuffer.allocate((int) (entry.getValue() - entry.getKey() + 1));
            channel.read(buffer);
            String chunk = new String(buffer.array());
            return Arrays.stream(chunk.split("\n"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Long, Long> calcChunks(Path path) throws IOException {
        long startPos = 0;
        Map<Long, Long> retMap = new HashMap<>();
        while (true) {
            long endPos = calculateEndPosition(path, startPos, 1000 * 5000);
            if (endPos == -1) {
                break;
            }
            long finalStartPos = startPos;
            retMap.put(finalStartPos, endPos);
            startPos = endPos + 1;
        }
        return retMap;
    }

    private static long calculateEndPosition(Path path, long startPos, long chunkSize) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            if (startPos >= channel.size()) {
                return -1;
            }

            long currentPos = startPos + chunkSize;
            if (currentPos >= channel.size()) {
                currentPos = channel.size() - 1;
            }

            channel.position(currentPos);
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int readBytes = channel.read(buffer);

            if (readBytes > 0) {
                for (int i = 0; i < readBytes; i++) {
                    if (buffer.get(i) == '\n') {
                        break;
                    }
                    currentPos++;
                }
            }

            return currentPos;
        }
    }
}
