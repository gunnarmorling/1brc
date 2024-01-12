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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class CalculateAverage_omarchenko4j {
    private static final String FILE = "./measurements.txt";
    private static final int NUMBER_OF_CORES = Runtime.getRuntime().availableProcessors();
    private static final int MAX_LINE_SIZE = 128;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        try (var file = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            var fileSize = file.size();
            try (var arena = Arena.ofShared()) {
                var segment = file.map(MapMode.READ_ONLY, 0, fileSize, arena);

                var tasks = sliceIntoTasks(segment);
                try (var executor = Executors.newFixedThreadPool(Math.min(tasks.size(), NUMBER_OF_CORES))) {
                    var futures = executor.invokeAll(tasks);

                    var measurements = new TreeMap<String, Aggregator>(String::compareTo);
                    for (var future : futures) {
                        var result = future.get();
                        for (var entry : result.entrySet()) {
                            var station = entry.getKey();
                            var a1 = entry.getValue();
                            var a2 = measurements.get(station);
                            if (a2 != null) {
                                a1.merge(a2);
                            }
                            measurements.put(station, a1);
                        }
                    }
                    System.out.println(measurements);
                }
            }
        }
    }

    private static List<Task> sliceIntoTasks(MemorySegment segment) {
        var tasks = new ArrayList<Task>(NUMBER_OF_CORES);

        var segmentSize = segment.byteSize();
        var chunkSize = segmentSize / NUMBER_OF_CORES;
        if (chunkSize < (long) NUMBER_OF_CORES * MAX_LINE_SIZE) {
            return List.of(new Task(segment, 0, segmentSize));
        }

        long offsetStart = 0;
        for (int coreNumber = 1; coreNumber <= NUMBER_OF_CORES; coreNumber++) {
            long offsetEnd = chunkSize * coreNumber;
            while (offsetEnd < segmentSize) {
                var b = segment.get(ValueLayout.JAVA_BYTE, offsetEnd);
                offsetEnd++;
                if (b == '\n') {
                    break;
                }
            }

            tasks.add(new Task(segment, offsetStart, offsetEnd));

            offsetStart = offsetEnd;
        }

        return tasks;
    }

    private static class Task implements Callable<Map<String, Aggregator>> {
        private static final byte SEPARATOR = ';';
        private static final byte END_LINE = '\n';

        private final MemorySegment segment;
        private final long startOffset;
        private final long endOffset;

        private Task(MemorySegment segment, long startOffset, long endOffset) {
            this.segment = segment;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public Map<String, Aggregator> call() {
            var measurements = new HashMap<String, Aggregator>(512);

            int startIndex = 0;
            int separatorIndex = 0;
            int endIndex = 0;

            var buffer = new byte[MAX_LINE_SIZE];
            int bufferIndex = 0;

            for (long offset = startOffset; offset < endOffset; offset++) {
                var b = segment.get(ValueLayout.JAVA_BYTE, offset);
                buffer[bufferIndex] = b;
                bufferIndex++;

                if (b == SEPARATOR) {
                    separatorIndex = bufferIndex - 1;
                    continue;
                }
                if (b == END_LINE) {
                    endIndex = bufferIndex - 1;

                    var station = new String(buffer, startIndex, separatorIndex);
                    var value = getDouble(buffer, separatorIndex + 1, endIndex - separatorIndex - 1);
                    measurements.computeIfAbsent(station, ignored -> new Aggregator()).addValue(value);

                    bufferIndex = 0;
                }
            }

            return measurements;
        }

        private static final byte MINUS = '-';
        private static final byte ZERO = '0';

        private double getDouble(byte[] buffer, int offset, int length) {
            if (length == 4) {
                if (buffer[offset] == MINUS) {
                    int value;
                    value = buffer[offset + 1] - ZERO;
                    value = (value * 10) + (buffer[offset + 3] - ZERO);
                    value = -value;
                    return value * .1D;
                }

                int value;
                value = buffer[offset] - ZERO;
                value = (value * 10) + (buffer[offset + 1] - ZERO);
                value = (value * 10) + (buffer[offset + 3] - ZERO);
                return value * .1D;
            }
            if (length == 3) {
                int value;
                value = buffer[offset] - ZERO;
                value = (value * 10) + (buffer[offset + 2] - ZERO);
                return value * .1D;
            }

            int value;
            value = buffer[offset + 1] - ZERO;
            value = (value * 10) + (buffer[offset + 2] - ZERO);
            value = (value * 10) + (buffer[offset + 4] - ZERO);
            value = -value;
            return value * .1D;
        }
    }

    public static class Aggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public void addValue(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        public void merge(Aggregator aggregator) {
            if (aggregator.min < min) {
                min = aggregator.min;
            }
            if (aggregator.max > max) {
                max = aggregator.max;
            }
            sum += aggregator.sum;
            count += aggregator.count;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
