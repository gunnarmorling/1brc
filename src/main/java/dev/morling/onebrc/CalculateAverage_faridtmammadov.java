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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_faridtmammadov {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        int availableProcessors = Runtime.getRuntime().availableProcessors();

        var map = getSegments(availableProcessors).stream()
                .map(CalculateAverage_faridtmammadov::aggregate).parallel()
                .flatMap(f -> f.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Aggregate::update, TreeMap::new));

        printFormatted(map);
    }

    private static List<MemorySegment> getSegments(int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            var fileSize = fileChannel.size();
            var segmentSize = fileSize / numberOfChunks;
            var segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            var baseAddress = segment.address();
            var endAddress = baseAddress + fileSize;
            var segments = new ArrayList<MemorySegment>();
            var startAddress = baseAddress;

            for (var i = 0; i < numberOfChunks; i++) {
                var pointer = startAddress + segmentSize;
                while (pointer < endAddress) {
                    long offset = pointer - baseAddress;
                    byte b = segment.get(ValueLayout.JAVA_BYTE, offset);
                    if (b == '\n') {
                        break;
                    }
                    pointer++;
                }
                if (pointer >= endAddress) {
                    var offsetStart = startAddress - baseAddress;
                    var offsetEnd = endAddress - baseAddress - offsetStart;
                    segments.add(segment.asSlice(offsetStart, offsetEnd));
                    break;
                }
                var offsetStart = startAddress - baseAddress;
                var offsetEnd = pointer - baseAddress - offsetStart;
                segments.add(segment.asSlice(offsetStart, offsetEnd));
                startAddress = pointer + 1;
            }

            return segments;
        }
    }

    private static Map<String, Aggregate> aggregate(MemorySegment segment) {
        var map = new HashMap<String, Aggregate>();
        var iterator = new MemorySegmentIterator(segment);

        while (iterator.hasNext()) {
            String city = parseCity(iterator);
            long temperature = parseTemperature(iterator);

            map.compute(city, (key, value) -> {
                if (value == null) {
                    return new Aggregate(temperature);
                }
                else {
                    return value.update(temperature);
                }
            });
        }

        return map;
    }

    private static String parseCity(MemorySegmentIterator iterator) {
        var byteStream = new ByteArrayOutputStream();
        while (iterator.hasNext()) {
            var b = iterator.getNextByte();
            if (b == ';') {
                return byteStream.toString(StandardCharsets.UTF_8);
            }
            byteStream.write(b);
        }

        return null;
    }

    public static long parseTemperature(MemorySegmentIterator iterator) {
        long value = 0L;
        int sign = 1;
        while (iterator.hasNext()) {
            byte b = iterator.getNextByte();
            if (b >= '0' && b <= '9') {
                value = value * 10 + b - '0';
            }
            else if (b == '\n') {
                return value * sign;
            }
            else if (b == '-') {
                sign = -1;
            }
        }

        return value * sign;
    }

    private static void printFormatted(Map<String, Aggregate> map) {
        var iterator = map.entrySet().iterator();
        var length = map.entrySet().size();
        System.out.print("{");
        for (int i = 0; i < length - 1; i++) {
            var entry = iterator.next();
            System.out.printf("%s=%s, ", entry.getKey(), entry.getValue().toString());
        }
        var lastEntry = iterator.next();
        System.out.printf("%s=%s}\n", lastEntry.getKey(), lastEntry.getValue().toString());
    }

    static class Aggregate {
        long min;
        long max;
        long sum;
        int count;

        public Aggregate(long temperature) {
            min = temperature;
            max = temperature;
            sum = temperature;
            count = 1;
        }

        public Aggregate update(long temp) {
            min = Math.min(min, temp);
            max = Math.max(max, temp);
            sum += temp;
            count++;
            return this;
        }

        public Aggregate update(Aggregate agg) {
            min = Math.min(min, agg.min);
            max = Math.max(max, agg.max);
            sum += agg.sum;
            count += agg.count;
            return this;
        }

        public String toString() {
            return String.format("%s/%s/%s", min / 10.0f, Math.round(sum * 1.0f / count) / 10.0f, max / 10.0f);
        }
    }

    static class MemorySegmentIterator {
        private long offset;
        private final MemorySegment segment;
        private final long segmentSize;

        public MemorySegmentIterator(MemorySegment segment) {
            this.segment = segment;
            this.segmentSize = segment.byteSize();
        }

        public boolean hasNext() {
            return offset < segmentSize;
        }

        public byte getNextByte() {
            var b = segment.get(ValueLayout.JAVA_BYTE, offset);
            offset++;
            return b;
        }
    }
}