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

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class CalculateAverage_bjhara {
    private static class Measurement {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public static Measurement combine(Measurement m1, Measurement m2) {
            var mres = new Measurement();
            mres.min = m1.min < m2.min ? m1.min : m2.min;
            mres.max = m1.max > m2.max ? m1.max : m2.max;
            mres.sum = m1.sum + m2.sum;
            mres.count = m1.count + m2.count;
            return mres;
        }
    }

    public static void main(String[] args) throws IOException {
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Arguments.measurmentsPath(args),
                EnumSet.of(StandardOpenOption.READ))) {

            var cities = splitFileChannel(fileChannel)
                    .parallel()
                    .map(CalculateAverage_bjhara::parseBuffer)
                    .collect(Collectors.reducing(CalculateAverage_bjhara::combineMaps));

            var sortedCities = new TreeMap<>(cities.orElseThrow());
            System.out.println(sortedCities);
        }
    }

    private static Map<String, Measurement> combineMaps(Map<String, Measurement> map1,
                                                        Map<String, Measurement> map2) {
        for (var entry : map2.entrySet()) {
            map1.merge(entry.getKey(), entry.getValue(), Measurement::combine);
        }

        return map1;
    }

    private static Stream<ByteBuffer> splitFileChannel(final FileChannel fileChannel) throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<ByteBuffer>() {
            private static final long CHUNK_SIZE = 1024 * 1024 * 10L;

            private final long size = fileChannel.size();
            private long start = 0;

            @Override
            public boolean hasNext() {
                return start < size;
            }

            @Override
            public ByteBuffer next() {
                try {
                    MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start,
                            Math.min(CHUNK_SIZE, size - start));

                    // don't split the data in the middle of lines
                    // find the closest previous newline
                    int realEnd = mappedByteBuffer.limit() - 1;
                    while (mappedByteBuffer.get(realEnd) != '\n')
                        realEnd--;

                    realEnd++;

                    mappedByteBuffer.limit(realEnd);
                    start += realEnd;

                    return mappedByteBuffer;
                }
                catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        }, Spliterator.IMMUTABLE), false);
    }

    private static Map<String, Measurement> parseBuffer(ByteBuffer bb) {
        Map<String, Measurement> cities = new HashMap<>();

        final int limit = bb.limit();
        final byte[] buffer = new byte[128];

        while (bb.position() < limit) {
            final int currentPosition = bb.position();

            // find the ; separator
            int separator = currentPosition;
            while (separator != limit && bb.get(separator) != ';')
                separator++;

            // find the end of the line
            int end = separator + 1;
            while (end != limit && !Character.isWhitespace((char) bb.get(end)))
                end++;

            // get the name as a string
            int nameLength = separator - currentPosition;
            bb.get(buffer, 0, nameLength);
            String city = new String(buffer, 0, nameLength);

            // get rid of the separator
            bb.get();

            // get the double value
            int valueLength = end - separator - 1;
            bb.get(buffer, 0, valueLength);
            String valueStr = new String(buffer, 0, valueLength);
            double value = Double.parseDouble(valueStr);

            // and get rid of the new line (handle both kinds)
            byte newline = bb.get();
            if (newline == '\r')
                bb.get();

            // update the map with the new measurement
            Measurement agg = cities.get(city);
            if (agg == null) {
                agg = new Measurement();
                cities.put(city, agg);
            }

            agg.min = agg.min < value ? agg.min : value;
            agg.max = agg.max > value ? agg.max : value;
            agg.sum += value;
            agg.count++;
        }

        return cities;
    }
}
