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
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Initial submission:                                 1m 35s
 * Adding memory mapped files:                         0m 55s (based on bjhara's submission)
 * Using big decimal and iterating the buffer once:    0m 20s
 * <p>
 * Using 21.0.1 Temurin with ShenandoahGC on Macbook (Intel) Pro
 * `sdk use java 21.0.1-tem`
 *
 * When using Oracle GraalVM 21.0.1+12.1
 * `sdk use java 21.0.1-graal`
 * It takes 0m 15s on my machine
 * `sdk use java 21.0.1-graalce`
 * It takes 0m 20s on my machine
 *
 * @author Filip Hrisafov
 */
public class CalculateAverage_filiphr {

    private static final long CHUNK_SIZE = 1024 * 1024 * 10L; // 1KB * 10KB ~ 10MB

    private static final class Measurement {

        private double min = Long.MAX_VALUE;
        private double max = Long.MIN_VALUE;
        private double sum = 0L;
        private long count = 0L;

        private void add(double value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public static Measurement combine(Measurement m1, Measurement m2) {
            Measurement measurement = new Measurement();
            measurement.min = Math.min(m1.min, m2.min);
            measurement.max = Math.max(m1.max, m2.max);
            measurement.sum = m1.sum + m2.sum;
            measurement.count = m1.count + m2.count;
            return measurement;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round((sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        // long start = System.nanoTime();

        Map<String, Measurement> measurements;
        try (FileChannel fileChannel = FileChannel.open(Arguments.measurmentsPath(args), StandardOpenOption.READ)) {
            measurements = fineChannelStream(fileChannel)
                    .parallel()
                    .map(CalculateAverage_filiphr::parseBuffer)
                    .reduce(Collections.emptyMap(), CalculateAverage_filiphr::mergeMaps);
        }

        System.out.println(new TreeMap<>(measurements));
        // System.out.println("Done in " + (System.nanoTime() - start) / 1000000 + " ms");
    }

    private static Map<String, Measurement> mergeMaps(Map<String, Measurement> map1, Map<String, Measurement> map2) {
        if (map1.isEmpty()) {
            return map2;
        }
        else {
            Set<String> cities = new HashSet<>(map1.keySet());
            cities.addAll(map2.keySet());
            Map<String, Measurement> result = HashMap.newHashMap(cities.size());

            for (String city : cities) {
                Measurement m1 = map1.get(city);
                Measurement m2 = map2.get(city);
                if (m2 == null) {
                    // When m2 is null then it is not possible for m1 to be null as well,
                    // since cities is a union of the map key sets
                    result.put(city, m1);
                }
                else if (m1 == null) {
                    // When m1 is null then it is not possible for m2 to be null as well,
                    // since cities is a union of the map key sets
                    result.put(city, m2);
                }
                else {
                    result.put(city, Measurement.combine(m1, m2));
                }
            }

            return result;
        }
    }

    /**
     * This is an adapted implementation of the bjhara parseBuffer
     */
    private static Map<String, Measurement> parseBuffer(ByteBuffer bb) {
        Map<String, Measurement> measurements = HashMap.newHashMap(415);
        int limit = bb.limit();
        byte[] buffer = new byte[128];
        CharBuffer charBuffer = CharBuffer.allocate(8);

        while (bb.position() < limit) {
            int bufferIndex = 0;

            // Iterate through the byte buffer and fill the buffer until we find the separator (;)
            while (bb.position() < limit) {
                byte positionByte = bb.get();
                if (positionByte == ';') {
                    break;
                }
                buffer[bufferIndex++] = positionByte;
            }

            // Create the city
            String city = new String(buffer, 0, bufferIndex);

            charBuffer.clear();
            byte lastPositionByte = '\n';
            while (bb.position() < limit) {
                byte positionByte = bb.get();
                if (positionByte == '\r' || positionByte == '\n') {
                    lastPositionByte = positionByte;
                    break;
                }
                charBuffer.append((char) positionByte);
            }

            int position = charBuffer.position();
            charBuffer.position(0);
            // Create the temperature string
            BigDecimal bigDecimal = new BigDecimal(charBuffer.array(), 0, position);
            double value = bigDecimal.doubleValue();

            measurements.computeIfAbsent(city, k -> new Measurement())
                    .add(value);

            // and get rid of the new line (handle both kinds)
            if (lastPositionByte == '\r') {
                bb.get();
            }
        }

        return measurements;
    }

    /**
     * Thanks to bjhara and royvanrijn for the idea of using (and learning about) memory mapped files.
     */
    private static Stream<ByteBuffer> fineChannelStream(FileChannel fileChannel) throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(fileChannelIterator(fileChannel), Spliterator.IMMUTABLE), false);
    }

    private static Iterator<ByteBuffer> fileChannelIterator(FileChannel fileChannel) throws IOException {
        return new Iterator<>() {

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
        };
    }
}
