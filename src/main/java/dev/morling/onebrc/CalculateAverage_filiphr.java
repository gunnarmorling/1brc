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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
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
 * Using long parse:                                   0m 11s
 * Using array hash code for city key:                 0m 7.1s (this is invalid since it can lead to hash collisions)
 * Manually compute the value:                         0m 6.8s
 * Revert array hash code for city key:                0m 10s
 * Use array hash and Arrays#equals for city key:      0m 7.2s
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

    private static final String FILE = "./measurements.txt";
    private static final long CHUNK_SIZE = 1024 * 1024 * 10L; // 1KB * 10KB ~ 10MB

    private static final class Measurement {

        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum = 0L;
        private long count = 0L;

        private void add(long value) {
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
            return round(min / 10.0) + "/" + round((sum / 10.0) / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        // long start = System.nanoTime();

        Map<Key, Measurement> measurements;
        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            measurements = fineChannelStream(fileChannel)
                    .parallel()
                    .map(CalculateAverage_filiphr::parseBuffer)
                    .reduce(Collections.emptyMap(), CalculateAverage_filiphr::mergeMaps);
        }

        Map<String, Measurement> finalMeasurements = new TreeMap<>();
        for (Map.Entry<Key, Measurement> entry : measurements.entrySet()) {
            StoredKey key = (StoredKey) entry.getKey();
            Measurement measurement = entry.getValue();
            finalMeasurements.put(new String(key.keyBytes), measurement);
        }

        System.out.println(finalMeasurements);
        // System.out.println("Done in " + (System.nanoTime() - start) / 1000000 + " ms");
    }

    private static Map<Key, Measurement> mergeMaps(Map<Key, Measurement> map1, Map<Key, Measurement> map2) {
        if (map1.isEmpty()) {
            return map2;
        }
        else {
            Set<Key> cities = new HashSet<>(map1.keySet());
            cities.addAll(map2.keySet());
            Map<Key, Measurement> result = HashMap.newHashMap(cities.size());

            for (Key city : cities) {
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
     * This is an adapted implementation of the bjhara parseBuffer.
     * We are using {@code Map<Integer, Measurement>} because creating the string key on every single line is obsolete.
     * Instead, we create a hash key from the string, and we use that as a key in the map.
     */
    private static Map<Key, Measurement> parseBuffer(ByteBuffer bb) {
        Map<Key, Measurement> measurements = HashMap.newHashMap(415);
        int limit = bb.limit();
        byte[] cityBuffer = new byte[128];

        while (bb.position() < limit) {
            int cityBufferIndex = 0;

            // Iterate through the byte buffer and fill the buffer until we find the separator (;)
            // While iterating we are also going to compute the city hash key
            int cityHash = 1;
            while (bb.position() < limit) {
                byte positionByte = bb.get();
                if (positionByte == ';') {
                    break;
                }
                cityBuffer[cityBufferIndex++] = positionByte;
                cityHash = 31 * cityHash + positionByte;
            }

            SearchKey searchKey = new SearchKey(cityBuffer, cityHash, cityBufferIndex);

            byte lastPositionByte = '\n';
            boolean negative = false;
            long value = 0;
            while (bb.position() < limit) {
                byte positionByte = bb.get();
                if (positionByte == '\r' || positionByte == '\n') {
                    lastPositionByte = positionByte;
                    break;
                }
                else if (positionByte == '-') {
                    negative = true;
                }
                else if (positionByte != '.') {
                    // The 0 to 9 characters have an int value of 48 (for 0) to 57 (for 9)
                    // Therefore, in order to compute the digit we subtract with 48
                    int digit = positionByte - 48;
                    // We are computing the value by hand (in order to avoid iterating the index twice)
                    value = value * 10 + digit;
                }
            }

            if (negative) {
                value = -value;
            }

            Measurement measurement = measurements.get(searchKey);
            if (measurement == null) {
                byte[] keyBytes = new byte[cityBufferIndex];
                System.arraycopy(cityBuffer, 0, keyBytes, 0, cityBufferIndex);
                StoredKey storedKey = new StoredKey(keyBytes, cityHash);
                measurement = new Measurement();
                measurements.put(storedKey, measurement);
            }
            measurement.add(value);

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

    /**
     * This is a class that is used to reference a city key using its bytes only.
     * It has the hash precomputed, and it is equal to a {@link SearchKey} when the key bytes are equal to the {@link SearchKey#buffer} up to the {@link SearchKey#limit}.
     */
    private static final class StoredKey implements Key {

        private final byte[] keyBytes;
        private final int hash;

        private StoredKey(byte[] keyBytes, int hash) {
            this.keyBytes = keyBytes;
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (o instanceof SearchKey key) {
                return Arrays.equals(keyBytes, 0, keyBytes.length, key.buffer, 0, key.limit);
            }
            else if (o instanceof StoredKey key) {
                return Arrays.equals(keyBytes, key.keyBytes);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * A class that is used to lookup for a value in a map.
     * This key is equal to {@link StoredKey} when the buffer has the same contents as the {@link StoredKey#keyBytes}.
     */
    private static final class SearchKey implements Key {

        private final byte[] buffer;
        private final int hash;
        private final int limit;

        private SearchKey(byte[] buffer, int hash, int limit) {
            this.buffer = buffer;
            this.hash = hash;
            this.limit = limit;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }

            if (o instanceof StoredKey key) {
                return Arrays.equals(buffer, 0, limit, key.keyBytes, 0, limit);
            }
            else if (o instanceof SearchKey key) {
                return Arrays.equals(buffer, 0, limit, key.buffer, 0, key.limit);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private interface Key {

    }

}
