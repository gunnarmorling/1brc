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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculateAverage_truelive {

    private static final String FILE = "./measurements.txt";
    private static final long CHUNK_SIZE = 1024 * 1024 * 10L;

    private static double getDouble(final byte[] arr, int pos) {
        final int negative = ~(arr[pos] >> 4) & 1;
        int sig = 1;
        sig -= 2 * negative;
        pos += negative;
        final int digit1 = arr[pos] - '0';
        pos++;
        if (arr[pos] == '.') {
            return sig * (digit1 + (arr[pos + 1] - '0') / 10.0);
        }
        else {
            return sig * (digit1 * 10 + (arr[pos] - '0') + (arr[pos + 2] - '0') / 10.0);
        }
    }

    private record Measurement(DoubleAccumulator min, DoubleAccumulator max, DoubleAccumulator sum, LongAdder count) {
        public static Measurement of(final Double initialMeasurement) {
            final Measurement measurement = new Measurement(
                    new DoubleAccumulator(Math::min, initialMeasurement),
                    new DoubleAccumulator(Math::max, initialMeasurement),
                    new DoubleAccumulator(Double::sum, initialMeasurement),
                    new LongAdder()
            );
            measurement.count.increment();
            return measurement;
        }

        public Measurement add(final double measurment) {
            min.accumulate(measurment);
            max.accumulate(measurment);
            sum.accumulate(measurment);
            count.increment();
            return this;
        }

        public String toString() {
            return round(min.doubleValue()) +
                   "/" +
                   round(sum.doubleValue() / count.sum()) +
                   "/" +
                   round(max.doubleValue());
        }

        private double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public static Measurement combineWith(final Measurement m1, final Measurement m2) {
            m1.min.accumulate(m2.min.doubleValue());
            m1.max.accumulate(m2.max.doubleValue());
            m1.sum.accumulate(m2.sum.doubleValue());
            m1.count.add(m2.count.sum());
            return new Measurement(
                    m1.min,
                    m1.max,
                    m1.sum,
                    m1.count
            );
        }
    }

    public static void main(final String[] args) throws IOException {
        // long before = System.currentTimeMillis();
        /**
         * Shoutout to bjhara
         */
        final Iterator<ByteBuffer> iterator = new Iterator<>() {
            final FileChannel in = new FileInputStream(FILE).getChannel();
            final long total = in.size();
            long start;

            @Override
            public boolean hasNext() {
                return start < total;
            }

            @Override
            public ByteBuffer next() {
                try {
                    final MappedByteBuffer mbb = in.map(
                            FileChannel.MapMode.READ_ONLY,
                            start,
                            Math.min(CHUNK_SIZE, total - start));
                    int realEnd = mbb.limit() - 1;
                    while (mbb.get(realEnd) != '\n') {
                        realEnd--;
                    }

                    realEnd++;
                    mbb.limit(realEnd);
                    start += realEnd;

                    return mbb;
                }
                catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        final Map<String, Measurement> reduce = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iterator, Spliterator.IMMUTABLE), true)
                .map(CalculateAverage_truelive::parseBuffer)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        Measurement::combineWith,
                        TreeMap::new));
        System.out.println(reduce);

    }

    private static Map<String, Measurement> parseBuffer(final ByteBuffer bug) {

        final Map<String, Measurement> resultMap = new HashMap<>();
        bug.mark();
        String name = null;
        final byte[] arr = new byte[128];
        int cur = 0;
        while (bug.hasRemaining()) {
            char c = (char) bug.get();
            arr[cur++] = (byte) c;
            while (c != ';') {
                c = (char) bug.get();
                arr[cur++] = (byte) c;
            }
            name = new String(arr, 0, cur - 1);
            cur = 0;
            while (c != '\n') {
                c = (char) bug.get();
                arr[cur++] = (byte) c;
            }
            final double temp = getDouble(arr, 0);
            resultMap.compute(name, (k, v) -> (v == null) ? Measurement.of(temp) : v.add(temp));
            cur = 0;
        }
        return resultMap;
    }

}
