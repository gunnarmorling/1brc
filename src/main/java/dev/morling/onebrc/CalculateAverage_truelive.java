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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculateAverage_truelive {
    private static final long CHUNK_SIZE = 1024 * 1024 * 10L;

    private static int branchlessParseInt(final byte[] input, final int length) {
        // 0 if positive, 1 if negative
        final int negative = ~(input[0] >> 4) & 1;
        // 0 if nr length is 3, 1 if length is 4
        final int has4 = ((length - negative) >> 2) & 1;

        final int digit1 = input[negative] - '0';
        final int digit2 = input[negative + has4] - '0';
        final int digit3 = input[2 + negative + has4] - '0';

        return (-negative ^ (has4 * (digit1 * 100) + digit2 * 10 + digit3) - negative);
    }

    // branchless max (unprecise for large numbers, but good enough)
    static int max(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    // branchless min (unprecise for large numbers, but good enough)
    static int min(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
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

    private static Map<String, Measurement> combineMaps(
                                                        final Map<String, Measurement> map1,
                                                        final Map<String, Measurement> map2) {
        for (final var entry : map2.entrySet()) {
            map1.merge(entry.getKey(), entry.getValue(), Measurement::combineWith);
        }

        return map1;
    }

    public static void main(final String[] args) throws IOException {
        // long before = System.currentTimeMillis();
        /**
         * Shoutout to bjhara
         */
        final Iterator<ByteBuffer> iterator = new Iterator<>() {
            final FileChannel in = new FileInputStream(Arguments.measurmentsFilename(args)).getChannel();
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
                .parallel()
                .map(CalculateAverage_truelive::parseBuffer)
                .reduce(CalculateAverage_truelive::combineMaps).get();

        System.out.print("{");
        System.out.print(
                reduce
                        .entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")));
        System.out.println("}");

        // System.out.println("Took: " + (System.currentTimeMillis() - before));

    }

    private static Map<String, Measurement> parseBuffer(final ByteBuffer bug) {

        final Map<String, Measurement> resultMap = new HashMap<>();
        bug.mark();
        String name = null;
        final byte[] arr = new byte[128];
        while (bug.hasRemaining()) {
            final char c = (char) bug.get();
            if (c == ';') {
                final int pos = bug.position();
                bug.reset();
                final int len = pos - bug.position() - 1;
                bug.get(bug.position(), arr, 0, len);
                name = new String(arr, 0, len);
                bug.position(pos);
                bug.mark();
            }
            else if (c == '\n') {
                final int pos = bug.position();
                bug.reset();
                final int len = pos - bug.position();
                bug.get(bug.position(), arr, 0, len);
                final double temp = Double.parseDouble(new String(arr, 0, len));
                resultMap.compute(name, (k, v) -> (v == null) ? Measurement.of(temp) : v.add(temp));
                bug.position(pos);
                bug.mark();
            }
        }
        return resultMap;
    }
}
