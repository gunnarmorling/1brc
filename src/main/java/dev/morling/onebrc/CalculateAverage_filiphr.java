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
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Initial submission:          1m 35s
 * Adding memory mapped files:  0m 55s (based on bjhara's submission)
 *
 * <p>
 * Using 21.0.1 Temurin with ShenandoahGC on Macbook (Intel) Pro
 * `sdk use java 21.0.1-tem`
 *
 * @author Filip Hrisafov
 */
public class CalculateAverage_filiphr {

    private static final String FILE = "./measurements.txt";
    private static final long CHUNK_SIZE = 1024 * 1024 * 10L; // 1KB * 10KB ~ 10MB

    private static final class Measurement {

        private final AtomicReference<Double> min = new AtomicReference<>(Double.POSITIVE_INFINITY);
        private final AtomicReference<Double> max = new AtomicReference<>(Double.NEGATIVE_INFINITY);
        private final AtomicReference<Double> sum = new AtomicReference<>(0d);
        private final AtomicLong count = new AtomicLong(0);

        private Measurement combine(double value) {
            this.min.accumulateAndGet(value, Math::min);
            this.max.accumulateAndGet(value, Math::max);
            this.sum.accumulateAndGet(value, Double::sum);
            this.count.incrementAndGet();
            return this;
        }

        @Override
        public String toString() {
            return round(min.get()) + "/" + round(sum.get() / count.get()) + "/" + round(max.get());
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        // long start = System.nanoTime();

        Map<String, Measurement> measurements = new ConcurrentHashMap<>(700);
        try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            fineChannelStream(fileChannel)
                    .parallel()
                    .forEach(byteBuffer -> {
                        processBuffer(byteBuffer, (city, temperature) -> {
                            measurements.computeIfAbsent(city, k -> new Measurement())
                                    .combine(temperature);
                        });
                    });
        }

        System.out.println(new TreeMap<>(measurements));
        // System.out.println("Done in " + (System.nanoTime() - start) / 1000000 + " ms");
    }

    /**
     * This is an adapted implementation of the bjhara parseBuffer
     */
    private static void processBuffer(ByteBuffer bb, BiConsumer<String, Double> entryConsumer) {
        int limit = bb.limit();
        byte[] buffer = new byte[128];

        while (bb.position() < limit) {
            int currentPosition = bb.position();

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

            entryConsumer.accept(city, value);

            // and get rid of the new line (handle both kinds)
            byte newline = bb.get();
            if (newline == '\r')
                bb.get();
        }
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
