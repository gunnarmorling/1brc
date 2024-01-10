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

import static java.lang.Double.parseDouble;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * First version.
 * <p>
 * Read the file in parallel with VirtualThreads, and store all bytes in memory.
 * <p>
 * Then process the bytes serially in the main thread (next version should make this parallel).
 * <p>
 * Results:
 * <pre>
 * real    2m34.461s
 * user    2m19.800s
 * sys     0m16.336s
 * </pre>
 */
public class CalculateAverage_Kidlike {

    public static void main(String[] args) {
        File file = new File("./measurements.txt");
        long fileSize = file.length();
        int processors = Runtime.getRuntime().availableProcessors();
        long chunkSize = fileSize / processors;

        var byteBuffers = new ConcurrentHashMap<Long, MappedByteBuffer>(processors, 1, processors);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < processors; i++) {
                long start = i * chunkSize;
                long length = (i == processors - 1) ? fileSize - chunkSize * (processors - 1) : chunkSize;
                executor.execute(() -> {
                    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                        MappedByteBuffer byteBuffer = raf.getChannel().map(MapMode.READ_ONLY, start, length);
                        byteBuffer.load();
                        byteBuffers.put(start, byteBuffer);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        System.out.println(calculateMeasurements(byteBuffers));
    }

    private static TreeMap<String, MeasurementAggregator> calculateMeasurements(
            Map<Long, MappedByteBuffer> buffers) {
        var results = new TreeMap<String, MeasurementAggregator>();

        var state = State.NEXT_READ_CITY;
        var citySink = new CheapByteBuffer(100);
        var measurementSink = new CheapByteBuffer(10);

        var sortedBuffers = new TreeMap<>(buffers);

        for (MappedByteBuffer buffer : sortedBuffers.values()) {
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                char c = (char) b;

                if (c == ';') {
                    state = State.NEXT_READ_MEASUREMENT;
                    continue;
                }

                if (c == '\n') {
                    String city = new String(citySink.getBytes());
                    double measurement = parseDouble(new String(measurementSink.getBytes()));
                    results.compute(city, (k, v) -> {
                        var entry = Optional.ofNullable(v).orElse(new MeasurementAggregator());
                        entry.count++;
                        entry.sum += measurement;
                        if (measurement < entry.min) {
                            entry.min = measurement;
                        }
                        if (measurement > entry.max) {
                            entry.max = measurement;
                        }
                        return entry;
                    });

                    citySink.clear();
                    measurementSink.clear();
                    state = State.NEXT_READ_CITY;
                    continue;
                }

                switch (state) {
                    case NEXT_READ_CITY -> citySink.append(b);
                    case NEXT_READ_MEASUREMENT -> measurementSink.append(b);
                }
            }
        }

        return results;
    }

    private enum State {
        NEXT_READ_CITY,
        NEXT_READ_MEASUREMENT
    }

    private static class MeasurementAggregator {

        private static final DecimalFormat rounder = new DecimalFormat("#.#");

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private long count = 0;

        @Override
        public String toString() {
            return rounder.format(min) + "/" + rounder.format(sum / count) + "/" + rounder.format(max);
        }
    }

    private static class CheapByteBuffer {
        private final byte[] data;
        private int length;

        public CheapByteBuffer(final int startSize) {
            this.data = new byte[startSize];
            this.length = 0;
        }

        public void append(final byte b) {
            data[length++] = b;
        }

        public void clear() {
            this.length = 0;
        }

        public byte[] getBytes() {
            return Arrays.copyOf(this.data, this.length);
        }
    }
}
