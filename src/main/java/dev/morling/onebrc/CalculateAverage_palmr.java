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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CalculateAverage_palmr {
    public static final int CHUNK_SIZE = 1024 * 1024 * 10; // Trial and error showed ~10MB to be a good size on our machine
    public static final int LITTLE_CHUNK_SIZE = 128; // Enough bytes to cover a station name and measurement value :fingers-crossed:
    public static final int STATION_NAME_BUFFER_SIZE = 50;
    public static final int THREAD_COUNT = Math.min(8, Runtime.getRuntime().availableProcessors());

    public static void main(String[] args) throws IOException {

        @SuppressWarnings("resource") // It's faster to leak the file than be well-behaved
        RandomAccessFile file = new RandomAccessFile(Arguments.measurmentsFilename(args), "r");
        FileChannel channel = file.getChannel();
        long fileSize = channel.size();

        long threadChunk = fileSize / THREAD_COUNT;

        Thread[] threads = new Thread[THREAD_COUNT];
        ByteArrayKeyedMap[] results = new ByteArrayKeyedMap[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int j = i;
            long startPoint = j * threadChunk;
            long endPoint = startPoint + threadChunk;
            Thread thread = new Thread(() -> {
                try {
                    results[j] = readAndParse(channel, startPoint, endPoint, fileSize);
                }
                catch (Throwable t) {
                    System.err.println("It's broken :(");
                    // noinspection CallToPrintStackTrace
                    t.printStackTrace();
                }
            });
            threads[i] = thread;
            thread.start();
        }

        final Map<String, MeasurementAggregator> finalAggregator = new TreeMap<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            results[i].getAsUnorderedList().forEach(v -> {
                String stationName = new String(v.stationNameBytes, StandardCharsets.UTF_8);
                finalAggregator.compute(stationName, (_, x) -> {
                    if (x == null) {
                        return v;
                    }
                    else {
                        x.count += v.count;
                        x.min = Math.min(x.min, v.min);
                        x.max = Math.max(x.max, v.max);
                        x.sum += v.sum;
                        return x;
                    }
                });
            });
        }
        System.out.println(finalAggregator);
    }

    private static ByteArrayKeyedMap readAndParse(final FileChannel channel,
                                                  final long startPoint,
                                                  final long endPoint,
                                                  final long fileSize) {
        final State state = new State();

        boolean skipFirstEntry = startPoint != 0;

        long offset = startPoint;
        while (offset < endPoint) {
            parseData(channel, state, offset, Math.min(CHUNK_SIZE, fileSize - offset), false, skipFirstEntry);
            skipFirstEntry = false;
            offset += CHUNK_SIZE;
        }

        if (offset < fileSize) {
            // Make sure we finish reading any partially read entry by going a little in to the next chunk, stopping at the first newline
            parseData(channel, state, offset, Math.min(LITTLE_CHUNK_SIZE, fileSize - offset), true, false);
        }

        return state.aggregators;
    }

    private static void parseData(final FileChannel channel,
                                  final State state,
                                  final long offset,
                                  final long bufferSize,
                                  final boolean stopAtNewline,
                                  final boolean skipFirstEntry) {
        ByteBuffer byteBuffer;
        try {
            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, bufferSize);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        boolean isSkippingToFirstCleanEntry = skipFirstEntry;

        while (byteBuffer.hasRemaining()) {
            byte currentChar = byteBuffer.get();

            if (isSkippingToFirstCleanEntry) {
                if (currentChar == '\n') {
                    isSkippingToFirstCleanEntry = false;
                }

                continue;
            }

            if (currentChar == ';') {
                state.parsingValue = true;
            }
            else if (currentChar == '\n') {
                if (state.stationPointerEnd != 0) {
                    double value = state.measurementValue * state.exponent;

                    MeasurementAggregator aggregator = state.aggregators.computeIfAbsent(state.stationBuffer, state.stationPointerEnd, state.signedHashCode);
                    aggregator.count++;
                    aggregator.min = Math.min(aggregator.min, value);
                    aggregator.max = Math.max(aggregator.max, value);
                    aggregator.sum += value;
                }

                if (stopAtNewline) {
                    return;
                }

                // reset
                state.reset();
            }
            else {
                if (!state.parsingValue) {
                    state.stationBuffer[state.stationPointerEnd++] = currentChar;
                    state.signedHashCode = 31 * state.signedHashCode + (currentChar & 0xff);
                }
                else {
                    if (currentChar == '-') {
                        state.exponent = -0.1;
                    }
                    else if (currentChar != '.') {
                        state.measurementValue = state.measurementValue * 10 + (currentChar - '0');
                    }
                }
            }
        }
    }

    static final class State {
        ByteArrayKeyedMap aggregators = new ByteArrayKeyedMap();
        boolean parsingValue = false;
        byte[] stationBuffer = new byte[STATION_NAME_BUFFER_SIZE];
        int signedHashCode = 0;
        int stationPointerEnd = 0;
        double measurementValue = 0;
        double exponent = 0.1;

        public void reset() {
            parsingValue = false;
            signedHashCode = 0;
            stationPointerEnd = 0;
            measurementValue = 0;
            exponent = 0.1;
        }
    }

    private static class MeasurementAggregator {
        final byte[] stationNameBytes;
        final int stationNameHashCode;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator(final byte[] stationNameBytes, final int stationNameHashCode) {
            this.stationNameBytes = stationNameBytes;
            this.stationNameHashCode = stationNameHashCode;
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class ByteArrayKeyedMap {
        private final int BUCKET_COUNT = 0xFFF; // 413 unique stations in the data set, & 0xFFF ~= 399 (only 14 collisions (given our hashcode implementation))
        private final MeasurementAggregator[] buckets = new MeasurementAggregator[BUCKET_COUNT + 1];
        private final List<MeasurementAggregator> compactUnorderedBuckets = new ArrayList<>(413);

        public MeasurementAggregator computeIfAbsent(final byte[] key, final int keyLength, final int keyHashCode) {
            int index = keyHashCode & BUCKET_COUNT;

            while (true) {
                MeasurementAggregator maybe = buckets[index];
                if (maybe == null) {
                    final byte[] copiedKey = Arrays.copyOf(key, keyLength);
                    MeasurementAggregator measurementAggregator = new MeasurementAggregator(copiedKey, keyHashCode);
                    buckets[index] = measurementAggregator;
                    compactUnorderedBuckets.add(measurementAggregator);
                    return measurementAggregator;
                }
                else {
                    if (Arrays.equals(key, 0, keyLength, maybe.stationNameBytes, 0, maybe.stationNameBytes.length)) {
                        return maybe;
                    }
                    index++;
                    index &= BUCKET_COUNT;
                }
            }
        }

        public List<MeasurementAggregator> getAsUnorderedList() {
            return compactUnorderedBuckets;
        }
    }
}
