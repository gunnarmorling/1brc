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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculateAverage_palmr {
    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_SIZE = 1024 * 1024 * 10; // Trial and error showed ~10MB to be a good size on our machine
    private static final int STATION_NAME_BUFFER_SIZE = 128;
    private static final int THREAD_COUNT = Math.min(8, Runtime.getRuntime().availableProcessors());
    private static final char SEPARATOR_CHAR = ';';
    private static final char END_OF_RECORD = '\n';
    private static final char MINUS_CHAR = '-';
    private static final char DECIMAL_POINT_CHAR = '.';

    public static void main(String[] args) throws IOException {

        final var file = new RandomAccessFile(FILE, "r");
        final var channel = file.getChannel();

        final TreeMap<String, MeasurementAggregator> results = StreamSupport.stream(ThreadChunk.chunk(file, THREAD_COUNT), true)
                .map(chunk -> parseChunk(chunk, channel))
                .flatMap(bakm -> bakm.getAsUnorderedList().stream())
                .collect(Collectors.toMap(m -> new String(m.stationNameBytes, StandardCharsets.UTF_8), m -> m, MeasurementAggregator::merge, TreeMap::new));
        System.out.println(results);
    }

    private record ThreadChunk(long startPoint, long endPoint, long size) {
        public static Spliterator<CalculateAverage_palmr.ThreadChunk> chunk(final RandomAccessFile file, final int chunkCount) throws IOException {
            final var fileSize = file.length();
            final var idealChunkSize = Math.max(CHUNK_SIZE, fileSize / THREAD_COUNT);
            final var chunks = new CalculateAverage_palmr.ThreadChunk[chunkCount];

            var validChunks = 0;
            var startPoint = 0L;
            for (int i = 0; i < chunkCount; i++) {
                var endPoint = Math.min(startPoint + idealChunkSize, fileSize);
                if (startPoint + idealChunkSize < fileSize)
                {
                    file.seek(endPoint);
                    while (endPoint++ < fileSize && file.readByte() != END_OF_RECORD) {
                        Thread.onSpinWait();
                    }
                }

                final var actualSize = endPoint - startPoint;
                if (actualSize > 1) {
                    chunks[i] = new CalculateAverage_palmr.ThreadChunk(startPoint, endPoint, actualSize);
                    startPoint += actualSize;
                    validChunks++;
                }
                else {
                    break;
                }
            }

            return Spliterators.spliterator(chunks, 0, validChunks,
                    Spliterator.ORDERED |
                            Spliterator.DISTINCT |
                            Spliterator.SORTED |
                            Spliterator.NONNULL |
                            Spliterator.IMMUTABLE |
                            Spliterator.CONCURRENT
            );
        }
    }

    private static ByteArrayKeyedMap parseChunk(ThreadChunk chunk, FileChannel channel) {
        final var state = new State();

        var offset = chunk.startPoint;
        while (offset < chunk.endPoint) {
            parseData(channel, state, offset, Math.min(CHUNK_SIZE, chunk.endPoint - offset));
            offset += CHUNK_SIZE;
        }

        return state.aggregators;
    }

    private static void parseData(final FileChannel channel,
                                  final State state,
                                  final long offset,
                                  final long bufferSize) {
        final ByteBuffer byteBuffer;
        try {
            byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, bufferSize);

            while (byteBuffer.hasRemaining()) {
                final var currentChar = byteBuffer.get();

                if (currentChar == SEPARATOR_CHAR) {
                    state.parsingValue = true;
                }
                else if (currentChar == END_OF_RECORD) {
                    if (state.stationPointerEnd != 0) {
                        final var value = state.measurementValue * state.exponent;

                        MeasurementAggregator aggregator = state.aggregators.computeIfAbsent(state.stationBuffer, state.stationPointerEnd, state.signedHashCode);
                        aggregator.count++;
                        aggregator.min = Math.min(aggregator.min, value);
                        aggregator.max = Math.max(aggregator.max, value);
                        aggregator.sum += value;
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
                        if (currentChar == MINUS_CHAR) {
                            state.exponent = -0.1;
                        }
                        else if (currentChar != DECIMAL_POINT_CHAR) {
                            state.measurementValue = state.measurementValue * 10 + (currentChar - '0');
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class State {
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
            return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
        }

        private double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        private MeasurementAggregator merge(final MeasurementAggregator b) {
            this.count += b.count;
            this.min = Math.min(this.min, b.min);
            this.max = Math.max(this.max, b.max);
            this.sum += b.sum;
            return this;
        }
    }

    /**
     * Very basic hash table implementation, only implementing computeIfAbsent since that's all the code needs.
     * It's sized to give minimal collisions with the example test set. this may not hold true if the stations list
     * changes, but it should still perform fairly well.
     * It uses Open Addressing, meaning it's just one array, rather Separate Chaining which is what the default java HashMap uses.
     * IT also uses Linear probing for collision resolution, which given the minimal collision count should hold up well.
     */
    private static class ByteArrayKeyedMap {
        private final int BUCKET_COUNT = 0xFFFF;
        private final MeasurementAggregator[] buckets = new MeasurementAggregator[BUCKET_COUNT + 1];
        private final List<MeasurementAggregator> compactUnorderedBuckets = new ArrayList<>(413);

        public MeasurementAggregator computeIfAbsent(final byte[] key, final int keyLength, final int keyHashCode) {
            var index = keyHashCode & BUCKET_COUNT;

            while (true) {
                MeasurementAggregator maybe = buckets[index];
                if (maybe != null) {
                    if (Arrays.equals(key, 0, keyLength, maybe.stationNameBytes, 0, maybe.stationNameBytes.length)) {
                        return maybe;
                    }
                    index++;
                    index &= BUCKET_COUNT;
                }
                else {
                    final var copiedKey = Arrays.copyOf(key, keyLength);
                    MeasurementAggregator measurementAggregator = new MeasurementAggregator(copiedKey, keyHashCode);
                    buckets[index] = measurementAggregator;
                    compactUnorderedBuckets.add(measurementAggregator);
                    return measurementAggregator;
                }
            }
        }

        public List<MeasurementAggregator> getAsUnorderedList() {
            return compactUnorderedBuckets;
        }
    }
}
