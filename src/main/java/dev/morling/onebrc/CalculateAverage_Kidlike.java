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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Versions (correlate to git commits):
 * <ol>
 *     <li>2m34s: parallel file read -> load byte chunks in memory -> sequentially process bytes for result</li>
 *     <li>0m59s: process byte chunks in parallel (had to introduce smarter byte chunking so it splits only on newlines)</li>
 *     <li>0m46s: smaller numeric types for MeasurementAggregator</li>
 *     <li>0m39s: implement custom byte[] to int parsing, instead of Double.parseDouble(new String(bytes))</li>
 *     <li>0m18s: run with GraalVM native-image</li>
 *     <li>0m14s: remove ConcurrentHashMap</li>
 *     <li>0m11s: remove Map#compute</li>
 * </ol>
 *
 * <p>
 * Hardware:
 * <ul>
 *     <li>CPU: https://www.cpubenchmark.net/cpu.php?cpu=AMD+Ryzen+7+5800H&id=3907</li>
 *     <li>NVMe: https://www.harddrivebenchmark.net/hdd.php?hdd=SKHynix%20HFS001TDE9X084N&id=28560</li>
 * </ul>
 * </p>
 */
public class CalculateAverage_Kidlike {

    public static void main(String[] args) {
        File file = new File("./measurements.txt");
        long fileSize = file.length();
        int processors = (fileSize < 1_000_000) ? 1 : Runtime.getRuntime().availableProcessors();
        long chunkSize = fileSize / processors;

        MappedByteBuffer[] byteBuffers = new MappedByteBuffer[processors];

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < processors; i++) {
                long start = i * chunkSize;
                long length = (i == processors - 1) ? fileSize - chunkSize * (processors - 1) : chunkSize;
                int processor = i;
                executor.execute(() -> {
                    long realStart = start;
                    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                        if (start != 0) {
                            raf.seek(start);
                            while (raf.readByte() != '\n') {
                                // move pointer to newline, so that we can process the byte chunks in parallel later.
                            }
                            realStart = raf.getFilePointer();
                        }
                        long realLength = fileSize - realStart;
                        if (realStart + length < fileSize) {
                            raf.seek(realStart + length);
                            while (raf.readByte() != '\n') {
                                // move pointer to newline, so that we can process the byte chunks in parallel later.
                            }
                            realLength = raf.getFilePointer() - realStart;
                        }

                        MappedByteBuffer byteBuffer = raf.getChannel().map(MapMode.READ_ONLY, realStart, realLength);
                        byteBuffer.load();
                        byteBuffers[processor] = byteBuffer;
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        System.out.println(new TreeMap(calculateMeasurements(byteBuffers)));
    }

    private static Map<String, MeasurementAggregator> calculateMeasurements(MappedByteBuffer[] buffers) {
        return Arrays.stream(buffers).parallel()
                .map(buffer -> {
                    var results = new HashMap<String, MeasurementAggregator>();

                    var state = State.NEXT_READ_CITY;
                    var citySink = new CheapByteBuffer(100);
                    var measurementSink = new CheapByteBuffer(10);

                    while (buffer.hasRemaining()) {
                        byte b = buffer.get();
                        char c = (char) b;

                        if (c == ';') {
                            state = State.NEXT_READ_MEASUREMENT;
                            continue;
                        }

                        if (c == '\n') {
                            String city = new String(citySink.getBytes(), UTF_8);
                            int measurement = bytesToInt(measurementSink.getBytes());
                            var entry = results.get(city);
                            if (entry == null) {
                                entry = new MeasurementAggregator();
                                results.put(city, entry);
                            }

                            entry.count++;
                            entry.sum += measurement;
                            entry.min = (short) Math.min(entry.min, measurement);
                            entry.max = (short) Math.max(entry.max, measurement);

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

                    return results;
                })
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, MeasurementAggregator::merge));
    }

    /**
     * Removes decimal points and returns an integer. For example -12.3 would return -123
     */
    static int bytesToInt(byte[] bytes) {
        short index = (short) (bytes.length - 1);

        boolean isNegative = (bytes[0] == '-');
        int number = (bytes[index] - '0');
        index -= 2;

        number += (10 * (bytes[index--] - '0'));

        if (index == 1 || (!isNegative && index == 0)) {
            number += (100 * (bytes[index] - '0'));
        }

        if (isNegative) {
            return -number;
        }
        else {
            return number;
        }
    }

    private enum State {
        NEXT_READ_CITY,
        NEXT_READ_MEASUREMENT
    }

    /**
     * Numbers are stored as integers, because of {@link #bytesToInt}, and then divided by 10.0 to restore their decimal point.
     */
    private static class MeasurementAggregator {

        private static final DecimalFormat rounder = new DecimalFormat("0.0");
        short min = Short.MAX_VALUE;
        short max = Short.MIN_VALUE;
        long sum;
        int count;

        private MeasurementAggregator merge(MeasurementAggregator other) {
            min = (short) Math.min(min, other.min);
            max = (short) Math.max(max, other.max);
            sum += other.sum;
            count += other.count;

            return this;
        }

        @Override
        public String toString() {
            return rounder.format(min / 10.0)
                    + "/" + rounder.format(Math.round((double) sum / count) / 10.0)
                    + "/" + rounder.format(max / 10.0);
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
