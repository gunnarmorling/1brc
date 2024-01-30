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
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class CalculateAverage_sudhirtumati {

    private static final String FILE = "./measurements.txt";
    private static final int bufferSize = 8192;
    private static final byte SEMICOLON = (byte) ';';
    private static final byte NEW_LINE = (byte) '\n';
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final Semaphore PERMITS = new Semaphore(THREAD_COUNT);
    private static final MeasurementAggregator globalAggregator = new MeasurementAggregator();
    private static final Semaphore AGGREGATOR_PERMITS = new Semaphore(1);
    private static final Map<Integer, String> LOCATION_STORE = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        CalculateAverage_sudhirtumati instance = new CalculateAverage_sudhirtumati();
        instance.chunkProcess();
    }

    private void chunkProcess() throws IOException, InterruptedException {
        try (FileInputStream is = new FileInputStream(FILE);
             FileChannel fc = is.getChannel()) {
            for (int i = 0; i < THREAD_COUNT; i++) {
                PERMITS.acquire();
                Thread t = new ChunkProcessingThread(i, fc);
                t.setName(STR."T\{i}");
                t.start();
            }
            do {
                Thread.sleep(100);
            } while (PERMITS.availablePermits() != THREAD_COUNT);
        }
        System.out.println(globalAggregator.getResult());
    }

    static class ChunkProcessingThread extends Thread {

        private int index;
        private final FileChannel fc;
        private final MeasurementAggregator aggregator;

        ChunkProcessingThread(int index, FileChannel fc) {
            this.index = index;
            this.fc = fc;
            aggregator = new MeasurementAggregator();
        }

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.allocate(index == 0 ? bufferSize : bufferSize + 50);
            long fcPosition = index == 0 ? 0 : (((long) index * bufferSize) - 50);
            try {
                while (fc.read(buffer, fcPosition) != -1) {
                    buffer.flip();
                    if (index != 0 /* && fc.position() != bufferSize */) {
                        seekStartPos(buffer);
                    }
                    processBuffer(buffer);
                    index += THREAD_COUNT;
                    fcPosition = ((long) index * bufferSize) - 50L;
                    if (buffer.capacity() == 8192) {
                        buffer = ByteBuffer.allocate(bufferSize + 50);
                    }
                    buffer.position(0);
                }
                AGGREGATOR_PERMITS.acquire();
                globalAggregator.process(aggregator);
                AGGREGATOR_PERMITS.release();
            }
            catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            PERMITS.release();
        }

        private void processBuffer(ByteBuffer buffer) throws IOException {
            int mStartMark = buffer.position();
            int tStartMark = -1;
            int count = buffer.position();
            do {
                byte b = buffer.get(count);
                if (b == SEMICOLON) {
                    tStartMark = count;
                }
                else if (b == NEW_LINE) {
                    byte[] locArr = new byte[tStartMark - mStartMark];
                    byte[] tempArr = new byte[count - tStartMark];
                    buffer.get(mStartMark, locArr);
                    buffer.get(mStartMark + locArr.length + 1, tempArr);
                    aggregator.process(locArr, tempArr);
                    mStartMark = count + 1;
                }
                count++;
            } while (count < buffer.limit());
        }

        private void seekStartPos(ByteBuffer buffer) {
            int i = buffer.limit() > 50 ? 49 : buffer.limit() - 2;
            for (; i >= 0; i--) {
                if (buffer.get(i) == NEW_LINE) {
                    buffer.position(i + 1);
                    break;
                }
            }
        }
    }

    static final class MeasurementAggregator {
        private static final long MAX_VALUE_DIVIDE_10 = Long.MAX_VALUE / 10;
        private final Map<Integer, Measurement> store = new HashMap<>();

        public void process(MeasurementAggregator other) {
            other.store.forEach((k, v) -> {
                Measurement m = store.get(k);
                if (m == null) {
                    m = new Measurement();
                    store.put(k, m);
                }
                m.process(v);
            });
        }

        public void process(byte[] location, byte[] temperature) throws IOException {
            Integer hashCode = Arrays.hashCode(location);
            LOCATION_STORE.computeIfAbsent(hashCode, _ -> new String(location));
            // String loc = new String(location);
            Measurement measurement = store.get(hashCode);
            if (measurement == null) {
                measurement = new Measurement();
                store.put(hashCode, measurement);
            }
            double tempD = parseDouble(temperature);
            measurement.process(tempD);
        }

        public double parseDouble(byte[] bytes) {
            long value = 0;
            int exp = 0;
            boolean negative = false;
            int decimalPlaces = Integer.MIN_VALUE;
            int index = 0;
            int ch = bytes[index];
            if (ch == '-') {
                negative = true;
                ch = bytes[++index];
            }
            while (index < bytes.length) {
                if (ch >= '0' && ch <= '9') {
                    while (value >= MAX_VALUE_DIVIDE_10) {
                        value >>>= 1;
                        exp++;
                    }
                    value = value * 10 + (ch - '0');
                    decimalPlaces++;

                }
                else if (ch == '.') {
                    decimalPlaces = 0;
                }
                if (index == bytes.length - 1) {
                    break;
                }
                else {
                    ch = bytes[++index];
                }
            }
            return asDouble(value, exp, negative, decimalPlaces);
        }

        private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
            if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
                if (value < Long.MAX_VALUE / (1L << 32)) {
                    exp -= 32;
                    value <<= 32;
                }
                if (value < Long.MAX_VALUE / (1L << 16)) {
                    exp -= 16;
                    value <<= 16;
                }
                if (value < Long.MAX_VALUE / (1L << 8)) {
                    exp -= 8;
                    value <<= 8;
                }
                if (value < Long.MAX_VALUE / (1L << 4)) {
                    exp -= 4;
                    value <<= 4;
                }
                if (value < Long.MAX_VALUE / (1L << 2)) {
                    exp -= 2;
                    value <<= 2;
                }
                if (value < Long.MAX_VALUE / (1L << 1)) {
                    exp -= 1;
                    value <<= 1;
                }
            }
            for (; decimalPlaces > 0; decimalPlaces--) {
                exp--;
                long mod = value % 5;
                value /= 5;
                int modDiv = 1;
                if (value < Long.MAX_VALUE / (1L << 4)) {
                    exp -= 4;
                    value <<= 4;
                    modDiv <<= 4;
                }
                if (value < Long.MAX_VALUE / (1L << 2)) {
                    exp -= 2;
                    value <<= 2;
                    modDiv <<= 2;
                }
                if (value < Long.MAX_VALUE / (1L << 1)) {
                    exp -= 1;
                    value <<= 1;
                    modDiv <<= 1;
                }
                if (decimalPlaces > 1)
                    value += modDiv * mod / 5;
                else
                    value += (modDiv * mod + 4) / 5;
            }
            final double d = Math.scalb((double) value, exp);
            return negative ? -d : d;
        }

        public String getResult() {
            Map<String, Measurement> sortedMap = new TreeMap<>();
            store.forEach((k, v) -> sortedMap.put(LOCATION_STORE.get(k), v));
            return sortedMap.toString();
        }
    }

    static final class Measurement {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public void process(double value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
        }

        public void process(Measurement other) {
            if (other.min < min) {
                this.min = other.min;
            }
            if (other.max > max) {
                this.max = other.max;
            }
            this.sum += other.sum;
            this.count += other.count;
        }

        public String toString() {
            ResultRow result = new ResultRow(min, sum, count, max);
            return result.toString();
        }
    }

    private record ResultRow(double min, double sum, double count, double max) {

        public String toString() {
            return STR."\{round(min)}/\{round((Math.round(sum * 10.0) / 10.0) / count)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

}
