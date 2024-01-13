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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Calculates the average using AIO and multiple threads.
 *
 * @author Xylitol
 */
public class CalculateAverage_C5H12O5 {
    private static final int BUFFER_CAPACITY = 1024 * 1024 * 10;
    private static final int MAP_CAPACITY = 10000;
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final BlockingQueue<byte[]> BYTES_QUEUE = new LinkedBlockingQueue<>(PROCESSORS);
    private static long readPosition;

    public static void main(String[] args) throws Exception {
        System.out.println(calc("./measurements.txt"));
    }

    /**
     * Calculate the average.
     */
    public static String calc(String path) throws IOException, ExecutionException, InterruptedException {
        readPosition = 0;
        Map<String, MeasurementData> result = HashMap.newHashMap(MAP_CAPACITY);
        // read and offer to queue
        try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                Paths.get(path), Set.of(StandardOpenOption.READ), Executors.newVirtualThreadPerTaskExecutor())) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_CAPACITY);
            channel.read(buffer, readPosition, buffer, new CompletionHandler<>() {
                @Override
                public void completed(Integer bytesRead, ByteBuffer buffer) {
                    try {
                        if (bytesRead > 0) {
                            for (int i = buffer.position() - 1; i >= 0; i--) {
                                if (buffer.get(i) == '\n') {
                                    buffer.limit(i + 1);
                                    break;
                                }
                            }
                            buffer.flip();
                            byte[] bytes = new byte[buffer.remaining()];
                            buffer.get(bytes);
                            readPosition += buffer.limit();
                            BYTES_QUEUE.put(bytes);
                            buffer.clear();
                            channel.read(buffer, readPosition, buffer, this);
                        }
                        else {
                            for (int i = 0; i < PROCESSORS; i++) {
                                BYTES_QUEUE.put(new byte[0]);
                            }
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer buffer) {
                    // ignore
                }
            });

            @SuppressWarnings("unchecked")
            FutureTask<Map<MeasurementName, MeasurementData>>[] tasks = new FutureTask[PROCESSORS];
            for (int i = 0; i < PROCESSORS; i++) {
                tasks[i] = new FutureTask<>(new Task());
                new Thread(tasks[i]).start();
            }
            for (FutureTask<Map<MeasurementName, MeasurementData>> task : tasks) {
                task.get().forEach((k, v) -> result.merge(k.toString(), v, MeasurementData::merge));
            }
        }
        return new TreeMap<>(result).toString();
    }

    /**
     * The measurement name.
     */
    private record MeasurementName(byte[] bytes, int length) {

        @Override
        public boolean equals(Object name) {
            MeasurementName other = (MeasurementName) name;
            if (other.length != length) {
                return false;
            }
            return Arrays.compare(bytes, 0, length, other.bytes, 0, length) == 0;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (int i = 0; i < length; i++) {
                result = 31 * result + bytes[i];
            }
            return result;
        }

        @Override
        public String toString() {
            return new String(bytes, 0, length, StandardCharsets.UTF_8);
        }
    }

    /**
     * The measurement data.
     */
    private static class MeasurementData {
        private int min;
        private int max;
        private int sum;
        private int count;

        public MeasurementData(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        public MeasurementData merge(MeasurementData data) {
            return merge(data.min, data.max, data.sum, data.count);
        }

        public MeasurementData merge(int min, int max, int sum, int count) {
            this.min = Math.min(this.min, min);
            this.max = Math.max(this.max, max);
            this.sum += sum;
            this.count += count;
            return this;
        }

        @Override
        public String toString() {
            return (min / 10.0) + "/" + (Math.round((double) sum / count) / 10.0) + "/" + (max / 10.0);
        }
    }

    /**
     * The task to calculate.
     */
    private static class Task implements Callable<Map<MeasurementName, MeasurementData>> {

        @Override
        public Map<MeasurementName, MeasurementData> call() throws InterruptedException {
            // poll from queue and calculate
            Map<MeasurementName, MeasurementData> result = HashMap.newHashMap(MAP_CAPACITY);
            for (byte[] bytes = BYTES_QUEUE.take(); true; bytes = BYTES_QUEUE.take()) {
                if (bytes.length == 0) {
                    break;
                }
                int start = 0;
                for (int end = 0; end < bytes.length; end++) {
                    if (bytes[end] == '\n') {
                        byte[] newBytes = new byte[end - start];
                        System.arraycopy(bytes, start, newBytes, 0, newBytes.length);
                        int semicolon = newBytes.length - 4;
                        for (; semicolon >= 0; semicolon--) {
                            if (newBytes[semicolon] == ';') {
                                break;
                            }
                        }
                        MeasurementName station = new MeasurementName(newBytes, semicolon);
                        int value = toInt(newBytes, semicolon + 1);
                        MeasurementData data = result.get(station);
                        if (data != null) {
                            data.merge(value, value, value, 1);
                        }
                        else {
                            result.put(station, new MeasurementData(value));
                        }
                        start = end + 1;
                    }
                }
            }
            return result;
        }

        /**
         * Convert the byte array to int.
         */
        private static int toInt(byte[] bytes, int start) {
            boolean negative = false;
            int result = 0;
            for (int i = start; i < bytes.length; i++) {
                byte b = bytes[i];
                if (b == '-') {
                    negative = true;
                    continue;
                }
                if (b != '.') {
                    result = result * 10 + (b - '0');
                }
            }
            return negative ? -result : result;
        }
    }
}
