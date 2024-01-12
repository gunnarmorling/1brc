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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
    private static final int BUFFER_CAPACITY = 1024 * 1024;
    private static final int MAP_CAPACITY = 10000;
    private static final int QUEUE_CAPACITY = 2;

    public static void main(String[] args) throws Exception {
        // Files.list(Paths.get("./src/test/resources/samples"))
        // .filter(file -> file.toString().endsWith(".txt"))
        // .forEach(file -> {
        // try {
        // String actual = calc(file);
        // String expected = Files.readAllLines(Paths.get(file.toString().replace(".txt", ".out"))).get(0);
        // System.out.println(file.getFileName() + ": " + expected.equals(actual));
        // } catch (Exception e) {
        // System.out.println(file.getFileName() + ": " + false);
        // e.printStackTrace();
        // }
        // });
        // long start = System.currentTimeMillis();
        System.out.println(calc(Paths.get("./measurements.txt")));
        // System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
    }

    /**
     * Calculate the average.
     */
    public static String calc(Path file) throws IOException, ExecutionException, InterruptedException {
        long[] positions = fragment(file, Runtime.getRuntime().availableProcessors());
        FutureTask<Map<MeasurementName, MeasurementData>>[] tasks = new FutureTask[positions.length];
        for (int i = 0; i < positions.length; i++) {
            tasks[i] = new FutureTask<>(new Task(file, (i == 0 ? 0 : positions[i - 1] + 1), positions[i]));
            new Thread(tasks[i]).start();
        }
        Map<String, MeasurementData> result = HashMap.newHashMap(MAP_CAPACITY);
        for (FutureTask<Map<MeasurementName, MeasurementData>> task : tasks) {
            task.get().forEach((k, v) -> result.merge(k.toString(), v, MeasurementData::merge));
        }
        return new TreeMap<>(result).toString();
    }

    /**
     * Fragment the file into chunks.
     */
    private static long[] fragment(Path filePath, int chunkNum) throws IOException {
        long fileSize = Files.size(filePath);
        long chunkSize = fileSize / chunkNum;
        long[] positions = new long[chunkNum];
        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
            long position = chunkSize;
            for (int i = 0; i < chunkNum - 1; i++) {
                if (position >= fileSize) {
                    break;
                }
                file.seek(position);
                while (file.read() != '\n') {
                    position++;
                }
                positions[i] = position;
                position += chunkSize;
            }
        }
        positions[chunkNum - 1] = fileSize;
        return Arrays.stream(positions).filter(value -> value != 0).toArray();
    }

    /**
     * The measurement name.
     */
    private record MeasurementName(byte[] bytes) {

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof MeasurementName)) {
                return false;
            }
            return Arrays.equals(bytes, ((MeasurementName) other).bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return new String(bytes, StandardCharsets.UTF_8);
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
     * The task to read and calculate.
     */
    private static class Task implements Callable<Map<MeasurementName, MeasurementData>> {
        private final Path file;
        private long readPosition;
        private long calcPosition;
        private final long limitSize;
        private final BlockingQueue<byte[]> bytesQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        public Task(Path file, long position, long limitSize) {
            this.file = file;
            this.readPosition = position;
            this.calcPosition = position;
            this.limitSize = limitSize;
        }

        @Override
        public Map<MeasurementName, MeasurementData> call() throws IOException {
            // read and offer to queue
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                    file, Set.of(StandardOpenOption.READ), Executors.newVirtualThreadPerTaskExecutor());
            ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_CAPACITY);
            channel.read(buffer, readPosition, buffer, new CompletionHandler<>() {
                @Override
                public void completed(Integer bytesRead, ByteBuffer buffer) {
                    if (bytesRead > 0 && readPosition < limitSize) {
                        try {
                            buffer.flip();
                            byte[] bytes = new byte[buffer.remaining()];
                            buffer.get(bytes);
                            readPosition += bytesRead;
                            if (readPosition > limitSize) {
                                int diff = (int) (readPosition - limitSize);
                                byte[] newBytes = new byte[bytes.length - diff];
                                System.arraycopy(bytes, 0, newBytes, 0, newBytes.length);
                                bytesQueue.put(newBytes);
                            }
                            else {
                                bytesQueue.put(bytes);
                                buffer.clear();
                                channel.read(buffer, readPosition, buffer, this);
                            }
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer buffer) {
                    // ignore
                }
            });

            // poll from queue and calculate
            Map<MeasurementName, MeasurementData> result = HashMap.newHashMap(MAP_CAPACITY);
            byte[] readBytes = null;
            byte[] remaining = null;
            while (calcPosition < limitSize) {
                readBytes = bytesQueue.poll();
                if (readBytes != null) {
                    List<byte[]> lines = split(readBytes, (byte) '\n');
                    for (int i = 0; i < lines.size(); i++) {
                        byte[] lineBytes = lines.get(i);
                        if (i == 0 && remaining != null) {
                            byte[] newBytes = new byte[remaining.length + lineBytes.length];
                            System.arraycopy(remaining, 0, newBytes, 0, remaining.length);
                            System.arraycopy(lineBytes, 0, newBytes, remaining.length, lineBytes.length);
                            lineBytes = newBytes;
                        }
                        if (i == lines.size() - 1) {
                            remaining = lineBytes;
                            break;
                        }
                        agg(result, lineBytes);
                    }
                    calcPosition += readBytes.length;
                }
            }
            if (remaining != null && remaining.length > 0) {
                agg(result, remaining);
            }
            channel.close();
            return result;
        }

        /**
         * Aggregate the measurement data.
         */
        private static void agg(Map<MeasurementName, MeasurementData> result, byte[] bytes) {
            List<byte[]> parts = split(bytes, (byte) ';');
            MeasurementName station = new MeasurementName(parts.getFirst());
            int value = toInt(parts.getLast());
            MeasurementData data = result.get(station);
            if (data != null) {
                data.merge(value, value, value, 1);
            }
            else {
                result.put(station, new MeasurementData(value));
            }
        }

        /**
         * Convert the byte array to int.
         */
        private static int toInt(byte[] bytes) {
            boolean negative = false;
            int result = 0;
            for (byte b : bytes) {
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

        /**
         * Split the byte array by given byte.
         */
        private static List<byte[]> split(byte[] bytes, byte separator) {
            List<byte[]> result = new ArrayList<>();
            int start = 0;
            for (int end = 0; end < bytes.length; end++) {
                if (bytes[end] == separator) {
                    byte[] newBytes = new byte[end - start];
                    System.arraycopy(bytes, start, newBytes, 0, newBytes.length);
                    result.add(newBytes);
                    start = end + 1;
                }
            }
            if (start <= bytes.length) {
                byte[] newBytes = new byte[bytes.length - start];
                System.arraycopy(bytes, start, newBytes, 0, newBytes.length);
                result.add(newBytes);
            }
            return result;
        }
    }
}
