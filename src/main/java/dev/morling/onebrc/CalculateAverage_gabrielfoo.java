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

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class CalculateAverage_gabrielfoo {
    private static final String FILE = "./measurements.txt";
    private static final int UTF8_MAX_LEN_100_BYTES = 400;
    private static final int DOUBLE_DIGITS_MAX = 3;
    private static final int UNIQUE_STATION_NAMES = 10000;

    private static class ResultRow {
        private double min = Double.POSITIVE_INFINITY;
        private double sum = 0.0;
        private double max = Double.NEGATIVE_INFINITY;
        private int count = 0;

        public String toString() {
            return min + "/" + (Math.round(sum / count) / 10.0) + "/" + max;
        }

        public void updateMinMax(double incoming) {
            min = Math.min(min, incoming);
            max = Math.max(max, incoming);
            sum += incoming * 10.0;
            count += 1;
        }

        public void combine(ResultRow other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }
    }

    public static MappedByteBuffer[] mapFileToMemory(final RandomAccessFile file, final int chunkCount) throws Exception {
        FileChannel channel = file.getChannel();
        final long chunkSize = Math.ceilDiv(file.length(), chunkCount);

        MappedByteBuffer buffers[] = new MappedByteBuffer[chunkCount];

        long position = 0;
        for (int i = 0; i < chunkCount - 1; ++i) {
            file.seek(position + chunkSize);
            long ptr = file.getFilePointer();

            while (file.readByte() != '\n') {
                file.seek(++ptr);
            }

            buffers[i] = channel.map(FileChannel.MapMode.READ_ONLY, position, ptr - position + 1);

            position = ptr + 1;
        }

        buffers[buffers.length - 1] = channel.map(FileChannel.MapMode.READ_ONLY, position, file.length() - position);

        return buffers;
    }

    public static void main(String[] args) throws Exception {
        final RandomAccessFile file = new RandomAccessFile(FILE, "r");
        final int coreCount = file.length() < 2147483647 ? 1 : Runtime.getRuntime().availableProcessors();
        ArrayList<HashMap<String, ResultRow>> maps = new ArrayList<>();

        final ThreadFactory threadFactory = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(coreCount, threadFactory);

        Future<?> initFuture = executor.submit(() -> {
            for (int i = 0; i < coreCount; ++i) {
                maps.add(new HashMap<>(UNIQUE_STATION_NAMES, 0.9f));
            }
        });

        MappedByteBuffer[] buffers = mapFileToMemory(file, coreCount);
        initFuture.get();

        Future<?>[] futures = new Future<?>[buffers.length];

        for (int k = 0; k < buffers.length; ++k) {
            final MappedByteBuffer buffer = buffers[k];
            final var map = maps.get(k);
            futures[k] = executor.submit(() -> {
                int start = 0;
                byte[] stationArr = new byte[UTF8_MAX_LEN_100_BYTES];
                double[] floatArr = new double[DOUBLE_DIGITS_MAX];
                byte currentByte;

                while (buffer.hasRemaining()) {
                    currentByte = buffer.get();
                    stationArr[buffer.position() - start - 1] = currentByte;

                    if (currentByte == ';') {
                        final int stationEnd = buffer.position() - 1;
                        // convert to double now
                        currentByte = buffer.get();
                        boolean neg = currentByte == '-';
                        if (neg)
                            currentByte = buffer.get();
                        floatArr[0] = currentByte - '0';
                        currentByte = buffer.get();
                        if (currentByte == '.') {
                            floatArr[1] = (buffer.get() - '0') / 10.0;
                            floatArr[2] = 0.0;
                        }
                        else {
                            floatArr[0] *= 10.0;
                            floatArr[1] = (currentByte - '0');
                            buffer.get();
                            floatArr[2] = (buffer.get() - '0') / 10.0;
                        }
                        final double f = (neg ? -1 : 1) * (floatArr[0] + floatArr[1] + floatArr[2]);

                        buffer.get(); // discard \n

                        String station = new String(stationArr, 0, stationEnd - start);

                        map.compute(station, (key, existingRow) -> {
                            ResultRow row = (existingRow == null) ? new ResultRow() : existingRow;
                            row.updateMinMax(f);
                            return row;
                        });

                        start = buffer.position();
                    }
                }

            });
        }

        for (Future<?> future : futures) {
            future.get();
        }

        HashMap<String, ResultRow> resultHashMap = maps.get(0);

        maps.stream().skip(1).flatMap(map -> map.entrySet().stream()).forEach(entry -> {
            resultHashMap.merge(entry.getKey(), entry.getValue(), (oldVal, newVal) -> {
                oldVal.combine(newVal);
                return oldVal;
            });
        });

        TreeMap<String, ResultRow> res = new TreeMap<>(resultHashMap);

        executor.shutdown();

        System.out.println(res);
    }
}
