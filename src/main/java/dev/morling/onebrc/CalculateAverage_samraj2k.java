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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_samraj2k {
    private static final int THREAD_COUNT = 1000;

    private static final Path FILE_PATH = Paths.get("./measurements.txt");

    private static class Results {
        private int min;
        private int max;
        private long count;
        private long sum;

        public Results(int temperature) {
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        public Results(Results results) {
            this.min = results.min;
            this.max = results.max;
            this.sum = results.sum;
            this.count = results.count;
        }

        public void update(int temperature) {
            if (this.min > temperature) {
                this.min = temperature;
            }
            if (this.max < temperature) {
                this.max = temperature;
            }
            this.sum += temperature;
            this.count += 1;
        }

        public void combine(Results results) {
            if (this.min > results.min) {
                this.min = results.min;
            }
            if (this.max < results.max) {
                this.max = results.max;
            }
            this.sum += results.sum;
            this.count += results.count;
        }
    }

    private static final HashMap<String, Results>[] temperatureStores = new HashMap[THREAD_COUNT];

    private static final TreeMap<String, Results> temperatureStore = new TreeMap<>();

    private static long findEndPosition(FileChannel channel, long position) throws IOException {
        ByteBuffer bufferedReader = ByteBuffer.allocate(1);
        while (position < channel.size()) {
            channel.read(bufferedReader, position);
            if (bufferedReader.get(0) != '\n') {
                position++;
                bufferedReader.clear();
            }
            else {
                return position + 1;
            }
        }
        return channel.size();
    }

    public static void main(String[] args) throws FileNotFoundException {
        // long start = System.currentTimeMillis();
        try {
            FileChannel channel = FileChannel.open(FILE_PATH, StandardOpenOption.READ);
            long fileSize = channel.size();
            long chunkSize = (fileSize + THREAD_COUNT - 1) / THREAD_COUNT; // Add 1 to account for any remainder bytes

            long offset = 0;
            for (int i = 0; i < THREAD_COUNT; i++) {
                temperatureStores[i] = new HashMap<>();
            }
            // Create and start the threads
            Thread[] threads = new Thread[THREAD_COUNT];
            for (int i = 0; i < THREAD_COUNT; i++) {
                long finalOffset = offset;
                long finalEnd = findEndPosition(channel, finalOffset + chunkSize);

                int finalI = i;
                threads[i] = Thread.startVirtualThread(() -> {
                    ReaderThread readerThread = new ReaderThread(channel, finalOffset, finalEnd - finalOffset, finalI);
                    readerThread.run();
                });
                offset = finalEnd;
            }

            // Wait for all threads to complete
            for (int i = 0; i < THREAD_COUNT; i++) {
                threads[i].join();
            }
            channel.close();
            for (int i = 0; i < THREAD_COUNT; i++) {
                for (Map.Entry<String, Results> entry : temperatureStores[i].entrySet()) {
                    String key = entry.getKey();
                    Results value = entry.getValue();
                    if (temperatureStore.containsKey(key)) {
                        temperatureStore.get(key).combine(value);
                    }
                    else {
                        temperatureStore.put(key, new Results(value));
                    }
                }
            }
            StringBuilder sb = new StringBuilder("{");
            temperatureStore.forEach((key, value) -> sb.append(key).append("=").append((double) value.min / 10.0)
                    .append("/").append(Math.round(((double) value.sum / value.count)) / 10.0)
                    .append("/").append((double) value.max / 10.0)
                    .append(", "));
            sb.delete(sb.length() - 2, sb.length());
            sb.append("}");
            String result = sb.toString();
            System.out.println(result);

        }
        catch (Exception ex) {
            // ignore
        }
        // System.out.println(System.currentTimeMillis() - start);

    }

    private static class ReaderThread {
        private final FileChannel fileChannel;
        private final long fileOffset;
        private final long chunkSize;
        private final int threadCount;

        public ReaderThread(FileChannel file, long fileOffset, long chunkSize, int threadCount) {
            this.fileChannel = file;
            this.fileOffset = fileOffset;
            this.chunkSize = chunkSize;
            this.threadCount = threadCount;
        }

        public void run() {
            try {
                // Map the file segment into memory using FileChannel and MappedByteBuffer
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, fileOffset, chunkSize);

                // Read the data from the buffer and process it
                int sz = mappedByteBuffer.limit();
                byte[] buffer = new byte[sz];
                mappedByteBuffer.get(buffer);
                int curPos = 0;
                while (curPos < sz) {
                    int idx = curPos;
                    // each line gets processed
                    while (buffer[idx] != ';') {
                        idx++;
                    }
                    byte[] subArray = Arrays.copyOfRange(buffer, curPos, idx);
                    String utf8String = new String(subArray, StandardCharsets.UTF_8);
                    // till here we have string
                    // skip ;
                    idx++;
                    boolean negative = false;
                    if (buffer[idx] == '-') {
                        // negative
                        negative = true;
                        idx++;
                    }
                    int tempValue;
                    if (buffer[idx + 1] == '.') {
                        tempValue = buffer[idx] - '0';
                        idx++;
                    }
                    else {
                        tempValue = (buffer[idx + 1] - '0') + 10 * (buffer[idx] - '0');
                        idx += 2;
                    }
                    // skip .
                    idx++;
                    tempValue = 10 * tempValue + (buffer[idx] - '0');
                    if (negative) {
                        tempValue = -1 * tempValue;
                    }
                    if (temperatureStores[threadCount].containsKey(utf8String)) {
                        temperatureStores[threadCount].get(utf8String).update(tempValue);
                    }
                    else {
                        temperatureStores[threadCount].put(utf8String, new Results(tempValue));
                    }
                    idx++;
                    // reached \n
                    curPos = idx;
                    curPos++;
                }
                mappedByteBuffer.clear();

            }
            catch (IOException e) {
                // ignore
            }
        }
    }
}
