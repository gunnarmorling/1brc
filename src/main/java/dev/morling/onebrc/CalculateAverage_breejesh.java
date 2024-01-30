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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateAverage_breejesh {
    private static final String FILE = "./measurements.txt";
    private static final int TWO_BYTE_TO_INT = 480 + 48; // 48 is the ASCII code for '0'
    private static final int THREE_BYTE_TO_INT = 4800 + 480 + 48;

    private static final class Measurement {

        private int min;
        private int max;
        private int total;
        private int count;

        public Measurement(int value) {
            this.min = value;
            this.max = value;
            this.total = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return STR."\{min / 10.0}/\{Math.round(((double) total) / count) / 10.0}/\{max / 10.0}";
        }

        private void append(int min, int max, int total, int count) {
            if (min < this.min)
                this.min = min;
            if (max > this.max)
                this.max = max;
            this.total += total;
            this.count += count;
        }

        public void append(int value) {
            append(value, value, value, 1);
        }

        public void merge(Measurement other) {
            append(other.min, other.max, other.total, other.count);
        }
    }

    public static void main(String[] args) throws Exception {
        // long start = System.currentTimeMillis();
        // Find system details to determine cores and
        final RandomAccessFile file = new RandomAccessFile(FILE, "r");
        int cores = Runtime.getRuntime().availableProcessors();
        long fileSize = file.length();
        long splitSectionSize = fileSize / cores;

        // Divide file into segments equal to number of cores
        MappedByteBuffer[] buffers = new MappedByteBuffer[cores];
        for (int i = 0; i < cores; i++) {
            long sectionStart = i * (long) splitSectionSize;
            long sectionEnd = Math.min(fileSize, sectionStart + splitSectionSize + 100);
            buffers[i] = file.getChannel().map(FileChannel.MapMode.READ_ONLY, sectionStart, sectionEnd - sectionStart);
        }

        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(cores);
        List<CompletableFuture<Map<String, Measurement>>> futures = new ArrayList<>();
        for (int j = 0; j < buffers.length; ++j) {
            MappedByteBuffer currentBuffer = buffers[j];
            int finalJ = j;
            CompletableFuture<Map<String, Measurement>> future = CompletableFuture.supplyAsync(() -> {
                Map<String, Measurement> map = new HashMap<>();
                while (currentBuffer.position() < splitSectionSize) {
                    byte currentByte;
                    var byteCounter = 0;
                    var buffer = new byte[100];
                    // skip half segment
                    if (finalJ > 0) {
                        while (currentBuffer.get() != '\n') ;
                    }
                    if(!currentBuffer.hasRemaining()) break;

                    // read station
                    while ((currentByte = currentBuffer.get()) != ';') {
                        buffer[byteCounter++] = currentByte;
                    }

                    // read number
                    int value;
                    byte b1 = currentBuffer.get();
                    byte b2 = currentBuffer.get();
                    byte b3 = currentBuffer.get();
                    byte b4 = currentBuffer.get();
                    if (b2 == '.') {
                        // case of n.n
                        value = (b1 * 10 + b3 - TWO_BYTE_TO_INT);
                    } else {
                        if (b4 == '.') {
                            // case of -nn.n
                            value = -(b2 * 100 + b3 * 10 + currentBuffer.get() - THREE_BYTE_TO_INT);
                        } else if (b1 == '-') {
                            // case of -n.n
                            value = -(b2 * 10 + b4 - TWO_BYTE_TO_INT);
                        } else {
                            // case of nn.n
                            value = (b1 * 100 + b2 * 10 + b4 - THREE_BYTE_TO_INT);
                        }
                        currentBuffer.get(); // new line
                    }
                    String str = new String(buffer, 0, byteCounter, StandardCharsets.UTF_8);
                    if (map.containsKey(str)) {
                        map.get(str).append(value);
                    } else {
                        map.put(str, new Measurement(value));
                    }
                }
                return map;
            }, executor);

            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        Map<String, Measurement> finalMap = new TreeMap<>();
        for (CompletableFuture<Map<String, Measurement>> future : futures) {
            Map<String, Measurement> map = future.get();
            finalMap.putAll(map);
        }

        System.out.println(finalMap);
        // System.out.printf("Time %s", System.currentTimeMillis() - start);
        System.exit(0);
    }
}
