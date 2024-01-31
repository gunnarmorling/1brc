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

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
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
            StringBuilder result = new StringBuilder();
            result.append(min / 10.0);
            result.append("/");
            result.append(Math.round(((double) total) / count) / 10.0);
            result.append("/");
            result.append(max / 10.0);
            return result.toString();
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
        var file = new File(args.length > 0 ? args[0] : FILE);
        long fileSize = file.length();
        var numberOfCores = fileSize > 1_000_000 ? Runtime.getRuntime().availableProcessors() : 1;
        var splitSectionSize = (int) Math.min(Integer.MAX_VALUE, fileSize / numberOfCores); // bytebuffer position is an int, so can be max Integer.MAX_VALUE
        var segmentCount = (int) (fileSize / splitSectionSize);

        // Divide file into segments
        ExecutorService executor = Executors.newFixedThreadPool(segmentCount);
        List<CompletableFuture<Map<String, Measurement>>> futures = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            long sectionStart = i * (long) splitSectionSize;
            long sectionEnd = Math.min(fileSize, sectionStart + splitSectionSize + 100);
            var fileChannel = (FileChannel) Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
            CompletableFuture<Map<String, Measurement>> future = CompletableFuture.supplyAsync(() -> {
                MappedByteBuffer currentBuffer = null;
                try {
                    currentBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, sectionStart, sectionEnd - sectionStart);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                // Skip till new line for unequal segments, not to be done for first section
                if (sectionStart > 0) {
                    while (currentBuffer.get() != '\n')
                        ;
                }
                Map<String, Measurement> map = new HashMap<>();
                while (currentBuffer.position() < splitSectionSize) {
                    // Read station
                    String str = getStationFromBuffer(currentBuffer);
                    // Read number
                    int value = getValueFromBuffer(currentBuffer);
                    if (map.containsKey(str)) {
                        map.get(str).append(value);
                    }
                    else {
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
            map.keySet().stream().forEach(
                    key -> {
                        if (finalMap.containsKey(key)) {
                            finalMap.get(key).merge(map.get(key));
                        }
                        else {
                            finalMap.put(key, map.get(key));
                        }
                    });
        }

        System.out.println(finalMap);
        // System.out.printf("Time %s", System.currentTimeMillis() - start);
        System.exit(0);
    }

    private static String getStationFromBuffer(MappedByteBuffer currentBuffer) {
        byte currentByte;
        var byteCounter = 0;
        var buffer = new byte[100];
        while ((currentByte = currentBuffer.get()) != ';') {
            buffer[byteCounter++] = currentByte;
        }
        return new String(buffer, 0, byteCounter, StandardCharsets.UTF_8);
    }

    private static int getValueFromBuffer(MappedByteBuffer currentBuffer) {
        int value;
        byte[] nums = new byte[4];
        currentBuffer.get(nums);
        if (nums[1] == '.') {
            // case of n.n
            value = (nums[0] * 10 + nums[2] - TWO_BYTE_TO_INT);
        }
        else {
            if (nums[3] == '.') {
                // case of -nn.n
                value = -(nums[1] * 100 + nums[2] * 10 + currentBuffer.get() - THREE_BYTE_TO_INT);
            }
            else if (nums[0] == '-') {
                // case of -n.n
                value = -(nums[1] * 10 + nums[3] - TWO_BYTE_TO_INT);
            }
            else {
                // case of nn.n
                value = (nums[0] * 100 + nums[1] * 10 + nums[3] - THREE_BYTE_TO_INT);
            }
            currentBuffer.get(); // new line
        }
        return value;
    }
}
