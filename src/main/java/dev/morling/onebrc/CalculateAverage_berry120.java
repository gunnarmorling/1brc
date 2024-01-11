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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class CalculateAverage_berry120 {

    private static final String FILE = "./measurements.txt";
    // TODO: Tweak this number?
    public static final int NUM_VIRTUAL_THREADS = 1000;
    public static final boolean DEBUG = false;

    static class TemperatureSummary implements Comparable<TemperatureSummary> {
        byte[] name;
        int min;
        int max;
        int total;
        int sampleCount;

        public TemperatureSummary(byte[] name, int min, int max, int total, int sampleCount) {
            this.name = name;
            this.min = min;
            this.max = max;
            this.total = total;
            this.sampleCount = sampleCount;
        }

        @Override
        public int compareTo(TemperatureSummary o) {
            return new String(name).compareTo(new String(o.name));
        }

        @Override
        public String toString() {
            return "TemperatureSummary{" +
                    "name=" + new String(name) +
                    ", min=" + min +
                    ", max=" + max +
                    ", total=" + total +
                    ", sampleCount=" + sampleCount +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();

        Path path = Path.of(FILE);
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
        FileChannel channel = file.getChannel();
        long size = Files.size(path);
        int splitSize = size < 10_000_000 ? 1 : (NUM_VIRTUAL_THREADS - 1);
        long inc = (int) (size / splitSize);

        List<Long> positions = new ArrayList<>();
        positions.add(0L);

        MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), Arena.ofShared());

        long pos = 0;
        for (int i = 0; i < splitSize; i++) {
            long endPos = pos + inc - 1;
            while (segment.get(ValueLayout.JAVA_BYTE, endPos) != '\n') {
                endPos--;
            }
            pos = endPos + 1;
            positions.add(pos);
        }
        positions.add(size);

        if (DEBUG)
            System.out.println("WORKED OUT SPLITS: " + (System.currentTimeMillis() - time));

        List<Thread> threads = new ArrayList<>(NUM_VIRTUAL_THREADS);

        List<Map<?, TemperatureSummary>> maps = Collections.synchronizedList(new ArrayList<>());

        for (int split = 0; split < positions.size() - 1; split++) {

            long position = positions.get(split);
            long positionEnd = positions.get(split + 1);

            threads.add(Thread.ofVirtual().start(() -> {

                // TODO: Custom faster map?
                Map<Integer, TemperatureSummary> map = new HashMap<>();
                maps.add(map);

                // Care much less about this map, only used if collisions in the first
                Map<String, TemperatureSummary> backupMap = new HashMap<>();
                maps.add(backupMap);

                boolean processingPlaceName = true;

                byte[] placeName = new byte[100];
                int placeNameIdx = 0;

                byte[] digits = new byte[100];
                int digitIdx = 0;

                for (long address = position; address < positionEnd; address++) {
                    byte b = segment.get(ValueLayout.JAVA_BYTE, address);

                    if (b == 10) {
                        int rollingHash = 5381;
                        for (int i = 0; i < placeNameIdx; i++) {
                            rollingHash = (((rollingHash << 5) + rollingHash) + placeName[i]) & 0xFFFFF;
                        }

                        var existingTemperatureSummary = map.get(rollingHash);
                        int num = parse(digits, digitIdx - 1);

                        if (existingTemperatureSummary == null) {
                            byte[] thisPlace = new byte[placeNameIdx];
                            System.arraycopy(placeName, 0, thisPlace, 0, placeNameIdx);
                            map.put(rollingHash, new TemperatureSummary(thisPlace, num, num, num, 1));
                        }
                        else if (!Arrays.equals(placeName, 0, placeNameIdx, existingTemperatureSummary.name, 0, existingTemperatureSummary.name.length)) {

                            /*
                             * This block will be slow - don't really care, should be very rare
                             */
                            if (DEBUG)
                                System.out.println("BAD: COLLISION!");
                            byte[] thisPlace = new byte[placeNameIdx];
                            System.arraycopy(placeName, 0, thisPlace, 0, placeNameIdx);
                            String backupKey = new String(thisPlace);
                            var backupExistingTemperatureSummary = backupMap.get(backupKey);

                            if (backupExistingTemperatureSummary == null) {
                                backupMap.put(backupKey, new TemperatureSummary(thisPlace, num, num, num, 1));
                            }
                            else {
                                backupExistingTemperatureSummary.max = (Math.max(num, backupExistingTemperatureSummary.max));
                                backupExistingTemperatureSummary.min = (Math.min(num, backupExistingTemperatureSummary.min));
                                backupExistingTemperatureSummary.total += num;
                                backupExistingTemperatureSummary.sampleCount++;
                            }
                            /*
                             * End slow block
                             */
                        }
                        else {

                            existingTemperatureSummary.max = (Math.max(num, existingTemperatureSummary.max));
                            existingTemperatureSummary.min = (Math.min(num, existingTemperatureSummary.min));
                            existingTemperatureSummary.total += num;
                            existingTemperatureSummary.sampleCount++;
                        }

                        processingPlaceName = true;
                        placeNameIdx = 0;
                        digitIdx = 0;
                    }
                    else if (b == ';') {
                        processingPlaceName = false;
                    }
                    else if (processingPlaceName) {
                        placeName[placeNameIdx++] = b;
                    }
                    else {
                        digits[digitIdx++] = b;
                    }
                }
            }));

        }

        if (DEBUG) {
            System.out.println("STARTED THREADS: " + (System.currentTimeMillis() - time));
        }

        for (Thread thread : threads) {
            thread.join();
        }

        TreeMap<String, TemperatureSummary> mergedMap = new TreeMap<>();

        for (var map : maps) {
            for (TemperatureSummary t1 : map.values()) {
                if (t1 == null)
                    continue;

                var t2 = mergedMap.get(new String(t1.name));

                if (t2 == null) {
                    mergedMap.put(new String(t1.name), t1);
                }
                else {
                    var merged = new TemperatureSummary(t1.name, Math.min(t1.min, t2.min), Math.max(t1.max, t2.max), t1.total + t2.total,
                            t1.sampleCount + t2.sampleCount);
                    mergedMap.put(new String(t1.name), merged);
                }
            }
        }

        boolean first = true;
        StringBuilder output = new StringBuilder(16_000);
        output.append("{");
        for (var value : new TreeSet<>(mergedMap.values())) {
            if (first) {
                first = false;
            }
            else {
                output.append(", ");
            }
            output.append(new String(value.name)).append("=").append((double) value.min / 10).append("/")
                    .append(String.format("%.1f", ((double) value.total / value.sampleCount / 10))).append("/").append((double) value.max / 10);
        }
        output.append("}");

        System.out.println(output);
        // if (DEBUG)
        // System.out.println("CORRECT: " + output.toString().equals(CORRECT));

        if (DEBUG)
            System.out.println("TOTAL TIME: " + (System.currentTimeMillis() - time));

    }

    private static int parse(byte[] arr, int len) {
        // TODO: SIMD?
        int num = 0;
        for (int mI = len, m = 1; mI >= 0; mI--) {
            byte d = arr[mI];
            if (d == '.') {
            }
            else if (d == '-') {
                num = -num;
                m *= 10;
            }
            else {
                num += (d & 0xF) * m;
                m *= 10;
            }
        }
        return num;
    }

}
