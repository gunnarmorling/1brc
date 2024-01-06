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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_berry120 {

    private static final String FILE = "./measurements.txt";
    public static final int SPLIT_SIZE = 8;

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
        int splitSize = size < 10_000_000 ? 1 : SPLIT_SIZE;
        long inc = (int) (size / splitSize);

        List<Long> positions = new ArrayList<>();
        positions.add(0L);

        long pos = 0;
        for (int i = 0; i < splitSize; i++) {
            long startPos = pos;
            long endPos = pos + inc;
            MappedByteBuffer bb = channel.map(FileChannel.MapMode.READ_ONLY, startPos, endPos - startPos);
            int dec = 1;
            while (bb.get((int) inc - dec) != '\n') {
                dec++;
            }
            endPos -= (dec - 1);
            pos = endPos;
            positions.add(pos);
        }
        positions.add(size);

        List<TemperatureSummary[][]> arrs = Collections.synchronizedList(new ArrayList<>());

        var tp = Executors.newFixedThreadPool(splitSize);

        // System.out.println("SETUP TIME: " + (System.currentTimeMillis() - time));

        for (int i = 1; i < positions.size(); i++) {
            final int idx = i;
            tp.submit(() -> {
                TemperatureSummary[][] arr = new TemperatureSummary[0xFFFFF][1];
                arrs.add(arr);
                try {
                    long startPos = positions.get(idx - 1);
                    long endPos = positions.get(idx);
                    int bSize = (int) (endPos - startPos);
                    MappedByteBuffer bb = channel.map(FileChannel.MapMode.READ_ONLY, startPos, bSize);

                    byte[] bytes = new byte[0xFFFFF];
                    byte[] name = new byte[100];
                    int nameidx = 0;
                    int bytesDataLength;

                    int breakIdx = -1;
                    int rollingHash = 5381;
                    int sourceIdx = 0;
                    int startReadingIdx = 0;

                    bb.get(sourceIdx, bytes, 0, bytesDataLength = Math.min(bytes.length, bSize));

                    int bi = 0;
                    while (true) {

                        byte b = bytes[bi];

                        if (b == ';') {
                            breakIdx = bi + 1;
                            bi += 4;
                        }
                        else if (breakIdx == -1) {
                            // name[nameidx++] = b;
                            // 1M constrained djb2 should be good enough for our purposes
                            rollingHash = (((rollingHash << 5) + rollingHash) + b) & 0xFFFFF;

                            bi++;
                            if (bi >= bytesDataLength)
                                break;

                        }
                        else if (b == '\n') {
                            int numArrLen = bi - breakIdx;
                            int num = 0;
                            for (int mI = breakIdx + numArrLen - 1, m = 1; mI >= breakIdx; mI--, m *= 10) {

                                byte d = bytes[mI];
                                if (d == '.') {
                                    m /= 10;
                                }
                                else if (d == '-') {
                                    num = -num;
                                }
                                else {
                                    num += (d & 0xF) * m;
                                }
                            }

                            byte[] place = new byte[breakIdx - 1 - startReadingIdx];
                            System.arraycopy(bytes, startReadingIdx, place, 0, breakIdx - 1 - startReadingIdx);
                            var entry = arr[rollingHash][0];

                            if (entry == null) {
                                entry = new TemperatureSummary(place, num, num, num, 1);
                                arr[rollingHash][0] = entry;
                            }
                            else {
                                if (Arrays.equals(place, entry.name)) {
                                    entry.max = (Math.max(num, entry.max));
                                    entry.min = (Math.min(num, entry.min));
                                    entry.total += num;
                                    entry.sampleCount++;
                                }
                                else {
                                    TemperatureSummary[] growth = new TemperatureSummary[arr[rollingHash].length + 1];
                                    System.arraycopy(arr[rollingHash], 0, growth, 0, growth.length);
                                    arr[rollingHash] = growth;
                                }
                            }

                            startReadingIdx = bi + 1;
                            breakIdx = -1;
                            rollingHash = 5381;

                            if (bi > bytes.length - 128) {
                                sourceIdx += bi;
                                int remaining = bSize - sourceIdx;
                                bb.get(sourceIdx, bytes, 0, bytesDataLength = Math.min(bytes.length, remaining));
                                bi = -1;
                                if (bytes[0] == 10) {
                                    bi = 0;
                                }
                                startReadingIdx = 1;
                            }

                            bi++;
                            if (bi >= bytesDataLength)
                                break;

                        }
                        else {
                            bi++;
                        }

                    }

                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            });
            // break;
        }

        tp.close();
        tp.awaitTermination(1, TimeUnit.DAYS);

        TreeMap<String, TemperatureSummary> mergedMap = new TreeMap<>();
        // System.out.println("TIME WITHOUT MERGE: " + (System.currentTimeMillis() - time));

        for (var arr : arrs) {
            for (TemperatureSummary[] innerArr : arr) {
                for (TemperatureSummary t1 : innerArr) {
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
        }

        // System.out.println("TIME WITHOUT PRINT: " + (System.currentTimeMillis() - time));

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

//        System.out.println("TIME: " + (System.currentTimeMillis() - time));
    }

}
