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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_yonatang {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./measurements_100M.txt";

    private static final int DICT_SIZE = 12000;
    private static final int DICT_RECORD_SIZE = 3;
    private static final int DICT_SIZE_BYTES = DICT_SIZE * DICT_RECORD_SIZE;
    private static final long NO_VALUE = Long.MIN_VALUE;

    private static class HashTable {

        // Continuous array of [key, min, max, count, sum], which will be more CPU cache friendly.
        private final long[] data = new long[DICT_SIZE_BYTES];
        private int size = 0;

        public HashTable() {
            long d1 = ((Short.MAX_VALUE) | (Short.MIN_VALUE << 16)) & 0xFFFFFFFFL;
            for (int i = 0; i < DICT_SIZE_BYTES; i += 3) {
                data[i] = NO_VALUE;
                data[i + 1] = d1;
                data[i + 2] = 0;
            }
        }

        public int size() {
            return size;
        }

        private int getIndex(long key) {
            int idx = Math.abs((int) (key % DICT_SIZE)) * DICT_RECORD_SIZE;

            // data[idx]!=key should be first, it is more likely to stop there
            while (data[idx] != key && data[idx] != NO_VALUE) {
                idx += 3;
                if (idx >= DICT_SIZE_BYTES) {
                    idx = 0;
                }
            }
            if (data[idx] == NO_VALUE) {
                data[idx] = key;
                size++;
            }
            return idx;
        }

        private void addRawMeasurementAgg(long key, long d1, long d2) {
            int idx = getIndex(key);
            short currentMin = (short) (data[idx + 1] & 0xFFFF);
            short currentMax = (short) ((data[idx + 1] >> 16) & 0xFFFF);
            int currentCount = (int) ((data[idx + 1] >> 32) & 0xFFFFFFFF);

            short thisMin = (short) (d1 & 0xFFFF);
            short thisMax = (short) ((d1 >> 16) & 0xFFFF);
            int thisCount = (int) ((d1 >> 32) & 0xFFFFFFFF);

            thisMin = (short) Math.min(thisMin, currentMin);
            thisMax = (short) Math.max(thisMax, currentMax);
            thisCount += currentCount;

            data[idx + 1] = ((long) thisMin & 0xFFFF) | (((long) thisMax & 0xFFFF) << 16) | (((long) thisCount & 0xFFFFFFFF) << 32);

            data[idx + 2] += d2;
        }

        public TreeMap<String, ResultRow> toMap(HashMap<Long, String> nameDict) {
            TreeMap<String, ResultRow> finalMap = new TreeMap<>();
            for (int i = 0; i < DICT_SIZE_BYTES; i += DICT_RECORD_SIZE) {
                if (data[i] != NO_VALUE) {
                    short min = (short) (data[i + 1] & 0xFFFF);
                    short max = (short) ((data[i + 1] >> 16) & 0xFFFF);
                    int count = (int) ((data[i + 1] >> 32) & 0xFFFFFFFF);
                    long sum = data[i + 2];
                    String station = nameDict.get(data[i]);
                    finalMap.put(station, new ResultRow(min / 10.0, (sum / 10.0) / count, max / 10.0));
                }
            }
            return finalMap;
        }

        public void addMeasurement(long key, short temp) {
            int idx = getIndex(key);
            short min = (short) (data[idx + 1] & 0xFFFF);
            short max = (short) ((data[idx + 1] >> 16) & 0xFFFF);
            int count = (int) ((data[idx + 1] >> 32) & 0xFFFFFFFF);
            min = (short) Math.min(min, temp);
            max = (short) Math.max(max, temp);
            count += 1;

            data[idx + 1] = ((long) min & 0xFFFF) | (((long) max & 0xFFFF) << 16) | (((long) count & 0xFFFFFFFF) << 32);
            data[idx + 2] += temp;
        }

        public void mergeInto(HashTable other) {
            for (int i = 0; i < DICT_SIZE_BYTES; i += 3) {
                if (data[i] != NO_VALUE) {
                    other.addRawMeasurementAgg(data[i], data[i + 1], data[i + 2]);
                }
            }
        }

    }

    private static class ResultRow {
        final double min;
        final double mean;
        final double max;

        ResultRow(double min, double mean, double max) {
            this.min = min;
            this.mean = mean;
            this.max = max;
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class StationId {
        final long stationId;
        final int len;

        public StationId(long stationId, int len) {
            this.stationId = stationId;
            this.len = len;
        }
    }

    private static StationId parseStationId(MappedByteBuffer byteBuffer) {
        long stationId = 0L;
        boolean valid = false;
        int len = 0;
        while (byteBuffer.hasRemaining()) {
            byte ch = byteBuffer.get();
            if (ch == '\n') {
                continue;
            }
            if (ch == ';') {
                valid = true;
                break;
            }
            long theNew = ((long) ch) << (len * 8);
            stationId = stationId ^ theNew;
            len++;
        }
        if (!valid) {
            return null;
        }
        return new StationId(stationId, len);
    }

    private static short parseShort(MappedByteBuffer byteBuffer) {
        boolean valid = false;
        boolean negative = false;
        int num = 0;
        while (byteBuffer.hasRemaining()) {
            byte ch = byteBuffer.get();
            if (ch == '\n') {
                valid = true;
                break;
            }
            if (ch == '-') {
                negative = true;
            }
            else if (ch == '.') {
                // noop
            }
            else {
                num = (num * 10 + (ch - '0'));
            }
        }
        if (!valid) {
            return Short.MIN_VALUE;
        }

        return (short) (negative ? -num : num);
    }

    private static final int MARGIN = 130;

    private static void processChunk(FileChannel fc, int j, long chunkSize, HashTable[] maps, boolean isLast) {
        try {
            HashTable agg = new HashTable();
            maps[j] = agg;

            long startIdx = Math.max(j * chunkSize - MARGIN, 0);
            int padding;
            if (isLast) {
                chunkSize = fc.size() - startIdx;
                padding = 0;
            }
            else {
                padding = j == 0 ? 0 : MARGIN;
            }
            if (chunkSize == 0) {
                return;
            }
            MappedByteBuffer byteBuffer = fc.map(FileChannel.MapMode.READ_ONLY, startIdx, chunkSize + padding);
            // search back for the actual start line, at \n
            if (startIdx > 0) {
                int i = MARGIN;
                while (i > 0) {
                    byte ch = byteBuffer.get(i);
                    if (ch == '\n') {
                        break;
                    }
                    i--;
                }
                byteBuffer.position(i);
            }

            while (byteBuffer.hasRemaining()) {
                StationId stationIdWrapper = parseStationId(byteBuffer);
                if (stationIdWrapper == null) {
                    continue;
                }
                long stationId = stationIdWrapper.stationId;
                short value = parseShort(byteBuffer);
                if (value == Short.MIN_VALUE) {
                    continue;
                }
                agg.addMeasurement(stationId, value);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String readStationName(MappedByteBuffer bb, int len) {
        byte[] chars = new byte[len];
        int firstPos = bb.position() - len - 1;
        for (int i = 0; i < len; i++) {
            chars[i] = bb.get(firstPos + i);
        }
        return new String(chars, Charset.defaultCharset());
    }

    private static HashMap<Long, String> createNameDict(int expected, FileChannel fc) throws Exception {
        HashMap<Long, String> nameDict = new HashMap<>(expected);
        // If one of the station appear only after 2gb of file, we're screwed.
        // statistically impossible.
        long size = Math.min(Integer.MAX_VALUE, fc.size());
        MappedByteBuffer byteBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);
        while (byteBuffer.hasRemaining()) {
            StationId stationIdWrapper = parseStationId(byteBuffer);
            if (stationIdWrapper == null) {
                throw new RuntimeException("Err");
            }
            int len = stationIdWrapper.len;
            long stationId = stationIdWrapper.stationId;
            if (!nameDict.containsKey(stationId)) {
                String name = readStationName(byteBuffer, len);
                nameDict.put(stationId, name);
                if (nameDict.size() == expected) {
                    return nameDict;
                }
            }
            boolean valid = false;
            while (byteBuffer.hasRemaining()) {
                byte ch = byteBuffer.get();
                if (ch == '\n') {
                    valid = true;
                    break;
                }
            }
            if (!valid) {
                throw new RuntimeException("Err");
            }
        }
        throw new RuntimeException("Err");
    }

    public static void main(String[] args) throws Exception {
        // long start = System.nanoTime();

        File f = new File(FILE);
        try (RandomAccessFile raf = new RandomAccessFile(f, "r");
                FileChannel fc = raf.getChannel()) {

            int chunks = f.length() < 1_048_576 ? 1 : (Runtime.getRuntime().availableProcessors());

            long chunkSize = f.length() / chunks;

            Thread[] threads = new Thread[chunks];
            HashTable totalAgg = new HashTable();
            HashTable[] maps = new HashTable[chunks];

            for (int i = 0; i < chunks; i++) {
                final int j = i;
                Thread thread = new Thread(() -> processChunk(fc, j, chunkSize, maps, j == chunks - 1));
                threads[i] = thread;
                thread.start();
            }
            for (int i = 0; i < chunks; i++) {
                threads[i].join();
                maps[i].mergeInto(totalAgg);
            }

            HashMap<Long, String> nameDict = createNameDict(totalAgg.size(), fc);
            Map<String, ResultRow> finalMap = totalAgg.toMap(nameDict);
            // long end = System.nanoTime();

            System.out.println(finalMap);
            // System.out.println("Total time: " + Duration.ofNanos(end - start).toMillis() + "ms");
        }

    }
}
