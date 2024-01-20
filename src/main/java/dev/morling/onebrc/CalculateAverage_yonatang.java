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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_yonatang {
    private static final String FILE = "./measurements.txt";

    private static final int DICT_OFFSET_STATION = 2;
    private static final int DICT_OFFSET_SUM = 1;
    private static final int DICT_SIZE = 15000;
    private static final int DICT_STATION_RECORD_SIZE = 13;
    private static final int DICT_RECORD_SIZE = DICT_OFFSET_STATION + DICT_STATION_RECORD_SIZE;
    private static final int DICT_SIZE_BYTES = DICT_SIZE * DICT_RECORD_SIZE;
    private static final long[] DICT_ZERO_RECORD = new long[DICT_RECORD_SIZE];
    private static final long DICT_BASELINE_MEASURES = ((long) Short.MAX_VALUE & 0xFFFF) | (((long) Short.MIN_VALUE & 0xFFFF) << 16);

    public static class HashTable {

        // Continuous array of [key, min, max, count, sum], which will be more CPU cache friendly.
        private final long[] data = new long[DICT_SIZE_BYTES];

        public HashTable() {
            for (int i = 0; i < DICT_SIZE_BYTES; i += DICT_RECORD_SIZE) {
                data[i] = DICT_BASELINE_MEASURES;
            }
        }

        private int getIndex(long[] station) {
            long key = 0;
            short len = (short) (station[0] & 0xFF);
            int longs = ((len + 1) / 8) + 1;
            for (int i = 0; i < longs; i++) {
                key = key ^ station[i];
            }
            int idx = Math.abs((int) (key % DICT_SIZE)) * DICT_RECORD_SIZE;

            while (true) {
                if (data[idx] == DICT_BASELINE_MEASURES) {
                    break;
                }
                if (Arrays.equals(station, 0, longs,
                        data,
                        idx + DICT_OFFSET_STATION, idx + DICT_OFFSET_STATION + longs)) {
                    break;
                }
                idx += DICT_RECORD_SIZE;
                if (idx >= DICT_SIZE_BYTES) {
                    idx = 0;
                }
            }
            return idx;
        }

        private void addRawMeasurementAgg(long[] title, long measurements, long sum) {
            int idx = getIndex(title);
            short currentMin = (short) (data[idx] & 0xFFFF);
            short currentMax = (short) ((data[idx] >> 16) & 0xFFFF);
            int currentCount = (int) (data[idx] >> 32);

            short thisMin = (short) (measurements & 0xFFFF);
            short thisMax = (short) ((measurements >> 16) & 0xFFFF);
            int thisCount = (int) (measurements >> 32);

            thisMin = (short) Math.min(thisMin, currentMin);
            thisMax = (short) Math.max(thisMax, currentMax);
            thisCount += currentCount;

            data[idx] = ((long) thisMin & 0xFFFF) | (((long) thisMax & 0xFFFF) << 16) | (((long) thisCount) << 32);

            data[idx + DICT_OFFSET_SUM] += sum;
            System.arraycopy(title, 0, data, idx + DICT_OFFSET_STATION, DICT_STATION_RECORD_SIZE);
        }

        public TreeMap<String, ResultRow> toMap() {
            TreeMap<String, ResultRow> finalMap = new TreeMap<>();
            byte[] bytes = new byte[128];
            ByteBuffer bb = ByteBuffer.allocate(136);
            bb.order(ByteOrder.nativeOrder());
            for (int i = 0; i < DICT_SIZE_BYTES; i += DICT_RECORD_SIZE) {
                if (data[i] == DICT_BASELINE_MEASURES)
                    continue;

                short min = (short) (data[i] & 0xFFFF);
                short max = (short) ((data[i] >> 16) & 0xFFFF);
                int count = (int) (data[i] >> 32);
                long sum = data[i + DICT_OFFSET_SUM];
                for (int j = 0; j < DICT_STATION_RECORD_SIZE; j++) {
                    bb.putLong(data[i + DICT_OFFSET_STATION + j]);
                }
                bb.flip();
                byte len = bb.get();
                bb.get(1, bytes, 0, len);
                bb.clear();
                String station = new String(bytes, 0, len, Charset.defaultCharset());
                finalMap.put(station, new ResultRow(min / 10.0, (sum / 10.0) / count, max / 10.0));

            }
            return finalMap;
        }

        public void addMeasurement(long[] title, short temp) {
            int idx = getIndex(title);
            short min = (short) (data[idx] & 0xFFFF);
            short max = (short) ((data[idx] >> 16) & 0xFFFF);
            int count = (int) (data[idx] >> 32);
            min = (short) Math.min(min, temp);
            max = (short) Math.max(max, temp);
            count += 1;

            data[idx] = ((long) min & 0xFFFF) | (((long) max & 0xFFFF) << 16) | (((long) count) << 32);
            data[idx + DICT_OFFSET_SUM] += temp;
            System.arraycopy(title, 0, data, idx + DICT_OFFSET_STATION, DICT_STATION_RECORD_SIZE);
        }

        public void mergeInto(HashTable other) {
            long[] title = new long[DICT_STATION_RECORD_SIZE];
            for (int i = 0; i < DICT_SIZE_BYTES; i += DICT_RECORD_SIZE) {
                if (data[i] == DICT_BASELINE_MEASURES)
                    continue;
                System.arraycopy(data, i + DICT_OFFSET_STATION, title, 0, DICT_STATION_RECORD_SIZE);
                other.addRawMeasurementAgg(title, data[i], data[i + DICT_OFFSET_SUM]);
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

    public static boolean parseStation(MappedByteBuffer byteBuffer, ByteBuffer tempBb, long[] station) {
        System.arraycopy(DICT_ZERO_RECORD, 0, station, 0, DICT_STATION_RECORD_SIZE);
        byte len = 1;
        boolean valid = false;
        tempBb.clear();
        tempBb.put((byte) 0);
        while (byteBuffer.hasRemaining()) {
            byte ch = byteBuffer.get();
            if (ch == '\n') {
                continue;
            }
            if (ch == ';') {
                valid = true;
                break;
            }
            tempBb.put(ch);
            // long theNew = ((long) ch) << (len * 8);
            // stationId[0] = stationId[0] ^ theNew;
            // int arrIdx = len / 8;
            // station[arrIdx] = station[arrIdx] ^ theNew;
            len++;
        }
        tempBb.put(0, (byte) (len - 1));
        if (!valid) {
            return false;
        }
        tempBb.position(0);
        tempBb.asLongBuffer().get(station);

        int pivotIdx = (len) / 8;
        long pivotBits = (len % 8) * 8;
        long pivotMask = (1L << pivotBits) - 1;
        station[pivotIdx] = station[pivotIdx] & pivotMask;
        return true;
    }

    public static short parseShort(MappedByteBuffer byteBuffer) {
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
            long[] station = new long[DICT_STATION_RECORD_SIZE];
            ByteBuffer tempBb = ByteBuffer.allocate((DICT_STATION_RECORD_SIZE + 1) * Long.BYTES);
            tempBb.order(ByteOrder.nativeOrder());

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
                if (!parseStation(byteBuffer, tempBb, station)) {
                    continue;
                }
                short value = parseShort(byteBuffer);
                if (value == Short.MIN_VALUE) {
                    continue;
                }
                agg.addMeasurement(station, value);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
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

            Map<String, ResultRow> finalMap = totalAgg.toMap();
            // long end = System.nanoTime();

            System.out.println(finalMap);
            // System.err.println("Total time: " + java.time.Duration.ofNanos(end - start).toMillis() + "ms");
        }

    }
}
