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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_yonatang {
    private static final String FILE = "./measurements.txt";

    private static class Measurement {
        final long stationId;
        final double value;

        public Measurement(long stationId, double value) {
            this.stationId = stationId;
            this.value = value;
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

    private static class MeasurementAggregator {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum;
        long count;
    }

    private static class StationId {
        final long stationId;
        final int len;

        public StationId(long stationId, int len) {
            this.stationId = stationId;
            this.len = len;
        }
    }

    private static StationId parseStationId(MappedByteBuffer bb) {
        long stationId = 0L;
        boolean valid = false;
        int len = 0;
        while (bb.hasRemaining()) {
            byte ch = bb.get();
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

    private static double parseDouble(MappedByteBuffer bb) {
        boolean valid = false;
        boolean negative = false;
        int num = 0;
        while (bb.hasRemaining()) {
            byte ch = bb.get();
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
                num = num * 10 + (ch - '0');
            }
        }
        if (!valid) {
            return Double.NaN;
        }
        return (negative ? -num : num) / 10.0;
    }

    private static Measurement parseMeasurement(MappedByteBuffer bb) {
        StationId stationIdWrapper = parseStationId(bb);
        if (stationIdWrapper == null) {
            return null;
        }
        long stationId = stationIdWrapper.stationId;
        double d = parseDouble(bb);
        if (Double.isNaN(d)) {
            return null;
        }
        return new Measurement(stationId, d);
    }

    private static final int MARGIN = 130;
    private static final int MAX_KEYS = 10000;

    private static void processChunk(FileChannel fc, int j, long chunkSize, HashMap[] maps, boolean isLast) {
        try {
            HashMap<Long, MeasurementAggregator> agg = new HashMap<>(MAX_KEYS);
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
                Measurement measurement = parseMeasurement(byteBuffer);
                if (measurement != null) {
                    MeasurementAggregator magg = agg.computeIfAbsent(measurement.stationId, ma -> new MeasurementAggregator());
                    magg.min = Math.min(magg.min, measurement.value);
                    magg.max = Math.max(magg.max, measurement.value);
                    magg.sum += measurement.value;
                    magg.count++;
                }

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
        MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);
        while (bb.hasRemaining()) {
            StationId stationIdWrapper = parseStationId(bb);
            if (stationIdWrapper == null) {
                throw new RuntimeException("Err");
            }
            int len = stationIdWrapper.len;
            long stationId = stationIdWrapper.stationId;
            if (!nameDict.containsKey(stationId)) {
                String name = readStationName(bb, len);
                nameDict.put(stationId, name);
                if (nameDict.size() == expected) {
                    return nameDict;
                }
            }
            boolean valid = false;
            while (bb.hasRemaining()) {
                byte ch = bb.get();
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

        File f = new File(FILE);
        try (RandomAccessFile raf = new RandomAccessFile(f, "r");
                FileChannel fc = raf.getChannel()) {

            int chunks = f.length() < 1_048_576 ? 1 : Runtime.getRuntime().availableProcessors();

            long chunkSize = f.length() / chunks;

            Thread[] threads = new Thread[chunks];
            HashMap<Long, MeasurementAggregator> totalAgg = new HashMap<>(MAX_KEYS);
            HashMap[] maps = new HashMap[chunks];

            for (int i = 0; i < chunks; i++) {
                final int j = i;
                Thread thread = new Thread(() -> processChunk(fc, j, chunkSize, maps, j == chunks - 1));
                threads[i] = thread;
                thread.start();
            }
            for (int i = 0; i < chunks; i++) {
                threads[i].join();
                maps[i].forEach((k, ga) -> {
                    MeasurementAggregator a = (MeasurementAggregator) ga;
                    MeasurementAggregator totalAgg2 = totalAgg.computeIfAbsent((Long) k, ma -> new MeasurementAggregator());
                    totalAgg2.min = Math.min(totalAgg2.min, a.min);
                    totalAgg2.max = Math.max(totalAgg2.max, a.max);
                    totalAgg2.sum += a.sum;
                    totalAgg2.count += a.count;
                });
            }
            Map<String, ResultRow> finalMap = new TreeMap<>();
            HashMap<Long, String> nameDict = createNameDict(totalAgg.size(), fc);

            totalAgg.entrySet().stream().forEach(e -> {
                MeasurementAggregator v = e.getValue();
                String station = nameDict.get(e.getKey());
                finalMap.put(station, new ResultRow(v.min, (Math.round(v.sum * 10.0) / 10.0) / v.count, v.max));
            });
            System.out.println(finalMap);
        }

    }
}
