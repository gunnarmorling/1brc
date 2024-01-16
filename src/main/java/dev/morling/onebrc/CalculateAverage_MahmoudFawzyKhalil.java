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
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

// Solution using project Panama and Map Reduce
public class CalculateAverage_MahmoudFawzyKhalil {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {
        mapReduce();
    }

    private static void mapReduce() throws IOException {
        var f = new File(FILE);
        try (var raf = new RandomAccessFile(f, "r")) {
            FileChannel channel = raf.getChannel();
            long fileSize = channel.size();
            MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            long chunkSize = fileSize / ForkJoinPool.commonPool().getParallelism();
            List<Chunk> chunks = getChunks(ms, chunkSize);
            Map<String, MeasurementAggregate> result = chunks.stream()
                    .parallel()
                    .map(c -> readChunkToMap(c, ms))
                    .reduce(Collections.emptyMap(), (a, b) -> combine(a, b));
            System.out.println(new TreeMap<>(result));
        }
    }

    private static List<Chunk> getChunks(MemorySegment ms, long chunkSize) {
        List<Chunk> chunks = new ArrayList<>(32);
        long start = 0;
        long fileSize = ms.byteSize();
        long end = chunkSize;

        while (start < fileSize) {
            byte b = ms.get(ValueLayout.JAVA_BYTE, end);
            if (b == '\n') {
                chunks.add(new Chunk(start, end));
                start = end + 1;
                end = Math.min(end + chunkSize, fileSize - 2);
            }
            end++;
        }
        return chunks;
    }

    private static Map<String, MeasurementAggregate> readChunkToMap(Chunk chunk, MemorySegment ms) {
        Map<String, MeasurementAggregate> map = new HashMap<>();

        long start = chunk.start();
        while (start < chunk.end()) {
            long cityNameSize = 0;
            while (ms.get(ValueLayout.JAVA_BYTE, start + cityNameSize) != ';') {
                cityNameSize++;
            }

            String cityName = readString(ms, start, cityNameSize);
            start = start + cityNameSize + 1;

            long temperatureSize = 0;
            while (ms.get(ValueLayout.JAVA_BYTE, start + temperatureSize) != '\n') {
                temperatureSize++;
            }

            String temperature = readString(ms, start, temperatureSize);
            start = start + temperatureSize + 1;

            // System.out.println(STR."\{cityName};\{temperature}");
            addMeasurement(map, cityName, temperature);
        }

        return map;
    }

    // Credit goes to imrafaelmerino for combine function
    private static Map<String, MeasurementAggregate> combine(Map<String, MeasurementAggregate> xs, Map<String, MeasurementAggregate> ys) {
        Map<String, MeasurementAggregate> result = new HashMap<>();

        for (var key : xs.keySet()) {
            var m1 = xs.get(key);
            var m2 = ys.get(key);
            var combined = (m2 == null) ? m1 : (m1 == null) ? m2 : m1.combine(m2);
            result.put(key, combined);
        }

        for (var key : ys.keySet())
            result.putIfAbsent(key, ys.get(key));
        return result;
    }

    private static String readString(MemorySegment ms, long start, long size) {
        byte[] stringBytes = ms.asSlice(start, size)
                .toArray(ValueLayout.JAVA_BYTE);
        return new String(stringBytes);
    }

    private static void addMeasurement(Map<String, MeasurementAggregate> measurements, String station, String reading) {
        measurements.compute(station,
                (_, oldMeasurements) -> oldMeasurements == null ? MeasurementAggregate.of(reading) : oldMeasurements.update(reading));
    }

    record Chunk(long start, long end) {
    }

    private static final class MeasurementAggregate {
        private double min;
        private double max;
        private double sum;
        private long count;

        private MeasurementAggregate(double min, double max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public static MeasurementAggregate of(String temperature) {
            double measurement = Double.parseDouble(temperature);
            return new MeasurementAggregate(measurement, measurement, measurement, 1);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (MeasurementAggregate) obj;
            return Double.doubleToLongBits(this.min) == Double.doubleToLongBits(that.min) &&
                    Double.doubleToLongBits(this.max) == Double.doubleToLongBits(that.max) &&
                    Double.doubleToLongBits(this.sum) == Double.doubleToLongBits(that.sum) &&
                    this.count == that.count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, max, sum, count);
        }

        public MeasurementAggregate update(String part) {
            double measurement = Double.parseDouble(part);
            this.min = Math.min(this.min, measurement);
            this.max = Math.max(this.max, measurement);
            this.sum += measurement;
            this.count++;
            return this;
        }

        public String toString() {
            return min + "/" + round(round(sum) / count) + "/" + max;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public MeasurementAggregate combine(MeasurementAggregate m2) {
            return new MeasurementAggregate(
                    Math.min(this.min, m2.min),
                    Math.max(this.max, m2.max),
                    this.sum + m2.sum,
                    this.count + m2.count);
        }
    }
}
