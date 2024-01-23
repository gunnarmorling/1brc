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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandle;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.Class.forName;
import static java.lang.System.out;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Comparator.comparing;

public class CalculateAverage_3j5a {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile measurementsFile = new RandomAccessFile(FILE, "r")) {
            var slices = slice(measurementsFile);
            var measurementsChannel = measurementsFile.getChannel();
            slices.stream().parallel().map(slice -> {
                MappedByteBuffer measurementsSlice = map(slice, measurementsChannel);
                var measurementBuffer = new byte[rules.maxMeasurementLength];
                var measurements = HashMap.<Station, StationMeasurementStatistics> newHashMap(rules.uniqueStationsCount);
                while (measurementsSlice.hasRemaining()) {
                    var a = nextStationMeasurement(measurementBuffer, measurementsSlice);
                    var stats = measurements.get(a.station);
                    if (stats == null) {
                        a.station.detachFromMeasurementBuffer();
                        stats = new StationMeasurementStatistics(a);
                        measurements.put(a.station, stats);
                    }
                    else {
                        stats.add(a);
                    }
                }
                return measurements;
            }).reduce((aslice, bslice) -> {
                aslice.forEach((astation, astats) -> {
                    var bstats = bslice.putIfAbsent(astation, astats);
                    if (bstats != null) {
                        bstats.merge(astats);
                    }
                });
                return bslice;
            }).ifPresent(measurements -> {
                var results = new StringBuilder(measurements.size() * (rules.maxStationNameLength + rules.maxStationStatisticsOutputLength));
                measurements.values().stream()
                        .sorted(comparing(StationMeasurementStatistics::getName))
                        .forEach(stationStats -> results.append(stationStats).append(", "));
                out.println("{" + results.substring(0, results.length() - 2) + "}");
            });
        }
    }

    record Rules(int minMeasurementLength, int maxStationNameLength,
                 int maxMeasurementLength, int maxStationStatisticsOutputLength,
                 int uniqueStationsCount) {
        Rules() {
            this(5, 100, 106, 18, 10_000);
        }
    }

    private static final Rules rules = new Rules();

    record MeasurementsSlice(long start, long length) {
    }

    static class Station {

        private byte[] name;
        final int length;
        private int hash;

        private static final MethodHandle vectorizedHashCode;
        private static final int T_BYTE = 8;

        static {
            try {
                var arraysSupport = forName("jdk.internal.util.ArraysSupport");
                Class<?>[] vectorizedHashCodeSignature = { Object.class, int.class, int.class, int.class, int.class };
                var vectorizedHashCodeMethod = arraysSupport.getDeclaredMethod("vectorizedHashCode", vectorizedHashCodeSignature);
                vectorizedHashCode = lookup().unreflect(vectorizedHashCodeMethod);
            }
            catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        Station(byte[] name, int length) {
            this.name = name;
            this.length = length;
        }

        public void detachFromMeasurementBuffer() {
            var n = new byte[length];
            System.arraycopy(name, 0, n, 0, length);
            this.name = n;
        }

        @Override
        public boolean equals(Object that) {
            return Arrays.mismatch(this.name, 0, length, ((Station) that).name, 0, length) < 0;
        }

        @Override
        public int hashCode() {
            if (hash == 0) {
                try {
                    hash = (int) vectorizedHashCode.invokeExact((Object) name, 0, length, 1, T_BYTE);
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            return hash;
        }

    }

    record StationMeasurement(Station station, int temperature) {
    }

    private static class StationMeasurementStatistics {

        private final byte[] bname;
        private String name;
        private int min;
        private int max;
        private long sum;
        private int count = 1;

        StationMeasurementStatistics(StationMeasurement stationMeasurement) {
            this.bname = stationMeasurement.station.name;
            this.min = stationMeasurement.temperature;
            this.max = stationMeasurement.temperature;
            this.sum = stationMeasurement.temperature;
        }

        public String getName() {
            if (name == null) {
                name = new String(bname, StandardCharsets.UTF_8);
            }
            return name;
        }

        void add(StationMeasurement measurement) {
            var temperature = measurement.temperature;
            update(1, temperature, temperature, temperature);
        }

        void merge(StationMeasurementStatistics other) {
            update(other.count, other.min, other.max, other.sum);
        }

        private void update(int count, int min, int max, long sum) {
            this.count += count;
            if (this.min > min) {
                this.min = min;
            }
            if (this.max < max) {
                this.max = max;
            }
            this.sum += sum;
        }

        @Override
        public String toString() {
            var name = getName();
            var min = this.min / 10f;
            var mean = Math.round(this.sum / (float) this.count) / 10f;
            var max = this.max / 10f;
            return new StringBuilder(name.length() + rules.maxStationStatisticsOutputLength)
                    .append(name).append("=").append(min).append("/").append(mean).append("/").append(max)
                    .toString();
        }
    }

    private static StationMeasurement nextStationMeasurement(byte[] measurement, MappedByteBuffer memoryMappedSlice) {
        byte b;
        int i = rules.minMeasurementLength;
        memoryMappedSlice.get(measurement, 0, i);
        while ((b = memoryMappedSlice.get()) != '\n') {
            measurement[i] = b;
            i++;
        }
        var zeroOffset = '0';
        int temperature = measurement[--i] - zeroOffset;
        i--; // skipping dot
        var base = 10;
        while ((b = measurement[--i]) != ';') {
            if (b == '-') {
                temperature = -temperature;
            }
            else {
                temperature = base * (b - zeroOffset) + temperature;
                base *= base;
            }
        }
        return new StationMeasurement(new Station(measurement, i), temperature);
    }

    private static MappedByteBuffer map(MeasurementsSlice slice, FileChannel measurements) {
        try {
            return measurements.map(FileChannel.MapMode.READ_ONLY, slice.start, slice.length);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<MeasurementsSlice> slice(RandomAccessFile measurements) throws IOException {
        int chunks = Runtime.getRuntime().availableProcessors();
        List<MeasurementsSlice> measurementSlices;
        while ((measurementSlices = slice(measurements, chunks)) == null) {
            chunks++;
        }
        return measurementSlices;
    }

    private static List<MeasurementsSlice> slice(RandomAccessFile measurements, int chunks) throws IOException {
        long measurementsFileLength = measurements.length();
        long chunkLength = 0;
        long remainder;
        if (chunks < measurementsFileLength) {
            chunks--;
            do {
                chunkLength = measurementsFileLength / ++chunks;
                remainder = measurementsFileLength % chunkLength;
            } while (chunkLength + remainder > Integer.MAX_VALUE);
        }
        if (chunkLength <= rules.maxMeasurementLength) {
            return List.of(new MeasurementsSlice(0, measurementsFileLength));
        }
        var measurementSlices = new ArrayList<MeasurementsSlice>(chunks);
        var sliceStart = 0L;
        for (int i = 0; i < chunks - 1; i++) {
            var sliceLength = chunkLength;
            measurements.seek(sliceStart + sliceLength);
            while (measurements.readByte() != '\n') {
                measurements.seek(sliceStart + ++sliceLength);
            }
            sliceLength++;
            if (sliceLength > Integer.MAX_VALUE) {
                return null;
            }
            measurementSlices.add(new MeasurementsSlice(sliceStart, sliceLength));
            sliceStart = sliceStart + sliceLength;
        }
        var previousSlice = measurementSlices.getLast();
        var lastSliceStart = previousSlice.start + previousSlice.length;
        measurementSlices.addLast(new MeasurementsSlice(lastSliceStart, measurementsFileLength - lastSliceStart));
        return measurementSlices;
    }

}
