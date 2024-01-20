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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.lang.System.out;
import static java.util.Arrays.copyOfRange;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;

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
                    var stats = measurements.computeIfAbsent(a.station, k -> new StationMeasurementStatistics(a));
                    stats.add(a);
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
                var results = measurements.values().stream()
                        .parallel()
                        .sorted(comparing(StationMeasurementStatistics::getName))
                        .map(StationMeasurementStatistics::toString)
                        .collect(joining(", ", "{", "}"));
                out.println(results);
            });
        }
    }

    record Rules(int maxMeasurementLength, int uniqueStationsCount) {
        Rules() {
            this(106, 10_000);
        }
    }

    static Rules rules = new Rules();

    record MeasurementsSlice(long start, long length) {
    }

    static class Station {

        final byte[] name;
        private final int hash;

        Station(byte[] name) {
            this.name = name;
            hash = Arrays.hashCode(name);
        }

        @Override
        public boolean equals(Object that) {
            return Arrays.equals(this.name, ((Station) that).name);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    record StationMeasurement(Station station, int temperature) {
    }

    private static class StationMeasurementStatistics {

        private final byte[] nameBytes;
        private String name;
        private int min;
        private int max;
        private long sum;
        private int count;

        StationMeasurementStatistics(StationMeasurement stationMeasurement) {
            this.nameBytes = stationMeasurement.station.name;
            this.min = stationMeasurement.temperature;
            this.max = stationMeasurement.temperature;
        }

        public String getName() {
            if (name == null) {
                name = new String(nameBytes, StandardCharsets.UTF_8);
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
            var min = this.min / 10f;
            var mean = Math.ceilDiv(this.sum, this.count) / 10f;
            var max = this.max / 10f;
            return getName() + "=" + min + "/" + mean + "/" + max;
        }
    }

    private static StationMeasurement nextStationMeasurement(byte[] measurement, MappedByteBuffer memoryMappedSlice) {
        byte b;
        var i = 0;
        while ((b = memoryMappedSlice.get()) != '\n') {
            measurement[i] = b;
            i++;
        }
        int temperature = measurement[--i] - '0';
        i--; // skipping dot
        var base = 10;
        while ((b = measurement[--i]) != ';') {
            if (b == '-') {
                temperature *= -1;
            }
            else {
                temperature = base * (b - '0') + temperature;
                base *= base;
            }
        }
        return new StationMeasurement(new Station(copyOfRange(measurement, 0, i)), temperature);
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
