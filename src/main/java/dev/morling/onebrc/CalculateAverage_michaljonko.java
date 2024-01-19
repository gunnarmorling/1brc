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
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public final class CalculateAverage_michaljonko {

    private static final int MAX_STATION_NAME_LENGTH = 100;
    private static final int MAX_TEMPERATURE_LENGTH = 5;
    private static final int CPUs = Runtime.getRuntime().availableProcessors();
    private static final String FILE = "./measurements.txt";
    private static final Path PATH = Paths.get(FILE);
    private static final int MAX_STATION_NAMES = 10_000;

    public static void main(String[] args) throws IOException {
        System.out.println(
                sortedResults(
                        calculate(PATH, CPUs)));
    }

    private static String sortedResults(Collection<StationMeasurement> results) {
        return results.stream()
                .sorted(Comparator.comparing(stationMeasurement -> stationMeasurement.station.name))
                .map(StationMeasurement::data)
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static Collection<StationMeasurement> calculate(Path path, int partitionsAmount) {
        var memorySegments = FilePartitioner.createSegments(path, partitionsAmount);

        try (var executorService = Executors.newFixedThreadPool(CPUs)) {
            var futures = new ArrayList<Future<HashMap<Integer, StationMeasurement>>>(memorySegments.size());
            for (var memorySegment : memorySegments) {
                futures.add(executorService.submit(() -> parse(memorySegment)));
            }
            final var finalMap = new HashMap<Station, StationMeasurement>();
            for (var future : futures) {
                future.get().forEach((k, v) -> finalMap.merge(v.station, v, StationMeasurement::update));
            }
            return finalMap.values();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static HashMap<Integer, StationMeasurement> parse(MemorySegment memorySegment) {
        final var stationName = new byte[MAX_STATION_NAME_LENGTH];
        final var temperature = new byte[MAX_TEMPERATURE_LENGTH];
        byte b;
        var stationNameIndex = 0;
        var stationNameHash = 0;
        var temperatureIndex = 0;
        var offset = 0L;
        var memorySegmentSize = memorySegment.byteSize();
        var stationsMap = new HashMap<Integer, StationMeasurement>(MAX_STATION_NAMES);
        while (offset < memorySegmentSize) {
            while ((b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++)) != ';') {
                stationName[stationNameIndex++] = b;
                stationNameHash = 31 * stationNameHash + b;
            }

            while (offset < memorySegmentSize && (b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++)) != '\n') {
                temperature[temperatureIndex++] = b;
            }

            var finalStationNameIndex = stationNameIndex;
            stationsMap.computeIfAbsent(stationNameHash, ignored -> new StationMeasurement(new Station(stationName, finalStationNameIndex)))
                    .update(TemperatureParser.parse(temperature, temperatureIndex));

            stationNameIndex = 0;
            temperatureIndex = 0;
            stationNameHash = 0;
        }
        return stationsMap;
    }

    private static final class Station {

        private static final ConcurrentHashMap<byte[], String> NAME_CACHE = new ConcurrentHashMap<>(MAX_STATION_NAMES);
        private final byte[] raw;
        private final String name;

        private Station(byte[] _raw, int length) {
            this.raw = new byte[length];
            System.arraycopy(_raw, 0, this.raw, 0, length);
            name = NAME_CACHE.compute(this.raw, (k, v) -> v == null ? new String(raw, 0, raw.length) : v);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Station station = (Station) o;

            return Arrays.equals(raw, station.raw);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(raw);
        }
    }

    private static final class StationMeasurement {

        private final Station station;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long count = 0L;
        private long sum = 0L;

        private StationMeasurement(Station station) {
            this.station = station;
        }

        private StationMeasurement update(int value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
            return this;
        }

        private StationMeasurement update(StationMeasurement stationMeasurement) {
            this.count += stationMeasurement.count;
            this.sum += stationMeasurement.sum;
            this.min = Math.min(this.min, stationMeasurement.min);
            this.max = Math.max(this.max, stationMeasurement.max);
            return this;
        }

        private String data() {
            return String.format("%s=%.1f/%.1f/%.1f", station.name, min / 10.0d, (1.0d * sum / count) / 10.0d, max / 10.0d);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StationMeasurement that = (StationMeasurement) o;

            if (min != that.min) {
                return false;
            }
            if (max != that.max) {
                return false;
            }
            if (count != that.count) {
                return false;
            }
            if (sum != that.sum) {
                return false;
            }
            return Objects.equals(station, that.station);
        }

        @Override
        public int hashCode() {
            return station != null ? station.hashCode() : 0;
        }
    }

    private static final class TemperatureParser {

        private static int parse(byte[] raw, int size) {
            var sign = raw[0] == '-' ? -1 : 1;
            var offset = sign > 0 ? 0 : 1;
            var relativeSize = size - offset;
            return sign * switch (relativeSize) {
                case 2 -> (raw[size - 1] - '0');
                case 3 -> (raw[size - 1] - '0') + 10 * (raw[size - 3] - '0');
                case 4 -> (raw[size - 1] - '0') + 10 * (raw[size - 3] - '0') + 100 * (raw[size - 4] - '0');
                default -> 0;
            };
        }
    }

    private static final class FilePartitioner {

        private static final int PARTITIONING_THRESHOLD = 50 * 1_024 * 1_024;

        private static List<MemorySegment> createSegments(Path path, int partitionsAmount) {
            try (var channel = FileChannel.open(PATH, StandardOpenOption.READ)) {
                final var size = Files.size(path);
                final var memorySegment = channel.map(MapMode.READ_ONLY, 0, size, Arena.global());
                if (partitionsAmount < 2 || size < PARTITIONING_THRESHOLD) {
                    return List.of(memorySegment);
                }
                final var partitionSize = size / partitionsAmount;
                final var partitions = new MemorySegment[partitionsAmount];
                var startPosition = 0L;
                var endPosition = partitionSize;
                for (var partitionIndex = 0; partitionIndex < partitionsAmount; partitionIndex++) {
                    if (endPosition >= size) {
                        endPosition = size - 1;
                    }
                    while (endPosition < size
                            && memorySegment.get(ValueLayout.JAVA_BYTE, endPosition) != '\n') {
                        endPosition++;
                    }
                    partitions[partitionIndex] = memorySegment.asSlice(startPosition, endPosition - startPosition);
                    startPosition = ++endPosition;
                    endPosition += partitionSize;
                }
                return List.of(partitions);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
