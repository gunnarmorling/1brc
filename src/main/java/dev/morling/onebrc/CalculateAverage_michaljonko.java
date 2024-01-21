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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.FormatProcessor.FMT;

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
        try (var parseExecutorService = Executors.newFixedThreadPool(partitionsAmount);
                var mergeExecutorService = Executors.newSingleThreadExecutor()) {
            final var memorySegments = FilePartitioner.createSegments(path, partitionsAmount);
            final var parseFutures = new ArrayList<Future<Collection<StationMeasurement>>>(memorySegments.size());
            final var results = new HashMap<Station, StationMeasurement>();
            for (var memorySegment : memorySegments) {
                parseFutures.add(parseExecutorService.submit(() -> parse(memorySegment)));
            }
            Future<?> lastFuture = null;
            for (var future : parseFutures) {
                final var futureResult = future.get();
                lastFuture = mergeExecutorService.submit(
                        () -> futureResult.forEach(v -> results.merge(v.station, v, StationMeasurement::update)));
            }
            lastFuture.get();
            return results.values();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static Collection<StationMeasurement> parse(MemorySegment memorySegment) {
        final var stationNameRaw = new byte[MAX_STATION_NAME_LENGTH];
        final var temperatureRaw = new byte[MAX_TEMPERATURE_LENGTH];
        final var memorySegmentSize = memorySegment.byteSize();
        final var stationsMap = new HashMap<StationHash, StationMeasurement>(2 * MAX_STATION_NAMES);

        var stationNameRawIndex = 0;
        var stationNameHashLow = 0;
        var stationNameHashHigh = 97;
        var temperatureRawIndex = 0;
        var memorySegmentOffset = 0L;

        byte b;
        StationMeasurement stationMeasurement;
        StationHash stationHash;
        var temperature = 0;

        while (memorySegmentOffset < memorySegmentSize) {
            while ((b = memorySegment.get(ValueLayout.JAVA_BYTE, memorySegmentOffset++)) != ';') {
                stationNameRaw[stationNameRawIndex++] = b;
                stationNameHashLow = (31 * stationNameHashLow + b);
                stationNameHashHigh = (998551 * stationNameHashHigh + b);
            }

            while (memorySegmentOffset < memorySegmentSize && (b = memorySegment.get(ValueLayout.JAVA_BYTE, memorySegmentOffset++)) != '\n') {
                temperatureRaw[temperatureRawIndex++] = b;
            }
            temperature = TemperatureParser.parse(temperatureRaw, temperatureRawIndex);

            stationHash = new StationHash(stationNameHashLow, stationNameHashHigh);
            stationMeasurement = stationsMap.get(stationHash);
            if (stationMeasurement != null) {
                stationMeasurement.update(temperature);
            }
            else {
                stationsMap.put(stationHash, new StationMeasurement(new Station(stationNameRaw, stationNameRawIndex), temperature));
            }

            stationNameRawIndex = 0;
            temperatureRawIndex = 0;
            stationNameHashLow = 0;
            stationNameHashHigh = 97;
        }
        return stationsMap.values();
    }

    private static final class StationHash {
        private final int low;
        private final int high;

        private StationHash(int low, int high) {
            this.low = low;
            this.high = high;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            final StationHash that = (StationHash) o;
            return low == that.low && high == that.high;
        }

        @Override
        public int hashCode() {
            return high;
        }
    }

    private static final class Station {

        private final byte[] raw;
        private final String name;
        private final int hashCode;

        private Station(byte[] _raw, int length) {
            this.raw = new byte[length];
            System.arraycopy(_raw, 0, this.raw, 0, length);
            this.name = new String(raw, 0, raw.length);
            this.hashCode = Arrays.hashCode(raw);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }

            Station station = (Station) o;

            return Arrays.equals(raw, station.raw);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    private static final class StationMeasurement {

        private final Station station;
        private int min;
        private int max;
        private long count;
        private long sum;

        private StationMeasurement(Station station, int temperature) {
            this.station = station;
            this.min = this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        private StationMeasurement update(int temperature) {
            if (temperature < min) {
                min = temperature;
            }
            if (temperature > max) {
                max = temperature;
            }
            sum += temperature;
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
            final var name = station.name;
            final var min = this.min / 10.0d;
            final var avg = (1.0d * sum / count) / 10.0d;
            final var max = this.max / 10.0d;
            return FMT. "\{name}=%.1f\{min}/%.1f\{avg}/%.1f\{max}";
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

            return min == that.min
                    && max == that.max
                    && count == that.count
                    && sum == that.sum
                    && Objects.equals(station, that.station);
        }

        @Override
        public int hashCode() {
            return station.hashCode();
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
                var size = Files.size(path);
                var memorySegment = channel.map(MapMode.READ_ONLY, 0, size, Arena.global());
                if (partitionsAmount < 2 || size < PARTITIONING_THRESHOLD) {
                    return List.of(memorySegment);
                }
                var partitionSize = size / partitionsAmount;
                var partitions = new MemorySegment[partitionsAmount];
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
