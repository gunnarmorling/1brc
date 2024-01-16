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
import java.lang.foreign.ValueLayout.OfByte;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class CalculateAverage_michaljonko {

    private static final int CPUs = Runtime.getRuntime().availableProcessors();
    private static final String FILE = "./measurements.txt";
    private static final Path PATH = Paths.get(FILE);
    public static final int MAX_STATION_NAMES = 10_000;

    public static void main(String[] args) throws IOException {
        System.out.println("cpus:" + CPUs);
        System.out.println("file size:" + Files.size(PATH));
        System.out.println("partition size:~" + Files.size(PATH) / CPUs);
        System.out.println();

        System.out.println("--- multi");
        memorySegmentMultiThread();

        // System.out.println("--- single");
        // memorySegmentSingleThread();
    }

    private static void memorySegmentMultiThread() {
        var startTime = System.nanoTime();
        var size = 0L;
        var newLines = 0L;
        var memorySegments = FilePartitioner.createSegments(PATH, CPUs);

        MemorySegment memorySegment = memorySegments.getFirst();
        final byte[] stationName = new byte[100];
        final byte[] temperature = new byte[5];
        byte b;
        int stationNameIndex = 0;
        int stationNameHash = 0;
        int temperatureIndex = 0;
        int temperatureHash = 0;
        long offset = 0L;
        long memorySegmentSize = memorySegment.byteSize();
        ConcurrentHashMap<Integer, Station> stationsMap = new ConcurrentHashMap<>(MAX_STATION_NAMES);
        while (offset < memorySegmentSize) {
            while ((b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++)) != ';') {
                stationName[stationNameIndex++] = b;
                stationNameHash = 31 * stationNameHash + b;
            }
            while (offset < memorySegmentSize && (b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++)) != '\n') {
                temperature[temperatureIndex++] = b;
                temperatureHash = 31 * temperatureHash + b;
            }

            int finalStationNameIndex = stationNameIndex;
            stationsMap.computeIfAbsent(stationNameHash, ignore -> new Station(stationName, finalStationNameIndex));

            stationNameIndex = 0;
            temperatureIndex = 0;
            stationNameHash = 0;
            temperatureHash = 0;
        }

//        stationsMap.forEach((hash, station) -> System.out.println(hash + " : " + station.value()));
        System.out.println("Size: " + stationsMap.size());

        // try (var executorService = Executors.newFixedThreadPool(CPUs)) {
        // var callables = memorySegments.stream()
        // .map(memorySegment -> (Callable<Pair<Long, Long>>) () -> memorySegmentPerThread(memorySegment))
        // .toList();
        // for (var future : executorService.invokeAll(callables)) {
        // var pair = future.get();
        // size += pair.right();
        // newLines += pair.left();
        // }
        // }
        // catch (InterruptedException | ExecutionException e) {
        // throw new RuntimeException(e);
        // }
        // System.out.println("Took :" + Duration.ofNanos(System.nanoTime() - startTime) + " size:" + size + " lines:" + newLines);
    }

    private static final class Station {
        private final byte[] raw;

        private Station(byte[] _raw, int length) {
            this.raw = new byte[length];
            System.arraycopy(_raw, 0, this.raw, 0, length);
        }

        private String value() {
            return new String(raw, 0, raw.length);
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

    private static Pair<Long, Long> memorySegmentPerThread(MemorySegment memorySegment) {
        var newLineCount = 0L;
        var size = 0L;
        final var memorySegmentSize = memorySegment.byteSize();
        final byte[] bytes = new byte[256];
        byte b = 0;
        var index = 0;
        for (long offset = 0; offset < memorySegmentSize; offset++) {
            if ((b = memorySegment.get(ValueLayout.JAVA_BYTE, offset)) == '\n') {
                size += new String(bytes, 0, index, StandardCharsets.UTF_8).length();
                index = 0;
                newLineCount++;
            }
            bytes[index++] = b;
        }
        return new Pair<>(newLineCount, size);
    }

    private static void memorySegmentSingleThread() throws IOException {
        var startTime = System.nanoTime();
        var channel = FileChannel.open(PATH, StandardOpenOption.READ);
        var fileSize = Files.size(PATH);
        var memorySegment = channel.map(MapMode.READ_ONLY, 0, fileSize, Arena.global());
        channel.close();
        var newLineCount = 0L;
        var size = 0L;
        for (long i = 0; i < fileSize; i++) {
            if (memorySegment.get(ValueLayout.JAVA_BYTE, i) == '\n') {
                newLineCount++;
            }
            size++;
        }
        System.out.println("Took:" + Duration.ofNanos(System.nanoTime() - startTime)
                + " size:" + size
                + " newlines:" + newLineCount);

        System.out.println(new String(memorySegment.asSlice(fileSize / 2, 100).toArray(OfByte.JAVA_BYTE), StandardCharsets.UTF_8));
    }

    private record Pair<LEFT,RIGHT>(
    LEFT left, RIGHT right)
    {
    }

    public static final class FilePartitioner {

        private final int partitionsAmount;

        FilePartitioner(int partitionsAmount) {
            this.partitionsAmount = partitionsAmount;
        }

        public static List<MemorySegment> createSegments(Path path, int partitionsAmount) {
            try (var channel = FileChannel.open(PATH, StandardOpenOption.READ)) {
                final var size = Files.size(path);
                if (partitionsAmount < 2) {
                    return List.of(channel.map(MapMode.READ_ONLY, 0, size, Arena.global()));
                }
                final var partitionSize = size / partitionsAmount;
                final var partitions = new MemorySegment[partitionsAmount];
                final var buffer = ByteBuffer.allocateDirect(128);
                var startPosition = 0L;
                var endPosition = partitionSize;
                for (var partitionIndex = 0; partitionIndex < partitionsAmount; partitionIndex++) {
                    channel.read(buffer.clear(), endPosition >= size ? endPosition = (size - 1) : endPosition);
                    buffer.flip();
                    while (buffer.get() != '\n' && endPosition < size) {
                        endPosition++;
                    }
                    partitions[partitionIndex] = channel.map(MapMode.READ_ONLY, startPosition, endPosition - startPosition, Arena.ofShared());
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

    public static final class ParsingCache {

        private final ConcurrentHashMap<String, Double> cache = new ConcurrentHashMap<>(10_000);

        private static double parseRawDouble(String rawValue) {
            if (rawValue == null) {
                return Double.NaN;
            }
            final var rawValueArray = rawValue.toCharArray();
            if (rawValueArray.length == 0) {
                return Double.NaN;
            }

            final var sign = (rawValueArray[0] - '-') == 0 ? -1 : 1;
            final var arrayBeginning = sign > 0 ? 0 : 1;
            var separatorIndex = -1;
            var integer = 0;

            for (int index = rawValueArray.length - 1, factor = 1; index >= arrayBeginning; index--) {
                if (rawValueArray[index] == '.') {
                    separatorIndex = index;
                }
                else {
                    integer += (rawValueArray[index] - '0') * factor;
                    factor *= 10;
                }
            }

            if (separatorIndex < 0) {
                return sign * integer;
            }

            var div = 1;
            while (rawValueArray.length - (++separatorIndex) > 0) {
                div *= 10;
            }

            return 1.0d * sign * integer / div;
        }

        public double parseIfAbsent(String rawValue) {
            return cache.computeIfAbsent(rawValue, ParsingCache::parseRawDouble);
        }
    }
}
