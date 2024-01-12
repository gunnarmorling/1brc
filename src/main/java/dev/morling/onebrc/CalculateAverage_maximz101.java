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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateAverage_maximz101 {

    private static final String FILE = "./measurements.txt";

    private record Measurement(String station, double value) {
    }

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return STR."\{round(min)}/\{round(mean)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator(double min, double max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }
    }

    record FileChunkRange(long start, long end) {
    }

    public static void main(String[] args) throws IOException {
        int parallelism = args.length == 1 ? Integer.parseInt(args[0]) : Runtime.getRuntime().availableProcessors();

        Map<String, ResultRow> measurements = new ConcurrentHashMap<>();
        try (ExecutorService executor = Executors.newWorkStealingPool(parallelism)) {
            List<FileChunkRange> chunks = getChunks(new File(FILE), parallelism, 1_048_576);
            List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
            for (FileChunkRange chunk : chunks) {
                completableFutureList
                        .add(CompletableFuture
                                .supplyAsync(() -> computePartialAggregations(chunk), executor)
                                .thenAccept(map -> updateResultMap(map, measurements)));
            }
            CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0]))
                    .thenAccept(_ -> System.out.println(new TreeMap<>(measurements)))
                    .join();
        }
    }

    private static void updateResultMap(Map<String, MeasurementAggregator> map, Map<String, ResultRow> measurements) {
        map.forEach((station, agg) -> measurements.merge(station,
                new ResultRow(agg.min, agg.sum / agg.count, agg.max),
                (r1, r2) -> new ResultRow(Math.min(r1.min, r2.min), (r1.mean + r2.mean) / 2, Math.max(r1.max, r2.max))));
    }

    private static Map<String, MeasurementAggregator> computePartialAggregations(FileChunkRange chunk) {
        try (FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, chunk.start(), chunk.end() - chunk.start());
            return process(buffer);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<FileChunkRange> getChunks(File file, int chunksCount, int minChunkSize) {
        long fileSize = file.length();
        long chunkSize = fileSize / chunksCount;

        if (chunkSize < minChunkSize || chunksCount == 1) {
            return List.of(new FileChunkRange(0, fileSize));
        }

        int currentChunk = 1;
        long currentChunkStart = 0;
        long currentChunkEnd = chunkSize;
        var list = new ArrayList<FileChunkRange>(chunksCount);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (currentChunk <= chunksCount) {
                currentChunkEnd = findNextEOLFrom(raf, currentChunkEnd);
                list.add(new FileChunkRange(currentChunkStart, currentChunkEnd));
                // next
                currentChunkStart = currentChunkEnd + 1;
                currentChunkEnd = currentChunkStart + chunkSize;
                if (currentChunkEnd >= fileSize) {
                    list.add(new FileChunkRange(currentChunkStart, fileSize));
                    break;
                }
                currentChunk++;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    private static long findNextEOLFrom(RandomAccessFile raf, long currentChunkEnd) throws IOException {
        raf.seek(currentChunkEnd);
        while (currentChunkEnd < raf.length() && raf.read() != '\n') {
            currentChunkEnd++;
        }
        return currentChunkEnd;
    }

    private static Map<String, MeasurementAggregator> process(MappedByteBuffer buffer) {
        var map = new HashMap<String, MeasurementAggregator>();
        byte[] lineBytes = new byte[107];
        while (buffer.hasRemaining()) {
            int i = 0;
            lineBytes[i] = buffer.get();
            int separatorIdx = -1;
            while (lineBytes[i] != '\n' && buffer.hasRemaining()) {
                lineBytes[++i] = buffer.get();
                if (lineBytes[i] == ';') {
                    separatorIdx = i;
                }
            }
            Measurement measurement = parseLine(lineBytes, separatorIdx, i);
            map.merge(measurement.station,
                    new MeasurementAggregator(measurement.value, measurement.value, measurement.value, 1),
                    (agg, m) -> {
                        agg.min = Math.min(agg.min, m.min);
                        agg.max = Math.max(agg.max, m.max);
                        agg.sum += m.sum;
                        agg.count++;
                        return agg;
                    });
        }
        return map;
    }

    private static Measurement parseLine(byte[] lineBytes, int separatorIdx, int eolIdx) {
        return new Measurement(
                new String(lineBytes, 0, separatorIdx, StandardCharsets.UTF_8),
                bytesToDouble(lineBytes, separatorIdx + 1, eolIdx));
    }

    private static double bytesToDouble(byte[] bytes, int startIdx, int endIdx) {
        double d = 0d;
        boolean negative = bytes[startIdx] == '-';
        int numberStartIdx = negative ? 1 + startIdx : startIdx;
        boolean afterDot = false;
        int dots = 1;
        for (int i = numberStartIdx; i < endIdx; i++) {
            if (bytes[i] == '.') {
                afterDot = true;
                continue;
            }
            double n = bytes[i] - '0';
            if (afterDot) {
                d = d + n / Math.pow(10, dots++);
            }
            else {
                d = d * Math.pow(10, i - numberStartIdx) + n;
            }
        }
        return negative ? -d : d;
    }
}
