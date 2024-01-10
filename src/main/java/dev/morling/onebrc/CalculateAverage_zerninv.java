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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_zerninv {
    private static final String FILE = "./measurements.txt";
    private static final int MIN_FILE_SIZE = 1024 * 1024;
    private static final char DELIMITER = ';';
    private static final char LINE_SEPARATOR = '\n';
    private static final char ZERO = '0';
    private static final char NINE = '9';
    private static final char MINUS = '-';

    public static void main(String[] args) throws IOException {
        var results = new HashMap<String, MeasurementAggregation>();
        try (var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            var fileSize = channel.size();
            var cores = Runtime.getRuntime().availableProcessors();
            var chunks = cores - 1;
            var maxChunkSize = fileSize < MIN_FILE_SIZE ? fileSize : Math.min(fileSize / chunks, Integer.MAX_VALUE);
            var chunkOffsets = splitByChunks(channel, maxChunkSize);

            var executor = Executors.newFixedThreadPool(cores);
            List<Future<Map<String, MeasurementAggregation>>> fResults = new ArrayList<>();
            for (int i = 1; i < chunkOffsets.size(); i++) {
                final long prev = chunkOffsets.get(i - 1);
                final long curr = chunkOffsets.get(i);
                fResults.add(executor.submit(() -> calcForChunk(channel, prev, curr)));
            }

            fResults.forEach(f -> {
                try {
                    f.get().forEach((key, value) -> {
                        var result = results.get(key);
                        if (result != null) {
                            result.merge(value);
                        }
                        else {
                            results.put(key, value);
                        }
                    });
                }
                catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
            executor.shutdown();
        }
        System.out.println(new TreeMap<>(results));
    }

    private static List<Long> splitByChunks(FileChannel channel, long maxChunkSize) throws IOException {
        long size = channel.size();
        List<Long> result = new ArrayList<>();
        long current = 0;
        result.add(current);
        while (current < size) {
            var mbb = channel.map(FileChannel.MapMode.READ_ONLY, current, Math.min(size - current, maxChunkSize));
            int position = mbb.limit() - 1;
            while (mbb.get(position) != LINE_SEPARATOR) {
                position--;
            }
            current += position + 1;
            result.add(current);
        }
        return result;
    }

    private static Map<String, MeasurementAggregation> calcForChunk(FileChannel channel, long begin, long end) throws IOException {
        var results = new HashMap<CityWrapper, MeasurementAggregation>(10_000);
        var mbb = channel.map(FileChannel.MapMode.READ_ONLY, begin, end - begin);
        int cityOffset, hashCode, temperatureOffset, temperature;
        byte b;

        while (mbb.hasRemaining()) {
            cityOffset = mbb.position();
            hashCode = 0;
            while ((b = mbb.get()) != DELIMITER) {
                hashCode = 31 * hashCode + b;
            }

            temperatureOffset = mbb.position();
            CityWrapper city = new CityWrapper(mbb, cityOffset, temperatureOffset - cityOffset - 1, hashCode);

            temperature = 0;
            while ((b = mbb.get()) != LINE_SEPARATOR) {
                if (b >= ZERO && b <= NINE) {
                    temperature = temperature * 10 + (b - ZERO);
                }
            }
            if (mbb.get(temperatureOffset) == MINUS) {
                temperature *= -1;
            }

            var result = results.get(city);
            if (result != null) {
                result.addTemperature(temperature);
            }
            else {
                results.put(city, new MeasurementAggregation().addTemperature(temperature));
            }
        }
        return results.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }

    private static final class MeasurementAggregation {
        private long sum;
        private int count;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;

        public MeasurementAggregation addTemperature(int temperature) {
            sum += temperature;
            count++;
            min = Math.min(temperature, min);
            max = Math.max(temperature, max);
            return this;
        }

        public void merge(MeasurementAggregation o) {
            if (o == null) {
                return;
            }
            sum += o.sum;
            count += o.count;
            min = Math.min(min, o.min);
            max = Math.max(max, o.max);
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10d, sum / 10d / count, max / 10d);
        }
    }

    private record CityWrapper(MappedByteBuffer mbb, int begin, int size, int hash) {

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CityWrapper that = (CityWrapper) o;
            if (hash != that.hash || size != that.size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (mbb.get(begin + i) != mbb.get(that.begin + i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            byte[] arr = new byte[size];
            for (int i = 0; i < size; i++) {
                arr[i] = mbb.get(begin + i);
            }
            return new String(arr);
        }
    }
}
