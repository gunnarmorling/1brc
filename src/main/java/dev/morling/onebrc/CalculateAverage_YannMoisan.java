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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * based on imrafaelmerino
 * ./calculate_average_imrafaelmerino.sh  129.10s user 4.73s system 1395% cpu 9.591 total
 *
 * ./calculate_average_baseline.sh  193.27s user 5.81s system 100% cpu 3:17.85 total
 *
 * addition to copied implementation
 * - use a Location object as a key in the Map to avoid String instantiations.
 * ./calculate_average_YannMoisan.sh  118.36s user 5.72s system 1425% cpu 8.705 total
 *
 *  Model Name: MacBook Pro
 *  Chip: Intel Core i9
 *  Total Number of Cores: 8
 *  Memory: 64 GB
 *  */
public class CalculateAverage_YannMoisan {

    private static final String FILE = "./measurements.txt";
    private static final int FIELD_SIZE = 128;

    public static void main(String[] args) throws IOException {
        var chunkSize = 1024 * 1024 * 50L; // Long.parseLong(args[0].trim());
        var result = calculateStats(FILE, chunkSize);
        System.out.println(result);
    }

    private static Map<String, Stat> calculateStats(String file,
                                                    long chunkSize)
            throws IOException {

        try (var fileChannel = FileChannel.open(Paths.get(file),
                StandardOpenOption.READ)) {
            var stats = fileMemoryStream(fileChannel, chunkSize)
                    .parallel()
                    .map(p -> ManagedComputation.compute(() -> parse(p)))
                    .reduce(Collections.emptyMap(),
                            (stat1, stat2) -> combine(stat1, stat2));

            var tm = new TreeMap<String, Stat>();
            stats.forEach((k, v) -> tm.put(new String(k.value, 0, k.value.length), v));
            return tm;
        }

    }

    private static Map<Location, Stat> combine(Map<Location, Stat> xs,
                                               Map<Location, Stat> ys) {

        Map<Location, Stat> result = new HashMap<>();

        for (var key : xs.keySet()) {
            var m1 = xs.get(key);
            var m2 = ys.get(key);
            var combined = (m2 == null) ? m1 : (m1 == null) ? m2 : Stat.combine(m1, m2);
            result.put(key, combined);
        }

        for (var key : ys.keySet())
            result.putIfAbsent(key, ys.get(key));
        return result;

    }

    private static Map<Location, Stat> parse(ByteBuffer bb) {
        Map<Location, Stat> stats = new HashMap<>();
        var limit = bb.limit();
        var field = new byte[FIELD_SIZE];
        while (bb.position() < limit) {
            var fieldCurrentIndex = 0;
            field[fieldCurrentIndex++] = bb.get();
            while (bb.position() < limit) {
                var fieldByte = bb.get();
                if (fieldByte == ';')
                    break;
                field[fieldCurrentIndex++] = fieldByte;
            }
            var dst = new byte[fieldCurrentIndex];
            System.arraycopy(field, 0, dst, 0, fieldCurrentIndex);
            var fieldStr = new Location(dst);
            // System.arraycopy(field, 0, dst, 0, fieldCurrentIndex);
            var number = 0;
            var sign = 1;
            while (bb.position() < limit) {
                var numberByte = bb.get();
                if (numberByte == '-')
                    sign = -1;
                else if (numberByte == '\n')
                    break;
                else if (numberByte != '.')
                    number = number * 10 + (numberByte - '0');
            }
            stats.computeIfAbsent(fieldStr,
                    k -> new Stat())
                    .update(sign * number);
        }

        return stats;
    }

    private static Stream<ByteBuffer> fileMemoryStream(FileChannel fileChannel,
                                                       long chunkSize)
            throws IOException {

        var spliterator = Spliterators.spliteratorUnknownSize(fileMemoryIterator(fileChannel,
                chunkSize),
                Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator,
                false);
    }

    private static Iterator<ByteBuffer> fileMemoryIterator(FileChannel fileChannel, long chunkSize) throws IOException {
        return new Iterator<>() {

            private final long size = fileChannel.size();
            private long start = 0;

            @Override
            public boolean hasNext() {
                return start < size;
            }

            @Override
            public ByteBuffer next() {
                try {
                    var buffer = fileChannel.map(MapMode.READ_ONLY,
                            start,
                            Math.min(chunkSize,
                                    size - start));
                    var limmit = buffer.limit() - 1;
                    while (buffer.get(limmit) != '\n')
                        limmit--;
                    limmit++;
                    buffer.limit(limmit);
                    start += limmit;
                    return buffer;
                }
                catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        };
    }

    private static final class Location {
        public final byte[] value;

        public Location(byte[] value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Location location = (Location) o;
            return Arrays.equals(value, location.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    private static final class Stat {

        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0L;
        private long count = 0L;

        public static Stat combine(Stat m1,
                                   Stat m2) {
            var stat = new Stat();
            stat.min = Math.min(m1.min, m2.min);
            stat.max = Math.max(m1.max, m2.max);
            stat.sum = m1.sum + m2.sum;
            stat.count = m1.count + m2.count;
            return stat;
        }

        private void update(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        @Override
        public String toString() {
            return round(min / 10.0) + "/" + round((sum / 10.0) / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static final class ManagedComputation {
        static <T> T compute(final Supplier<T> supplier) {
            var managedBlocker = new ManagedSupplier<>(supplier);
            try {
                ForkJoinPool.managedBlock(managedBlocker);
                return managedBlocker.getResult();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

        }

        private static class ManagedSupplier<T> implements ForkJoinPool.ManagedBlocker {
            private final Supplier<T> task;
            private T result;
            private boolean isDone = false;

            private ManagedSupplier(final Supplier<T> supplier) {
                task = supplier;
            }

            @Override
            public boolean block() {
                result = task.get();
                isDone = true;
                return true;
            }

            @Override
            public boolean isReleasable() {
                return isDone;
            }

            T getResult() {
                return result;
            }
        }

    }
}
