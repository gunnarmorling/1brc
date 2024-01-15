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
 * @author Rafael Merino García
 *
 * <pre>
 *
 *  Model Name: MacBook Pro
 *  Model Identifier: MacBookPro17,1
 *  Chip: Apple M1
 *  Total Number of Cores: 8 (4 performance and 4 efficiency)
 *  Memory: 16 GB
 *
 *  Executed 10 times in my machine with a chunk size of 20MB
 *
 *     21.0.1-graal
 *     avg: 15,366 sg | min: 14,878 sg | max: 15,937 sg | acc: 153,657 sg | times: 10
 *
 *     21-oracle
 *     avg: 17,032 sg | min: 16,448 sg | max: 17,424 sg | acc: 170,325 sg | times: 10
 *
 *
 *
 *  Credits:
 *      . bjhara: Really nice splitearator to be able to use the Stream API.
 *      . ebarlas: working with integers since we only have to consider one decimal
 *        (I don't think this makes a big difference though)
 *      . filiphr: It was my starting point, since it's the most natural way of approaching
 *        the problem using the nice spliterartor from bjhara. This solution has the potential
 *        for substantial improvement by actively pursuing a <br>higher level of parallelization<br>.
 *  </pre>
 *
 *
 * <pre>
 * Generalization Note:
 *
 * - This solution is designed to be applicable to any CSV file under the following assumptions:
 *
 * - The line schema follows the pattern: name;value\n
 *
 * - The name is up to 128 characters (can be changed to hold any other size and irrelevant for the result)
 *
 * - The value is a decimal number with only one decimal digit.
 *
 * - The focus is on maintaining code simplicity without extreme optimization efforts,
 *   as achieving meaningful conclusions often requires substantial time and dedication,
 *   particularly with tools like JMH.
 *
 * - Emphasis on utilizing idiomatic Java and common data structures, following a pragmatic approach.
 *
 * - Addressing the question of whether the workload is CPU-bound or IO-bound is key; indications suggest
 *    both aspects are relevant. It's difficult to make the cores sweat! The observed trend in many solutions
 *    suggests the potential for increased parallelization to fully utilize multiple cores effectively.
 *    This solution brings to the table the Java class ManagedBlock, aiming to enhance parallelism in scenarios
 *    where threads from the Fork Join Pool are blocked.
 *
 *  - Commong guys! stop rolling the dice with fancy optimizations and reiventing hash maps structures and
 *   hash algorithms. This should be <a href="https://dailypapert.com/hard-fun/">hard fun</a>
 *   and not tedious. Dont get me wrong! just an opinion :)
 *
 * - Last but not least, Gunnar Morling, you rock man! Thanks for your time and effort.
 *
 * -
 *
 * </pre>
 */
public class CalculateAverage_imrafaelmerino {

    private static final String FILE = "./measurements.txt";
    private static final int FIELD_SIZE = 128;

    public static void main(String[] args) throws IOException {
        var chunkSize = Long.parseLong(args[0].trim());
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

            return new TreeMap<>(stats);
        }

    }

    private static Map<String, Stat> combine(Map<String, Stat> xs,
                                             Map<String, Stat> ys) {

        Map<String, Stat> result = new HashMap<>();

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

    private static Map<String, Stat> parse(ByteBuffer bb) {
        Map<String, Stat> stats = new HashMap<>();
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
            var fieldStr = new String(field, 0, fieldCurrentIndex);
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
