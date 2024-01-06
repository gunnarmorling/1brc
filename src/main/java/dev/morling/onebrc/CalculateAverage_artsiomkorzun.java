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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class CalculateAverage_artsiomkorzun {

    private static final Path FILE = Path.of("./measurements.txt");
    private static final long FILE_SIZE = size(FILE);

    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static final int SEGMENT_SIZE = 16 * 1024 * 1024;
    private static final int SEGMENT_COUNT = (int) ((FILE_SIZE + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
    private static final int SEGMENT_OVERLAP = 1024;

    public static void main(String[] args) throws Exception {
        // for (int i = 0; i < 10; i++) {
        // long start = System.currentTimeMillis();
        // execute();
        // long end = System.currentTimeMillis();
        // System.err.println("Time: " + (end - start));
        // }

        execute();
    }

    private static void execute() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Aggregates> result = new AtomicReference<>();
        Aggregator[] aggregators = new Aggregator[PARALLELISM];

        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(counter, result);
            aggregators[i].start();
        }

        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].join();
        }

        Aggregates aggregates = result.get();
        aggregates.sort();

        print(aggregates);
    }

    private static void print(Aggregates aggregates) {
        StringBuilder builder = new StringBuilder(aggregates.size() * 15 + 32);
        builder.append("{");
        aggregates.visit(aggregate -> {
            if (builder.length() > 1) {
                builder.append(", ");
            }

            builder.append(aggregate);
        });
        builder.append("}");
        System.out.println(builder);
    }

    private static long size(Path file) {
        try {
            return Files.size(file);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Row {
        final byte[] station = new byte[256];
        int length;
        int hash;
        int temperature;

        @Override
        public String toString() {
            return new String(station, 0, length) + ":" + temperature;
        }
    }

    private static class Aggregate implements Comparable<Aggregate> {
        final byte[] station;
        final int hash;
        int min;
        int max;
        long sum;
        int count;

        public Aggregate(Row row) {
            this.station = Arrays.copyOf(row.station, row.length);
            this.hash = row.hash;
            this.min = row.temperature;
            this.max = row.temperature;
            this.sum = row.temperature;
            this.count = 1;
        }

        public void add(Row row) {
            min = Math.min(min, row.temperature);
            max = Math.max(max, row.temperature);
            sum += row.temperature;
            count++;
        }

        public void merge(Aggregate right) {
            min = Math.min(min, right.min);
            max = Math.max(max, right.max);
            sum += right.sum;
            count += right.count;
        }

        @Override
        public int compareTo(Aggregate that) {
            byte[] lhs = this.station;
            byte[] rhs = that.station;
            int limit = Math.min(lhs.length, rhs.length);

            for (int offset = 0; offset < limit; offset++) {
                int left = lhs[offset];
                int right = rhs[offset];

                if (left != right) {
                    return (left & 0xFF) - (right & 0xFF);
                }
            }

            return lhs.length - rhs.length;
        }

        @Override
        public String toString() {
            return new String(station) + "=" + round(min) + "/" + round(1.0 * sum / count) + "/" + round(max);
        }

        private static double round(double v) {
            return Math.round(v) / 10.0;
        }
    }

    private static class Aggregates {

        private static final int GROW_FACTOR = 4;
        private static final float LOAD_FACTOR = 0.55f;

        private Aggregate[] aggregates = new Aggregate[1024];
        private int limit = (int) (aggregates.length * LOAD_FACTOR);
        private int size;

        public int size() {
            return size;
        }

        public void visit(Consumer<Aggregate> consumer) {
            if (size > 0) {
                for (Aggregate aggregate : aggregates) {
                    if (aggregate != null) {
                        consumer.accept(aggregate);
                    }
                }
            }
        }

        public void add(Row row) {
            int index = row.hash & (aggregates.length - 1);

            while (true) {
                Aggregate aggregate = aggregates[index];

                if (aggregate == null) {
                    aggregates[index] = new Aggregate(row);
                    if (++size >= limit) {
                        grow();
                    }
                    break;
                }

                if (row.hash == aggregate.hash && Arrays.equals(row.station, 0, row.length, aggregate.station, 0, aggregate.station.length)) {
                    aggregate.add(row);
                    break;
                }

                index = (index + 1) & (aggregates.length - 1);
            }
        }

        public void merge(Aggregate right) {
            int index = right.hash & (aggregates.length - 1);

            while (true) {
                Aggregate aggregate = aggregates[index];

                if (aggregate == null) {
                    aggregates[index] = right;
                    if (++size >= limit) {
                        grow();
                    }
                    break;
                }

                if (right.hash == aggregate.hash && Arrays.equals(right.station, aggregate.station)) {
                    aggregate.merge(right);
                    break;
                }

                index = (index + 1) & (aggregates.length - 1);
            }
        }

        public Aggregates sort() {
            Arrays.sort(aggregates, Comparator.nullsLast(Aggregate::compareTo));
            return this;
        }

        private void grow() {
            Aggregate[] oldAggregates = aggregates;
            aggregates = new Aggregate[oldAggregates.length * GROW_FACTOR];
            limit = (int) (aggregates.length * LOAD_FACTOR);

            for (Aggregate aggregate : oldAggregates) {
                if (aggregate != null) {
                    int index = aggregate.hash & (aggregates.length - 1);

                    while (aggregates[index] != null) {
                        index = (index + 1) & (aggregates.length - 1);
                    }

                    aggregates[index] = aggregate;
                }
            }
        }
    }

    private static class Aggregator extends Thread {

        private final AtomicInteger counter;
        private final AtomicReference<Aggregates> result;

        public Aggregator(AtomicInteger counter, AtomicReference<Aggregates> result) {
            super("aggregator");
            this.counter = counter;
            this.result = result;
        }

        @Override
        public void run() {
            Aggregates aggregates = new Aggregates();
            Row row = new Row();

            try (FileChannel channel = FileChannel.open(FILE, StandardOpenOption.READ)) {
                for (int segment; (segment = counter.getAndIncrement()) < SEGMENT_COUNT;) {
                    aggregate(channel, segment, aggregates, row);
                }
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }

            while (!result.compareAndSet(null, aggregates)) {
                Aggregates rights = result.getAndSet(null);

                if (rights != null) {
                    aggregates = merge(aggregates, rights);
                }
            }
        }

        private static void aggregate(FileChannel channel, int segment, Aggregates aggregates, Row row) throws Exception {
            long position = (long) SEGMENT_SIZE * segment;
            int size = (int) Math.min(SEGMENT_SIZE + SEGMENT_OVERLAP, FILE_SIZE - position);
            int limit = Math.min(SEGMENT_SIZE, size - 1);

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, size);

            if (position > 0) {
                next(buffer);
            }

            for (int offset = buffer.position(); offset <= limit;) {
                offset = parse(buffer, row, offset);
                aggregates.add(row);
            }
        }

        private static Aggregates merge(Aggregates lefts, Aggregates rights) {
            if (rights.size() < lefts.size()) {
                Aggregates temp = lefts;
                lefts = rights;
                rights = temp;
            }

            rights.visit(lefts::merge);
            return lefts;
        }

        private static void next(ByteBuffer buffer) {
            while (buffer.get() != '\n') {
                // continue
            }
        }

        private static int parse(ByteBuffer buffer, Row row, int offset) {
            byte[] station = row.station;
            int length = 0;
            int hash = 0;

            for (byte b; (b = buffer.get(offset++)) != ';';) {
                station[length++] = b;
                hash = 71 * hash + b;
            }

            row.length = length;
            row.hash = hash;

            int sign = 1;

            if (buffer.get(offset) == '-') {
                sign = -1;
                offset++;
            }

            int value = buffer.get(offset++) - '0';

            if (buffer.get(offset) != '.') {
                value = 10 * value + buffer.get(offset++) - '0';
            }

            value = 10 * value + buffer.get(offset + 1) - '0';
            row.temperature = value * sign;
            return offset + 3;
        }
    }
}
