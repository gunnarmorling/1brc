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
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class CalculateAverage_artsiomkorzun {

    private static final Path FILE = Path.of("./measurements.txt");
    private static final long FILE_SIZE = size(FILE);

    private static final int SEGMENT_SIZE = 16 * 1024 * 1024;
    private static final int SEGMENT_COUNT = (int) ((FILE_SIZE + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
    private static final int SEGMENT_OVERLAP = 1024;

    public static void main(String[] args) throws Exception {
        /*
         * for (int i = 0; i < 10; i++) {
         * long start = System.currentTimeMillis();
         * execute();
         * long end = System.currentTimeMillis();
         * System.err.println("Time: " + (end - start));
         * }
         */

        execute();
    }

    private static void execute() {
        Aggregates aggregates = IntStream.range(0, SEGMENT_COUNT)
                .parallel()
                .mapToObj(CalculateAverage_artsiomkorzun::aggregate)
                .reduce(new Aggregates(), CalculateAverage_artsiomkorzun::merge)
                .sort();

        print(aggregates);
    }

    private static Aggregates aggregate(int segment) {
        long position = (long) SEGMENT_SIZE * segment;
        int size = (int) Math.min(SEGMENT_SIZE + SEGMENT_OVERLAP, FILE_SIZE - position);
        int limit = Math.min(SEGMENT_SIZE, size - 1);

        MappedByteBuffer buffer = map(position, size); // leaking until gc

        if (position > 0) {
            next(buffer);
        }

        Aggregates aggregates = new Aggregates();
        Row row = new Row();

        while (buffer.position() <= limit) {
            parse(buffer, row);
            aggregates.add(row);
        }

        return aggregates;
    }

    private static Aggregates merge(Aggregates lefts, Aggregates rights) {
        Aggregates to = (lefts.size() < rights.size()) ? rights : lefts;
        Aggregates from = (lefts.size() < rights.size()) ? lefts : rights;
        from.visit(to::merge);
        return to;
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

    private static MappedByteBuffer map(long position, int size) {
        try (FileChannel channel = FileChannel.open(FILE, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, position, size); // leaking until gc
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void next(ByteBuffer buffer) {
        while (buffer.get() != '\n') {
            // continue
        }
    }

    private static void parse(ByteBuffer buffer, Row row) {
        int index = 0;
        byte b;

        while ((b = buffer.get()) != ';') {
            row.station[index++] = b;
        }

        row.length = index;

        double value = 0;
        double multiplier = 1;

        b = buffer.get();
        if (b == '-') {
            multiplier = -1;
        }
        else {
            assert b >= '0' && b <= '9';
            value = b - '0';
        }

        while ((b = buffer.get()) != '.') {
            assert b >= '0' && b <= '9';
            value = 10 * value + (b - '0');
        }

        b = buffer.get();
        assert b >= '0' && b <= '9';
        value = 10 * value + (b - '0');

        b = buffer.get();
        assert b == '\n';

        row.temperature = value * multiplier;
    }

    private static class Row {
        final byte[] station = new byte[256];
        int length;
        double temperature;

        @Override
        public String toString() {
            return new String(station, 0, length) + ":" + temperature;
        }
    }

    private static class Aggregate implements Comparable<Aggregate> {
        final byte[] station;
        double min;
        double max;
        double sum;
        double count;

        public Aggregate(byte[] station, int length, double temperature) {
            this.station = Arrays.copyOf(station, length);
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        public void add(double temperature) {
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
            sum += temperature;
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
            return new String(station) + "=" + round(min) + "/" + round(sum / count) + "/" + round(max);
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
            byte[] station = row.station;
            int length = row.length;
            double temperature = row.temperature;

            int hash = hash(station, length);
            int index = hash & (aggregates.length - 1);

            while (true) {
                Aggregate aggregate = aggregates[index];

                if (aggregate == null) {
                    aggregates[index] = new Aggregate(station, length, temperature);
                    if (++size >= limit) {
                        grow();
                    }
                    break;
                }

                if (equal(station, length, aggregate.station, aggregate.station.length)) {
                    aggregate.add(temperature);
                    break;
                }

                index = (index + 1) & (aggregates.length - 1);
            }
        }

        public void merge(Aggregate right) {
            byte[] station = right.station;

            int hash = hash(station, station.length);
            int index = hash & (aggregates.length - 1);

            while (true) {
                Aggregate aggregate = aggregates[index];

                if (aggregate == null) {
                    aggregates[index] = right;
                    if (++size >= limit) {
                        grow();
                    }
                    break;
                }

                if (equal(station, station.length, aggregate.station, aggregate.station.length)) {
                    aggregate.merge(right);
                    break;
                }

                index = (index + 1) & (aggregates.length - 1);
            }
        }

        public Aggregates sort() {
            Arrays.parallelSort(aggregates, Comparator.nullsLast(Aggregate::compareTo));
            return this;
        }

        private void grow() {
            Aggregate[] oldAggregates = aggregates;
            aggregates = new Aggregate[oldAggregates.length * GROW_FACTOR];
            limit = (int) (aggregates.length * LOAD_FACTOR);

            for (Aggregate aggregate : oldAggregates) {
                if (aggregate != null) {
                    int hash = hash(aggregate.station, aggregate.station.length);
                    int index = hash & (aggregates.length - 1);

                    while (aggregates[index] != null) {
                        index = (index + 1) & (aggregates.length - 1);
                    }

                    aggregates[index] = aggregate;
                }
            }
        }

        private static int hash(byte[] array, int length) {
            int hash = 0;

            for (int i = 0; i < length; i++) {
                hash = 71 * hash + array[i];
            }

            return hash;
        }

        private static boolean equal(byte[] left, int leftLength, byte[] right, int rightLength) {
            if (leftLength != rightLength) {
                return false;
            }

            for (int i = 0; i < leftLength; i++) {
                if (left[i] != right[i]) {
                    return false;
                }
            }

            return true;
        }
    }
}
