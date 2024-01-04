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

import org.radughiorma.Arguments;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.IntSupplier;

public class CalculateAverage_richardstartin {

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static String bufferToString(ByteBuffer slice) {
        byte[] bytes = new byte[slice.limit()];
        slice.get(0, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    interface IndexedStringConsumer {
        void accept(String value, int index);
    }

    /** Maps text to an integer encoding. Adapted from async-profiler. */
    public static class Dictionary {

        private static final int ROW_BITS = 7;
        private static final int ROWS = (1 << ROW_BITS);
        private static final int CELLS = 3;
        private static final int TABLE_CAPACITY = (ROWS * CELLS);

        private final Table table = new Table(nextBaseIndex());

        private static final AtomicIntegerFieldUpdater<Dictionary> BASE_INDEX_UPDATER = AtomicIntegerFieldUpdater.newUpdater(Dictionary.class, "baseIndex");
        volatile int baseIndex;

        private void forEach(Table table, IndexedStringConsumer consumer) {
            for (int i = 0; i < ROWS; i++) {
                Row row = table.rows[i];
                for (int j = 0; j < CELLS; j++) {
                    var slice = row.keys.get(j);
                    if (slice != null) {
                        consumer.accept(bufferToString(slice), table.index(i, j));
                    }
                }
                if (row.next != null) {
                    forEach(row.next, consumer);
                }
            }
        }

        public void forEach(IndexedStringConsumer consumer) {
            forEach(this.table, consumer);
        }

        public int encode(long hash, ByteBuffer slice) {
            Table table = this.table;
            while (true) {
                int rowIndex = (int) (Math.abs(hash) % ROWS);
                Row row = table.rows[rowIndex];
                for (int c = 0; c < CELLS; c++) {
                    ByteBuffer storedKey = row.keys.get(c);
                    if (storedKey == null) {
                        if (row.keys.compareAndSet(c, null, slice)) {
                            return table.index(rowIndex, c);
                        }
                        else {
                            storedKey = row.keys.get(c);
                            if (slice.equals(storedKey)) {
                                return table.index(rowIndex, c);
                            }
                        }
                    }
                    else if (slice.equals(storedKey)) {
                        return table.index(rowIndex, c);
                    }
                }
                table = row.getOrCreateNextTable(this::nextBaseIndex);
                hash = Long.rotateRight(hash, ROW_BITS);
            }
        }

        private int nextBaseIndex() {
            return BASE_INDEX_UPDATER.addAndGet(this, TABLE_CAPACITY);
        }

        private static final class Row {

            private static final AtomicReferenceFieldUpdater<Row, Table> NEXT_TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(Row.class, Table.class, "next");
            private final AtomicReferenceArray<ByteBuffer> keys = new AtomicReferenceArray<>(CELLS);
            volatile Table next;

            public Table getOrCreateNextTable(IntSupplier baseIndexSupplier) {
                Table next = this.next;
                if (next == null) {
                    Table newTable = new Table(baseIndexSupplier.getAsInt());
                    if (NEXT_TABLE_UPDATER.compareAndSet(this, null, newTable)) {
                        next = newTable;
                    }
                    else {
                        next = this.next;
                    }
                }
                return next;
            }
        }

        private static final class Table {

            final Row[] rows;
            final int baseIndex;

            private Table(int baseIndex) {
                this.baseIndex = baseIndex;
                this.rows = new Row[ROWS];
                Arrays.setAll(rows, i -> new Row());
            }

            int index(int row, int col) {
                return baseIndex + (col << ROW_BITS) + row;
            }
        }
    }

    private static long compilePattern(long repeat) {
        return 0x101010101010101L * repeat;
    }

    private static long compilePattern(char delimiter) {
        return compilePattern(delimiter & 0xFFL);
    }

    private static long compilePattern(byte delimiter) {
        return compilePattern(delimiter & 0xFFL);
    }

    private static final long NEW_LINE = compilePattern((byte) '\n');
    private static final long DELIMITER = compilePattern(';');

    private static int firstInstance(long word, long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
        return Long.numberOfTrailingZeros(tmp) >>> 3;
    }

    private static int findLastNewLine(ByteBuffer buffer) {
        return findLastNewLine(buffer, buffer.limit() - 1);
    }

    private static int findLastNewLine(ByteBuffer buffer, int offset) {
        for (int i = offset; i >= 0; i--) {
            if (buffer.get(i) == '\n') {
                return i;
            }
        }
        return 0;
    }

    private static int findIndexOf(ByteBuffer buffer, int offset, long pattern) {
        int i = offset;
        for (; i + Long.BYTES < buffer.limit(); i += Long.BYTES) {
            int index = firstInstance(buffer.getLong(i), pattern);
            if (index != Long.BYTES) {
                return i + index;
            }
        }
        byte b = (byte) (pattern & 0xFF);
        for (; i < buffer.limit(); i++) {
            if (buffer.get(i) == b) {
                return i;
            }
        }
        return buffer.limit();
    }

    private static long hash(ByteBuffer slice) {
        long hash = slice.limit() + PRIME_5 + 0x123456789abcdef1L;
        int i = 0;
        for (; i + Long.BYTES < slice.limit(); i += Long.BYTES) {
            hash = hashLong(hash, slice.getLong(i));
        }
        long part = 0L;
        for (; i < slice.limit(); i++) {
            part = (part >>> 8) | ((slice.get(i) & 0xFFL) << 56);
        }
        hash = hashLong(hash, part);
        return mix(hash);
    }

    static final long PRIME_1 = 0x9E3779B185EBCA87L;
    static final long PRIME_2 = 0xC2B2AE3D27D4EB4FL;
    static final long PRIME_3 = 0x165667B19E3779F9L;
    static final long PRIME_4 = 0x85EBCA77C2B2AE63L;
    static final long PRIME_5 = 0x27D4EB2F165667C5L;

    private static long hashLong(long hash, long k) {
        k *= PRIME_2;
        k = Long.rotateLeft(k, 31);
        k *= PRIME_1;
        hash ^= k;
        return Long.rotateLeft(hash, 27) * PRIME_1 + PRIME_4;
    }

    private static long mix(long hash) {
        hash ^= hash >>> 33;
        hash *= PRIME_2;
        hash ^= hash >>> 29;
        hash *= PRIME_3;
        hash ^= hash >>> 32;
        return hash;
    }

    static class Page {

        static final int PAGE_SIZE = 1024;
        static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
        static final int PAGE_MASK = PAGE_SIZE - 1;
        private static final double[] PAGE_PROTOTYPE = new double[PAGE_SIZE * 4];

        static {
            for (int i = 0; i < PAGE_SIZE * 4; i += 4) {
                PAGE_PROTOTYPE[i + 1] = Double.POSITIVE_INFINITY;
                PAGE_PROTOTYPE[i + 2] = Double.NEGATIVE_INFINITY;
            }
        }

        private static double[] newPage() {
            return Arrays.copyOf(PAGE_PROTOTYPE, PAGE_PROTOTYPE.length);
        }

        static void update(double[][] pages, int position, double value) {
            // find the page
            int pageIndex = position >>> PAGE_SHIFT;
            double[] page = pages[pageIndex];
            if (page == null) {
                pages[pageIndex] = page = newPage();
            }

            // update local aggregates
            page[(position & PAGE_MASK) * 4]++; // count
            page[(position & PAGE_MASK) * 4 + 1] = Math.min(page[(position & PAGE_MASK) * 4 + 1], value); // min
            page[(position & PAGE_MASK) * 4 + 2] = Math.max(page[(position & PAGE_MASK) * 4 + 2], value); // min
            page[(position & PAGE_MASK) * 4 + 3] += value; // sum
        }

        static ResultRow toResultRow(double[][] pages, int position) {
            double[] page = pages[position >>> PAGE_SHIFT];
            double count = page[(position & PAGE_MASK) * 4];
            double min = page[(position & PAGE_MASK) * 4 + 1];
            double max = page[(position & PAGE_MASK) * 4 + 2];
            double sum = page[(position & PAGE_MASK) * 4 + 3];
            return new ResultRow(min, sum / count, max);
        }
    }

    private static class AggregationTask extends RecursiveTask<double[][]> {

        private final Dictionary dictionary;
        private final List<ByteBuffer> slices;
        private final int min;
        private final int max;

        private AggregationTask(Dictionary dictionary, List<ByteBuffer> slices) {
            this(dictionary, slices, 0, slices.size() - 1);
        }

        private AggregationTask(Dictionary dictionary, List<ByteBuffer> slices, int min, int max) {
            this.dictionary = dictionary;
            this.slices = slices;
            this.min = min;
            this.max = max;
        }

        private void computeSlice(ByteBuffer slice, double[][] pages) {
            for (int offset = 0; offset < slice.limit();) {
                int nextSeparator = findIndexOf(slice, offset, DELIMITER);
                ByteBuffer key = slice.slice(offset, nextSeparator - offset).order(ByteOrder.LITTLE_ENDIAN);
                // find the global dictionary code to aggregate,
                // making this code global allows easy merging
                int dictId = dictionary.encode(hash(key), key);

                offset = nextSeparator + 1;
                int newLine = findIndexOf(slice, offset, NEW_LINE);
                // parse the double
                // todo do this without allocating a string, could use a fast parsing falgorithm
                var bytes = new byte[newLine - offset];
                slice.get(offset, bytes);
                var string = new String(bytes, StandardCharsets.US_ASCII);
                double d = Double.parseDouble(string);

                Page.update(pages, dictId, d);

                offset = newLine + 1;
            }
        }

        private static void merge(double[][] contribution, double[][] aggregate) {
            for (int i = 0; i < contribution.length; i++) {
                if (aggregate[i] == null) {
                    aggregate[i] = contribution[i];
                }
                else if (contribution[i] != null) {
                    double[] to = aggregate[i];
                    double[] from = contribution[i];
                    // todo won't vectorise - consider separating aggregates into distinct regions and apply
                    // loop fission (if this shows up in the profile)
                    for (int j = 0; j < to.length; j += 4) {
                        to[j] += from[j];
                        to[j + 1] = Math.min(to[j + 1], from[j + 1]);
                        to[j + 2] = Math.max(to[j + 2], from[j + 2]);
                        to[j + 3] += from[j + 3];
                    }
                }
            }
        }

        @Override
        protected double[][] compute() {
            if (min == max) {
                // fixme - hardcoded to problem size
                var pages = new double[1024][];
                var slice = slices.get(min);
                computeSlice(slice, pages);
                return pages;
            }
            else {
                int mid = (min + max) / 2;
                var low = new AggregationTask(dictionary, slices, min, mid);
                var high = new AggregationTask(dictionary, slices, mid + 1, max);
                var fork = high.fork();
                var partial = low.compute();
                merge(fork.join(), partial);
                return partial;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int maxChunkSize = 10 << 20; // 10MiB
        try (var raf = new RandomAccessFile(Arguments.measurmentsFilename(args), "r");
                var channel = raf.getChannel()) {
            long size = channel.size();
            // make as few mmap calls as possible subject to the 2GiB limit per buffer
            List<ByteBuffer> rawBuffers = new ArrayList<>();
            for (long offset = 0; offset < size - 1;) {
                long end = Math.min(Integer.MAX_VALUE, size - offset);
                ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, end)
                        .order(ByteOrder.LITTLE_ENDIAN);
                boolean lastSlice = end != Integer.MAX_VALUE;
                int limit = lastSlice
                        ? (int) end
                        : findLastNewLine(buffer);
                rawBuffers.add(buffer.limit(limit));
                offset += limit;
            }

            // now slice them up for parallel processing
            var slices = new ArrayList<ByteBuffer>();
            for (ByteBuffer rawBuffer : rawBuffers) {
                for (int offset = 0; offset < rawBuffer.limit();) {
                    int chunkSize = Math.min(rawBuffer.limit() - offset, maxChunkSize);
                    int target = offset + chunkSize;
                    int limit = target >= rawBuffer.limit()
                            ? rawBuffer.limit()
                            : findLastNewLine(rawBuffer, target);
                    int adjustment = rawBuffer.get(offset) == '\n' ? 1 : 0;
                    var slice = rawBuffer.slice(offset + adjustment, limit - offset - adjustment).order(ByteOrder.LITTLE_ENDIAN);
                    slices.add(slice);
                    offset = limit;
                }
            }

            var fjp = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
            Dictionary dictionary = new Dictionary();
            double[][] aggregates = fjp.submit(new AggregationTask(dictionary, slices)).join();
            var map = new TreeMap<String, ResultRow>();
            dictionary.forEach((key, index) -> map.put(key, Page.toResultRow(aggregates, index)));
            System.out.println(map);
        }
    }
}