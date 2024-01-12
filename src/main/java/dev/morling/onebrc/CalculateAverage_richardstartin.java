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
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

public class CalculateAverage_richardstartin {

    private static final String FILE = "./measurements.txt";

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    record Slot(byte[] key, int[] aggregates) {
        private static final int WIDTH = 8;
        private static int[] newAggregates(int stripes) {
            var aggregates = new int[stripes * WIDTH];
            for (int i = 0; i < aggregates.length; i += WIDTH) {
                aggregates[i] = Integer.MAX_VALUE;
                aggregates[i + 1] = Integer.MIN_VALUE;
            }
            return aggregates;
        }
        Slot(byte[] key, int stripes) {
            this(key, newAggregates(stripes));
        }

        void update(int stripe, int value) {
            int i = stripe * WIDTH;
            aggregates[i] = Math.min(value, aggregates[i]);
            aggregates[i + 1] = Math.max(value, aggregates[i + 1]);
            aggregates[i + 2] += value;
            aggregates[i + 3]++;
        }

        public ResultRow toResultRow() {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            int sum = 0;
            int count = 0;
            for (int i = 0; i < aggregates.length; i += WIDTH) {
                min = Math.min(min, aggregates[i]);
                max = Math.max(max, aggregates[i + 1]);
                sum += aggregates[i + 2];
                count += aggregates[i + 3];
            }
            return new ResultRow(min * 0.1, 0.1 * sum / count, max * 0.1);
        }

        public String toKey() {
            return new String(key, StandardCharsets.UTF_8);
        }
    }

    /** Maps text to an integer encoding. Adapted from async-profiler. */
    public static class Dictionary {

        private static final int ROW_BITS = 11;
        private static final int ROWS = (1 << ROW_BITS);
        private static final int TABLE_CAPACITY = ROWS;

        private final Table table = new Table(this, nextBaseIndex());

        private static final AtomicIntegerFieldUpdater<Dictionary> BASE_INDEX_UPDATER = AtomicIntegerFieldUpdater.newUpdater(Dictionary.class, "baseIndex");
        volatile int baseIndex;

        private void forEach(Table table, Consumer<Slot> consumer) {
            for (var row : table.rows) {
                var slot = row.slot;
                if (slot != null) {
                    consumer.accept(slot);
                }
                if (row.next != null) {
                    forEach(row.next, consumer);
                }
            }
        }

        public void forEach(Consumer<Slot> consumer) {
            forEach(this.table, consumer);
        }

        public Slot lookup(int hash, byte[] key, int length, int stripes) {
            Table table = this.table;
            while (true) {
                int rowIndex = Math.abs(hash) % ROWS;
                Row row = table.rows[rowIndex];
                var storedSlot = row.slot;
                if (storedSlot == null) {
                    Slot slot = new Slot(Arrays.copyOf(key, length), stripes);
                    if (row.compareAndSet(null, slot)) {
                        return slot;
                    }
                    else {
                        storedSlot = row.slot;
                        if (Arrays.equals(key, 0, length, storedSlot.key, 0, storedSlot.key.length)) {
                            return storedSlot;
                        }
                    }
                }
                else if (Arrays.equals(key, 0, length, storedSlot.key, 0, storedSlot.key.length)) {
                    return storedSlot;
                }
                table = row.getOrCreateNextTable();
                hash = Integer.rotateRight(hash, ROW_BITS);
            }
        }

        private int nextBaseIndex() {
            return BASE_INDEX_UPDATER.addAndGet(this, TABLE_CAPACITY);
        }

        private static final class Row {

            private static final AtomicReferenceFieldUpdater<Row, Table> NEXT_TABLE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(Row.class, Table.class, "next");
            private static final AtomicReferenceFieldUpdater<Row, Slot> SLOT_UPDATER = AtomicReferenceFieldUpdater.newUpdater(Row.class, Slot.class, "slot");
            private volatile Slot slot = null;
            private final Dictionary dictionary;
            volatile Table next;

            private Row(Dictionary dictionary) {
                this.dictionary = dictionary;
            }

            public Table getOrCreateNextTable() {
                Table next = this.next;
                if (next == null) {
                    Table newTable = new Table(dictionary, dictionary.nextBaseIndex());
                    if (NEXT_TABLE_UPDATER.compareAndSet(this, null, newTable)) {
                        next = newTable;
                    }
                    else {
                        next = this.next;
                    }
                }
                return next;
            }

            public boolean compareAndSet(Slot expected, Slot newSlot) {
                return SLOT_UPDATER.compareAndSet(this, expected, newSlot);
            }
        }

        private static final class Table {

            final Row[] rows;
            final int baseIndex;

            private Table(Dictionary dictionary, int baseIndex) {
                this.baseIndex = baseIndex;
                this.rows = new Row[ROWS];
                Arrays.setAll(rows, i -> new Row(dictionary));
            }
        }
    }

    private static long compilePattern(long repeat) {
        return 0x101010101010101L * repeat;
    }

    private static long compilePattern(byte delimiter) {
        return compilePattern(delimiter & 0xFFL);
    }

    private static final long DELIMITER = compilePattern((byte) ';');

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

    private static int findIndexOf(ByteBuffer buffer, int limit, int offset, long pattern) {
        int i = offset;
        for (; i < limit - Long.BYTES + 1; i += Long.BYTES) {
            long word = buffer.getLong(i);
            long input = word ^ pattern;
            long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
            tmp |= input | 0x7F7F7F7F7F7F7F7FL;
            if (tmp != -1L) {
                return i + (Long.numberOfTrailingZeros(~tmp) >>> 3);
            }
        }
        byte b = (byte) (pattern & 0xFF);
        for (; i < limit; i++) {
            if (buffer.get(i) == b) {
                return i;
            }
        }
        return buffer.limit();
    }

    private static int hash(byte[] bytes, int limit) {
        int hash = 1;
        for (int i = 0; i < limit; i++) {
            hash += hash * 129 + bytes[i];
        }
        return hash;
    }

    private static class AggregationTask extends RecursiveAction {

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

        private void computeSlice(int stripe) {
            var slice = slices.get(stripe);
            int end = slice.limit();
            byte[] tmp = new byte[128];
            for (int offset = 0; offset < end;) {
                int delimiter = findIndexOf(slice, end, offset, DELIMITER);
                int value = 0;
                int sign = 1;
                byte b;
                int i = delimiter + 1;
                while (i != end && (b = slice.get(i++)) != '\n') {
                    if (b != '.') {
                        if (b == '-') {
                            sign = -1;
                        }
                        else {
                            value = 10 * value + (b - '0');
                        }
                    }
                }
                value *= sign;
                int length = delimiter - offset;
                slice.get(offset, tmp, 0, length);
                dictionary.lookup(hash(tmp, length), tmp, length, slices.size()).update(stripe, value);
                offset = i;
            }
        }

        @Override
        protected void compute() {
            if (min == max) {
                computeSlice(min);
            }
            else {
                int mid = (min + max) / 2;
                var low = new AggregationTask(dictionary, slices, min, mid);
                var high = new AggregationTask(dictionary, slices, mid + 1, max);
                var fork = high.fork();
                low.compute();
                fork.join();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int maxChunkSize = 10 << 20; // 10MiB
        try (var raf = new RandomAccessFile(FILE, "r");
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

            try (var fjp = new ForkJoinPool(Runtime.getRuntime().availableProcessors())) {
                Dictionary dictionary = new Dictionary();
                fjp.submit(new AggregationTask(dictionary, slices)).join();
                var map = new TreeMap<String, ResultRow>();
                dictionary.forEach(slot -> map.put(slot.toKey(), slot.toResultRow()));
                System.out.println(map);
            }
        }
    }
}