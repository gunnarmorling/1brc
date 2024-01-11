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

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CalculateAverage_artsiomkorzun {

    private static final Path FILE = Path.of("./measurements.txt");
    private static final MemorySegment MAPPED_FILE = map(FILE);

    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static final int SEGMENT_SIZE = 16 * 1024 * 1024;
    private static final int SEGMENT_COUNT = (int) ((MAPPED_FILE.byteSize() + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
    private static final int SEGMENT_OVERLAP = 1024;
    private static final long COMMA_PATTERN = pattern(';');
    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    private static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();
    private static final Unsafe UNSAFE;

    static {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            UNSAFE = (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

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

        Map<String, Aggregate> aggregates = result.get().aggregate();
        System.out.println(text(aggregates));
    }

    private static MemorySegment map(Path file) {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = channel.size();
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, size, Arena.global());
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static long pattern(char c) {
        long b = c & 0xFFL;
        return b | (b << 8) | (b << 16) | (b << 24) | (b << 32) | (b << 40) | (b << 48) | (b << 56);
    }

    private static long getLongBigEndian(long address) {
        long value = UNSAFE.getLong(address);

        if (BYTE_ORDER == ByteOrder.LITTLE_ENDIAN) {
            value = Long.reverseBytes(value);
        }

        return value;
    }

    private static long getLongLittleEndian(long address) {
        long value = UNSAFE.getLong(address);

        if (BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
            value = Long.reverseBytes(value);
        }

        return value;
    }

    private static String text(Map<String, Aggregate> aggregates) {
        StringBuilder text = new StringBuilder(aggregates.size() * 32 + 2);
        text.append('{');

        for (Map.Entry<String, Aggregate> entry : aggregates.entrySet()) {
            if (text.length() > 1) {
                text.append(", ");
            }

            Aggregate aggregate = entry.getValue();
            text.append(entry.getKey()).append('=')
                    .append(round(aggregate.min)).append('/')
                    .append(round(1.0 * aggregate.sum / aggregate.cnt)).append('/')
                    .append(round(aggregate.max));
        }

        text.append('}');
        return text.toString();
    }

    private static double round(double v) {
        return Math.round(v) / 10.0;
    }

    private static class Row {
        long address;
        int length;
        int hash;
        int value;
    }

    private record Aggregate(int min, int max, long sum, int cnt) {
    }

    private static class Aggregates {

        private static final int SIZE = 16 * 1024;
        private final long pointer;

        public Aggregates() {
            int size = 32 * SIZE;
            long address = UNSAFE.allocateMemory(size + 8096);
            pointer = (address + 4095) & (~4095);
            UNSAFE.setMemory(pointer, size, (byte) 0);

            long word = pack(Short.MAX_VALUE, Short.MIN_VALUE, 0);
            for (int i = 0; i < SIZE; i++) {
                long entry = pointer + 32 * i;
                UNSAFE.putLong(entry + 24, word);
            }
        }

        public void add(Row row) {
            long index = index(row.hash);
            long header = ((long) row.hash << 32) | (row.length);

            while (true) {
                long address = pointer + (index << 5);
                long head = UNSAFE.getLong(address);
                long ref = UNSAFE.getLong(address + 8);
                boolean isHit = (head == 0) || (head == header && equal(ref, row.address, row.length));

                if (isHit) {
                    long sum = UNSAFE.getLong(address + 16) + row.value;
                    long word = UNSAFE.getLong(address + 24);
                    int min = Math.min(min(word), row.value);
                    int max = Math.max(max(word), row.value);
                    int cnt = cnt(word) + 1;

                    UNSAFE.putLong(address, header);
                    UNSAFE.putLong(address + 8, row.address);
                    UNSAFE.putLong(address + 16, sum);
                    UNSAFE.putLong(address + 24, pack(min, max, cnt));
                    break;
                }

                index = (index + 1) & (SIZE - 1);
            }
        }

        public void merge(Aggregates rights) {
            for (int rightIndex = 0; rightIndex < SIZE; rightIndex++) {
                long rightAddress = rights.pointer + (rightIndex << 5);
                long header = UNSAFE.getLong(rightAddress);
                long reference = UNSAFE.getLong(rightAddress + 8);

                if (header == 0) {
                    continue;
                }

                int hash = (int) (header >>> 32);
                int length = (int) (header);
                long index = index(hash);

                while (true) {
                    long address = pointer + (index << 5);
                    long head = UNSAFE.getLong(address);
                    long ref = UNSAFE.getLong(address + 8);
                    boolean isHit = (head == 0) || (head == header && equal(ref, reference, length));

                    if (isHit) {
                        long sum = UNSAFE.getLong(address + 16) + UNSAFE.getLong(rightAddress + 16);
                        long left = UNSAFE.getLong(address + 24);
                        long right = UNSAFE.getLong(rightAddress + 24);
                        int min = Math.min(min(left), min(right));
                        int max = Math.max(max(left), max(right));
                        int cnt = cnt(left) + cnt(right);

                        UNSAFE.putLong(address, header);
                        UNSAFE.putLong(address + 8, reference);
                        UNSAFE.putLong(address + 16, sum);
                        UNSAFE.putLong(address + 24, pack(min, max, cnt));
                        break;
                    }

                    index = (index + 1) & (SIZE - 1);
                }
            }
        }

        public Map<String, Aggregate> aggregate() {
            TreeMap<String, Aggregate> set = new TreeMap<>();

            for (int index = 0; index < SIZE; index++) {
                long address = pointer + (index << 5);
                long head = UNSAFE.getLong(address);
                long ref = UNSAFE.getLong(address + 8);

                if (head == 0) {
                    continue;
                }

                int length = (int) (head);
                byte[] array = new byte[length];
                UNSAFE.copyMemory(null, ref, array, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
                String key = new String(array);

                long sum = UNSAFE.getLong(address + 16);
                long word = UNSAFE.getLong(address + 24);

                Aggregate aggregate = new Aggregate(min(word), max(word), sum, cnt(word));
                set.put(key, aggregate);
            }

            return set;
        }

        private static long pack(int min, int max, int cnt) {
            return ((long) min << 48) | (((long) max & 0xFFFF) << 32) | cnt;
        }

        private static int cnt(long word) {
            return (int) word;
        }

        private static int max(long word) {
            return (short) (word >>> 32);
        }

        private static int min(long word) {
            return (short) (word >>> 48);
        }

        private static long index(int hash) {
            return (hash ^ (hash >> 16)) & (SIZE - 1);
        }

        private static boolean equal(long leftAddress, long rightAddress, int length) {
            int index = 0;

            while (length > 8) {
                long left = UNSAFE.getLong(leftAddress + index);
                long right = UNSAFE.getLong(rightAddress + index);

                if (left != right) {
                    return false;
                }

                length -= 8;
                index += 8;
            }

            int shift = 64 - (length << 3);
            long left = getLongBigEndian(leftAddress + index) >>> shift;
            long right = getLongBigEndian(rightAddress + index) >>> shift;
            return (left == right);
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

            for (int segment; (segment = counter.getAndIncrement()) < SEGMENT_COUNT;) {
                aggregate(aggregates, row, segment);
            }

            while (!result.compareAndSet(null, aggregates)) {
                Aggregates rights = result.getAndSet(null);

                if (rights != null) {
                    aggregates.merge(rights);
                }
            }
        }

        private static void aggregate(Aggregates aggregates, Row row, int segment) {
            long position = (long) SEGMENT_SIZE * segment;
            int size = (int) Math.min(SEGMENT_SIZE + SEGMENT_OVERLAP, MAPPED_FILE.byteSize() - position);
            long address = MAPPED_FILE.address() + position;
            long limit = address + Math.min(SEGMENT_SIZE, size - 1);

            if (segment > 0) {
                address = next(address);
            }

            while (address <= limit) {
                // this parsing can produce seg fault at page boundaries
                // e.g. file size is 4096 and the last entry is X=0.0, which is less than 8 bytes
                // as a result a read will be split across pages, where one of them is not mapped
                // but for some reason it works on my machine, leaving to investigate
                address = parseKey(address, row);
                address = parseValue(address, row);
                aggregates.add(row);
            }
        }

        private static long next(long address) {
            while (UNSAFE.getByte(address++) != '\n') {
                // continue
            }
            return address;
        }

        // idea: royvanrijn
        // explanation: https://richardstartin.github.io/posts/finding-bytes
        private static long parseKey(long address, Row row) {
            int length = 0;
            long hash = 0;
            long word;

            while (true) {
                word = getLongLittleEndian(address + length);
                long match = word ^ COMMA_PATTERN;
                long mask = ((match - 0x0101010101010101L) & ~match) & 0x8080808080808080L;

                if (mask == 0) {
                    hash = 71 * hash + word;
                    length += 8;
                    continue;
                }

                int bit = Long.numberOfTrailingZeros(mask);
                length += (bit >>> 3);
                hash = 71 * hash + (word & (0x00FFFFFFFFFFFFFFL >>> (63 - bit)));

                row.address = address;
                row.length = length;
                row.hash = Long.hashCode(hash);

                return address + length + 1;
            }
        }

        // idea: merykitty
        private static long parseValue(long address, Row row) {
            long word = getLongLittleEndian(address);
            long inverted = ~word;
            int dot = Long.numberOfTrailingZeros(inverted & DOT_BITS);
            long signed = (inverted << 59) >> 63;
            long mask = ~(signed & 0xFF);
            long digits = ((word & mask) << (28 - dot)) & 0x0F000F0F00L;
            long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            row.value = (int) ((abs ^ signed) - signed);
            return address + (dot >> 3) + 3;
        }
    }
}
