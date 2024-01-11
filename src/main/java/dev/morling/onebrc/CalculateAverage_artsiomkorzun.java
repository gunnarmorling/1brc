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
    private static final int SEGMENT_SIZE = 32 * 1024 * 1024;
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

    private record Aggregate(int min, int max, long sum, int cnt) {
    }

    private static class Aggregates {

        private static final int ENTRIES = 64 * 1024;
        private static final int SIZE = 32 * ENTRIES;

        private final long pointer;

        public Aggregates() {
            long address = UNSAFE.allocateMemory(SIZE + 8096);
            pointer = (address + 4095) & (~4095);
            UNSAFE.setMemory(pointer, SIZE, (byte) 0);
        }

        public void add(long reference, int length, int hash, int value) {
            for (int offset = offset(hash);; offset = next(offset)) {
                long address = pointer + offset;
                long ref = UNSAFE.getLong(address);

                if (ref == 0) {
                    alloc(reference, length, hash, value, address);
                    break;
                }

                if (equal(ref, reference, length)) {
                    long sum = UNSAFE.getLong(address + 16) + value;
                    int cnt = UNSAFE.getInt(address + 24) + 1;
                    short min = (short) Math.min(UNSAFE.getShort(address + 28), value);
                    short max = (short) Math.max(UNSAFE.getShort(address + 30), value);

                    UNSAFE.putLong(address + 16, sum);
                    UNSAFE.putInt(address + 24, cnt);
                    UNSAFE.putShort(address + 28, min);
                    UNSAFE.putShort(address + 30, max);
                    break;
                }
            }
        }

        public void merge(Aggregates rights) {
            for (int rightOffset = 0; rightOffset < SIZE; rightOffset += 32) {
                long rightAddress = rights.pointer + rightOffset;
                long reference = UNSAFE.getLong(rightAddress);

                if (reference == 0) {
                    continue;
                }

                int hash = UNSAFE.getInt(rightAddress + 8);
                int length = UNSAFE.getInt(rightAddress + 12);

                for (int offset = offset(hash);; offset = next(offset)) {
                    long address = pointer + offset;
                    long ref = UNSAFE.getLong(address);

                    if (ref == 0) {
                        UNSAFE.copyMemory(rightAddress, address, 32);
                        break;
                    }

                    if (equal(ref, reference, length)) {
                        long sum = UNSAFE.getLong(address + 16) + UNSAFE.getLong(rightAddress + 16);
                        int cnt = UNSAFE.getInt(address + 24) + UNSAFE.getInt(rightAddress + 24);
                        short min = (short) Math.min(UNSAFE.getShort(address + 28), UNSAFE.getShort(rightAddress + 28));
                        short max = (short) Math.max(UNSAFE.getShort(address + 30), UNSAFE.getShort(rightAddress + 30));

                        UNSAFE.putLong(address + 16, sum);
                        UNSAFE.putInt(address + 24, cnt);
                        UNSAFE.putShort(address + 28, min);
                        UNSAFE.putShort(address + 30, max);
                        break;
                    }
                }
            }
        }

        public Map<String, Aggregate> aggregate() {
            TreeMap<String, Aggregate> set = new TreeMap<>();

            for (int offset = 0; offset < SIZE; offset += 32) {
                long address = pointer + offset;
                long ref = UNSAFE.getLong(address);

                if (ref != 0) {
                    int length = UNSAFE.getInt(address + 12) - 1;
                    byte[] array = new byte[length];
                    UNSAFE.copyMemory(null, ref, array, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
                    String key = new String(array);

                    long sum = UNSAFE.getLong(address + 16);
                    int cnt = UNSAFE.getInt(address + 24);
                    short min = UNSAFE.getShort(address + 28);
                    short max = UNSAFE.getShort(address + 30);

                    Aggregate aggregate = new Aggregate(min, max, sum, cnt);
                    set.put(key, aggregate);
                }
            }

            return set;
        }

        private static void alloc(long reference, int length, int hash, int value, long address) {
            UNSAFE.putLong(address, reference);
            UNSAFE.putInt(address + 8, hash);
            UNSAFE.putInt(address + 12, length);
            UNSAFE.putLong(address + 16, value);
            UNSAFE.putInt(address + 24, 1);
            UNSAFE.putShort(address + 28, (short) value);
            UNSAFE.putShort(address + 30, (short) value);
        }

        private static int offset(int hash) {
            return ((hash) & (ENTRIES - 1)) << 5;
        }

        private static int next(int prev) {
            return (prev + 32) & (SIZE - 1);
        }

        private static boolean equal(long leftAddress, long rightAddress, int length) {
            while (length > 8) {
                long left = UNSAFE.getLong(leftAddress);
                long right = UNSAFE.getLong(rightAddress);

                if (left != right) {
                    return false;
                }

                leftAddress += 8;
                rightAddress += 8;
                length -= 8;
            }

            int shift = (8 - length) << 3;
            long left = getLongLittleEndian(leftAddress) << shift;
            long right = getLongLittleEndian(rightAddress) << shift;
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

            for (int segment; (segment = counter.getAndIncrement()) < SEGMENT_COUNT;) {
                long position = (long) SEGMENT_SIZE * segment;
                int size = (int) Math.min(SEGMENT_SIZE + SEGMENT_OVERLAP, MAPPED_FILE.byteSize() - position);
                long address = MAPPED_FILE.address() + position;
                long limit = address + Math.min(SEGMENT_SIZE, size - 1);

                if (segment > 0) {
                    address = next(address);
                }

                aggregate(aggregates, address, limit);
            }

            while (!result.compareAndSet(null, aggregates)) {
                Aggregates rights = result.getAndSet(null);

                if (rights != null) {
                    aggregates.merge(rights);
                }
            }
        }

        private static void aggregate(Aggregates aggregates, long position, long limit) {
            // this parsing can produce seg fault at page boundaries
            // e.g. file size is 4096 and the last entry is X=0.0, which is less than 8 bytes
            // as a result a read will be split across pages, where one of them is not mapped
            // but for some reason it works on my machine, leaving to investigate

            for (long start = position, hash = 0; position <= limit;) {
                int length; // idea: royvanrijn, explanation: https://richardstartin.github.io/posts/finding-bytes
                {
                    long word = getLongLittleEndian(position);
                    long match = word ^ COMMA_PATTERN;
                    long mask = (match - 0x0101010101010101L) & ~match & 0x8080808080808080L;

                    if (mask == 0) {
                        hash ^= word;
                        position += 8;
                        continue;
                    }

                    int bit = Long.numberOfTrailingZeros(mask);
                    position += (bit >>> 3) + 1; // +sep
                    hash ^= (word << (69 - bit));
                    length = (int) (position - start);
                }

                int value; // idea: merykitty
                {
                    long word = getLongLittleEndian(position);
                    long inverted = ~word;
                    int dot = Long.numberOfTrailingZeros(inverted & DOT_BITS);
                    long signed = (inverted << 59) >> 63;
                    long mask = ~(signed & 0xFF);
                    long digits = ((word & mask) << (28 - dot)) & 0x0F000F0F00L;
                    long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
                    value = (int) ((abs ^ signed) - signed);
                    position += (dot >> 3) + 3;
                }

                aggregates.add(start, length, mix(hash), value);

                start = position;
                hash = 0;
            }
        }

        private static long next(long position) {
            while (UNSAFE.getByte(position++) != '\n') {
                // continue
            }
            return position;
        }

        private static int mix(long x) {
            long h = x * -7046029254386353131L;
            h ^= h >>> 32;
            return (int) (h ^ h >>> 16);
        }
    }
}
