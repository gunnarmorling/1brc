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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CalculateAverage_artsiomkorzun {

    private static final Path FILE = Path.of("./measurements.txt");
    private static final int SEGMENT_SIZE = 4 * 1024 * 1024;
    private static final int SEGMENT_OVERLAP = 128;
    private static final long COMMA_PATTERN = 0x3B3B3B3B3B3B3B3BL;
    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

    private static final Unsafe UNSAFE;
    private static final long ADDRESS_OFFSET;

    static {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            UNSAFE = (Unsafe) unsafe.get(Unsafe.class);
            ADDRESS_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
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
        long fileSize = Files.size(FILE);
        int segmentCount = (int) ((fileSize + SEGMENT_SIZE - 1) / SEGMENT_SIZE);

        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Aggregates> result = new AtomicReference<>();

        int parallelism = Runtime.getRuntime().availableProcessors();
        Aggregator[] aggregators = new Aggregator[parallelism];

        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(counter, result, segmentCount);
            aggregators[i].start();
        }

        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].join();
        }

        Map<String, Aggregate> aggregates = result.get().aggregate();
        System.out.println(text(aggregates));
    }

    private static long address(ByteBuffer buffer) {
        return UNSAFE.getLong(buffer, ADDRESS_OFFSET);
    }

    private static ByteBuffer allocate(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size + 4096);
        long address = address(buffer);
        long aligned = (address + 4095) & (~4095);
        int padding = (int) (aligned - address);
        return buffer.position(padding).limit(padding + size).slice();
    }

    private static long word(long address) {
        return UNSAFE.getLong(address);
        /*
         * if (BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
         * value = Long.reverseBytes(value);
         * }
         *
         * return value;
         */
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
        private static final int SIZE = 128 * ENTRIES;

        private final ByteBuffer buffer = allocate(SIZE);
        private final long pointer = address(buffer);

        public long find(long word, int hash) {
            long address = pointer + offset(hash);
            long w = word(address + 24);
            return (w == word) ? address : 0;
        }

        public long find(long word1, long word2, int hash) {
            long address = pointer + offset(hash);
            long w1 = word(address + 24);
            long w2 = word(address + 32);
            return (word1 == w1) && (word2 == w2) ? address : 0;
        }

        public long put(long reference, long word, int length, int hash) {
            for (int offset = offset(hash);; offset = next(offset)) {
                long address = pointer + offset;
                if (equal(reference, word, address + 24, length)) {
                    return address;
                }

                int len = UNSAFE.getInt(address);
                if (len == 0) {
                    alloc(reference, length, hash, address);
                    return address;
                }
            }
        }

        public static void update(long address, int value) {
            long sum = UNSAFE.getLong(address + 8) + value;
            int cnt = UNSAFE.getInt(address + 16) + 1;
            short min = UNSAFE.getShort(address + 20);
            short max = UNSAFE.getShort(address + 22);

            UNSAFE.putLong(address + 8, sum);
            UNSAFE.putInt(address + 16, cnt);

            if (value < min) {
                UNSAFE.putShort(address + 20, (short) value);
            }

            if (value > max) {
                UNSAFE.putShort(address + 22, (short) value);
            }
        }

        public void merge(Aggregates rights) {
            for (int rightOffset = 0; rightOffset < SIZE; rightOffset += 128) {
                long rightAddress = rights.pointer + rightOffset;
                int length = UNSAFE.getInt(rightAddress);

                if (length == 0) {
                    continue;
                }

                int hash = UNSAFE.getInt(rightAddress + 4);

                for (int offset = offset(hash);; offset = next(offset)) {
                    long address = pointer + offset;

                    if (equal(address + 24, rightAddress + 24, length)) {
                        long sum = UNSAFE.getLong(address + 8) + UNSAFE.getLong(rightAddress + 8);
                        int cnt = UNSAFE.getInt(address + 16) + UNSAFE.getInt(rightAddress + 16);
                        short min = (short) Math.min(UNSAFE.getShort(address + 20), UNSAFE.getShort(rightAddress + 20));
                        short max = (short) Math.max(UNSAFE.getShort(address + 22), UNSAFE.getShort(rightAddress + 22));

                        UNSAFE.putLong(address + 8, sum);
                        UNSAFE.putInt(address + 16, cnt);
                        UNSAFE.putShort(address + 20, min);
                        UNSAFE.putShort(address + 22, max);
                        break;
                    }

                    int len = UNSAFE.getInt(address);

                    if (len == 0) {
                        UNSAFE.copyMemory(rightAddress, address, length + 24);
                        break;
                    }
                }
            }
        }

        public Map<String, Aggregate> aggregate() {
            TreeMap<String, Aggregate> set = new TreeMap<>();

            for (int offset = 0; offset < SIZE; offset += 128) {
                long address = pointer + offset;
                int length = UNSAFE.getInt(address);

                if (length != 0) {
                    byte[] array = new byte[length - 1];
                    UNSAFE.copyMemory(null, address + 24, array, Unsafe.ARRAY_BYTE_BASE_OFFSET, array.length);
                    String key = new String(array);

                    long sum = UNSAFE.getLong(address + 8);
                    int cnt = UNSAFE.getInt(address + 16);
                    short min = UNSAFE.getShort(address + 20);
                    short max = UNSAFE.getShort(address + 22);

                    Aggregate aggregate = new Aggregate(min, max, sum, cnt);
                    set.put(key, aggregate);
                }
            }

            return set;
        }

        private static void alloc(long reference, int length, int hash, long address) {
            UNSAFE.putInt(address, length);
            UNSAFE.putInt(address + 4, hash);
            UNSAFE.putShort(address + 20, Short.MAX_VALUE);
            UNSAFE.putShort(address + 22, Short.MIN_VALUE);
            UNSAFE.copyMemory(reference, address + 24, length);
        }

        private static int offset(int hash) {
            return ((hash) & (ENTRIES - 1)) << 7;
        }

        private static int next(int prev) {
            return (prev + 128) & (SIZE - 1);
        }

        private static boolean equal(long leftAddress, long leftWord, long rightAddress, int length) {
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

            return leftWord == word(rightAddress);
        }

        private static boolean equal(long leftAddress, long rightAddress, int length) {
            do {
                long left = UNSAFE.getLong(leftAddress);
                long right = UNSAFE.getLong(rightAddress);

                if (left != right) {
                    return false;
                }

                leftAddress += 8;
                rightAddress += 8;
                length -= 8;
            } while (length > 0);

            return true;
        }
    }

    private static class Aggregator extends Thread {

        private final AtomicInteger counter;
        private final AtomicReference<Aggregates> result;
        private final int segments;

        public Aggregator(AtomicInteger counter, AtomicReference<Aggregates> result, int segments) {
            super("aggregator");
            this.counter = counter;
            this.result = result;
            this.segments = segments;
        }

        @Override
        public void run() {
            Aggregates aggregates = new Aggregates();
            ByteBuffer buffer = allocate(SEGMENT_SIZE + SEGMENT_OVERLAP);

            try (FileChannel channel = FileChannel.open(FILE)) {
                for (int segment; (segment = counter.getAndIncrement()) < segments;) {
                    buffer.clear();

                    long position = (long) SEGMENT_SIZE * segment;
                    int size = channel.read(buffer, position);

                    long address = address(buffer);
                    long limit = address + Math.min(SEGMENT_SIZE, size - 1);

                    if (segment > 0) {
                        address = next(address);
                    }

                    aggregate(aggregates, address, limit);
                }
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
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

            while (position <= limit) { // branchy version, credit: thomaswue
                int length;
                int hash;

                long ptr = 0;
                long word = word(position);
                long separator = separator(word);

                if (separator != 0) {
                    length = length(separator);
                    word = mask(word, separator);
                    hash = mix(word);
                    ptr = aggregates.find(word, hash);
                }
                else {
                    long word0 = word;
                    word = word(position + 8);
                    separator = separator(word);

                    if (separator != 0) {
                        length = length(separator) + 8;
                        word = mask(word, separator);
                        hash = mix(word ^ word0);
                        ptr = aggregates.find(word0, word, hash);
                    }
                    else {
                        length = 16;
                        long h = word ^ word0;

                        while (true) {
                            word = word(position + length);
                            separator = separator(word);

                            if (separator == 0) {
                                length += 8;
                                h ^= word;
                                continue;
                            }

                            length += length(separator);
                            word = mask(word, separator);
                            hash = mix(h ^ word);
                            break;
                        }
                    }
                }

                if (ptr == 0) {
                    ptr = aggregates.put(position, word, length, hash);
                }

                position = update(ptr, position + length);
            }
        }

        private static long update(long ptr, long position) {
            // idea: merykitty
            long word = word(position);
            long inverted = ~word;
            int dot = Long.numberOfTrailingZeros(inverted & DOT_BITS);
            long signed = (inverted << 59) >> 63;
            long mask = ~(signed & 0xFF);
            long digits = ((word & mask) << (28 - dot)) & 0x0F000F0F00L;
            long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            int value = (int) ((abs ^ signed) - signed);

            Aggregates.update(ptr, value);
            return position + (dot >> 3) + 3;
        }

        private static long separator(long word) {
            long match = word ^ COMMA_PATTERN;
            return (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);
        }

        private static long mask(long word, long separator) {
            long mask = separator ^ (separator - 1);
            return word & mask;
        }

        private static int length(long separator) {
            return (Long.numberOfTrailingZeros(separator) >>> 3) + 1;
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
