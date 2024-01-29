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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CalculateAverage_artsiomkorzun {

    private static final Path FILE = Path.of("./measurements.txt");
    private static final long SEGMENT_SIZE = 4 * 1024 * 1024;
    private static final long COMMA_PATTERN = 0x3B3B3B3B3B3B3B3BL;
    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

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

        if (isSpawn(args)) {
            spawn();
            return;
        }

        execute();
    }

    private static boolean isSpawn(String[] args) {
        for (String arg : args) {
            if ("--worker".equals(arg)) {
                return false;
            }
        }

        return true;
    }

    private static void spawn() throws Exception {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> commands = new ArrayList<>();
        info.command().ifPresent(commands::add);
        info.arguments().ifPresent(args -> commands.addAll(Arrays.asList(args)));
        commands.add("--worker");

        new ProcessBuilder()
                .command(commands)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    private static void execute() throws Exception {
        MemorySegment fileMemory = map(FILE);
        long fileAddress = fileMemory.address();
        long fileSize = fileMemory.byteSize();
        int segmentCount = (int) ((fileSize + SEGMENT_SIZE - 1) / SEGMENT_SIZE);

        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Aggregates> result = new AtomicReference<>();

        int parallelism = Runtime.getRuntime().availableProcessors();
        Aggregator[] aggregators = new Aggregator[parallelism];

        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(counter, result, fileAddress, fileSize, segmentCount);
            aggregators[i].start();
        }

        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].join();
        }

        Map<String, Aggregate> aggregates = result.get().aggregate();
        System.out.println(text(aggregates));
        System.out.close();
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
        private static final int MASK = (ENTRIES - 1) << 7;

        private final long pointer;

        public Aggregates() {
            long address = UNSAFE.allocateMemory(SIZE + 4096);
            pointer = (address + 4095) & (~4095);
            UNSAFE.setMemory(pointer, SIZE, (byte) 0);
        }

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
            return hash & MASK;
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
        private final long fileAddress;
        private final long fileSize;
        private final int segmentCount;

        public Aggregator(AtomicInteger counter, AtomicReference<Aggregates> result,
                          long fileAddress, long fileSize, int segmentCount) {
            super("aggregator");
            this.counter = counter;
            this.result = result;
            this.fileAddress = fileAddress;
            this.fileSize = fileSize;
            this.segmentCount = segmentCount;
        }

        @Override
        public void run() {
            Aggregates aggregates = new Aggregates();

            for (int segment; (segment = counter.getAndIncrement()) < segmentCount;) {
                long position = SEGMENT_SIZE * segment;
                long size = Math.min(SEGMENT_SIZE, fileSize - position - 1);
                long start = fileAddress + position;
                long end = start + size;

                if (segment > 0) {
                    start = next(start);
                }

                long chunk = (end - start) / 3;
                long left = next(start + chunk);
                long right = next(start + chunk + chunk);

                aggregate(aggregates, start, left - 1, left, right - 1, right, end);
            }

            while (!result.compareAndSet(null, aggregates)) {
                Aggregates rights = result.getAndSet(null);

                if (rights != null) {
                    aggregates.merge(rights);
                }
            }
        }

        private static long next(long position) {
            while (UNSAFE.getByte(position++) != '\n') {
                // continue
            }
            return position;
        }

        private static void aggregate(Aggregates aggregates, long position1, long limit1, long position2, long limit2, long position3, long limit3) {
            while (position1 <= limit1 && position2 <= limit2 && position3 <= limit3) {
                long word1 = word(position1);
                long word2 = word(position2);
                long word3 = word(position3);

                long separator1 = separator(word1);
                long separator2 = separator(word2);
                long separator3 = separator(word3);

                position1 = process(aggregates, position1, word1, separator1);
                position2 = process(aggregates, position2, word2, separator2);
                position3 = process(aggregates, position3, word3, separator3);
            }

            while (position1 <= limit1) {
                long word1 = word(position1);
                long separator1 = separator(word1);
                position1 = process(aggregates, position1, word1, separator1);
            }

            while (position2 <= limit2) {
                long word2 = word(position2);
                long separator2 = separator(word2);
                position2 = process(aggregates, position2, word2, separator2);
            }

            while (position3 <= limit3) {
                long word3 = word(position3);
                long separator3 = separator(word3);
                position3 = process(aggregates, position3, word3, separator3);
            }
        }

        private static long process(Aggregates aggregates, long position, long word, long separator) {
            long end = position;

            int length;
            int hash;
            int value;

            if (separator != 0) {
                length = length(separator);
                word = mask(word, separator);
                hash = mix(word);
                end += length;

                long num = word(end);
                int dot = dot(num);
                value = value(num, dot);
                end += (dot >> 3) + 3;
                long pointer = aggregates.find(word, hash);

                if (pointer != 0) {
                    Aggregates.update(pointer, value);
                    return end;
                }
            }
            else {
                long word0 = word;
                word = word(end + 8);
                separator = separator(word);

                if (separator != 0) {
                    length = length(separator) + 8;
                    word = mask(word, separator);
                    hash = mix(word ^ word0);
                    end += length;

                    long num = word(end);
                    int dot = dot(num);
                    value = value(num, dot);
                    end += (dot >> 3) + 3;
                    long pointer = aggregates.find(word0, word, hash);

                    if (pointer != 0) {
                        Aggregates.update(pointer, value);
                        return end;
                    }
                }
                else {
                    length = 16;
                    long h = word ^ word0;

                    while (true) {
                        word = word(end + length);
                        separator = separator(word);

                        if (separator == 0) {
                            length += 8;
                            h ^= word;
                            continue;
                        }

                        length += length(separator);
                        word = mask(word, separator);
                        hash = mix(h ^ word);
                        end += length;

                        long num = word(end);
                        int dot = dot(num);
                        value = value(num, dot);
                        end += (dot >> 3) + 3;
                        break;
                    }
                }
            }

            long pointer = aggregates.put(position, word, length, hash);
            Aggregates.update(pointer, value);
            return end;
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

        private static int mix(long x) {
            long h = x * -7046029254386353131L;
            h ^= h >>> 35;
            return (int) h;
            // h ^= h >>> 32;
            // return (int) (h ^ h >>> 16);
        }

        private static int dot(long num) {
            return Long.numberOfTrailingZeros(~num & DOT_BITS);
        }

        private static int value(long w, int dot) {
            long signed = (~w << 59) >> 63;
            long mask = ~(signed & 0xFF);
            long digits = ((w & mask) << (28 - dot)) & 0x0F000F0F00L;
            long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            return (int) ((abs ^ signed) - signed);
        }
    }
}
