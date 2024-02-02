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
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CalculateAverage_artsiomkorzun {

    private static final Path FILE = Path.of("./measurements.txt");
    private static final long SEGMENT_SIZE = 2 * 1024 * 1024;
    private static final long COMMA_PATTERN = 0x3B3B3B3B3B3B3B3BL;
    private static final long LINE_PATTERN = 0x0A0A0A0A0A0A0A0AL;
    private static final long DOT_BITS = 0x10101000;
    private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);
    private static final long[] WORD_MASK = { 0, 0, 0, 0, 0, 0, 0, 0, -1 };
    private static final int[] LENGTH_MASK = { 0, 0, 0, 0, 0, 0, 0, 0, -1 };

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
        Optional<String> command = info.command();
        Optional<String[]> arguments = info.arguments();

        if (command.isPresent()) {
            commands.add(command.get());
        }

        if (arguments.isPresent()) {
            commands.addAll(Arrays.asList(arguments.get()));
        }

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

        Map<String, Aggregate> aggregates = result.get().build();
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

        private static final long ENTRIES = 64 * 1024;
        private static final long SIZE = 128 * ENTRIES;
        private static final long MASK = (ENTRIES - 1) << 7;

        private final long pointer;

        public Aggregates() {
            long address = UNSAFE.allocateMemory(SIZE + 4096);
            pointer = (address + 4095) & (~4095);
            UNSAFE.setMemory(pointer, SIZE, (byte) 0);
        }

        public long find(long word1, long word2, long hash) {
            long address = pointer + offset(hash);
            long w1 = word(address + 24);
            long w2 = word(address + 32);
            return (word1 == w1) && (word2 == w2) ? address : 0;
        }

        public long put(long reference, long word, long length, long hash) {
            for (long offset = offset(hash);; offset = next(offset)) {
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

        public static void update(long address, long value) {
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
            for (long rightOffset = 0; rightOffset < SIZE; rightOffset += 128) {
                long rightAddress = rights.pointer + rightOffset;
                int length = UNSAFE.getInt(rightAddress);

                if (length == 0) {
                    continue;
                }

                int hash = UNSAFE.getInt(rightAddress + 4);

                for (long offset = offset(hash);; offset = next(offset)) {
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

        public Map<String, Aggregate> build() {
            TreeMap<String, Aggregate> set = new TreeMap<>();

            for (long offset = 0; offset < SIZE; offset += 128) {
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

        private static void alloc(long reference, long length, long hash, long address) {
            UNSAFE.putInt(address, (int) length);
            UNSAFE.putInt(address + 4, (int) hash);
            UNSAFE.putShort(address + 20, Short.MAX_VALUE);
            UNSAFE.putShort(address + 22, Short.MIN_VALUE);
            UNSAFE.copyMemory(reference, address + 24, length);
        }

        private static long offset(long hash) {
            return hash & MASK;
        }

        private static long next(long prev) {
            return (prev + 128) & (SIZE - 1);
        }

        private static boolean equal(long leftAddress, long leftWord, long rightAddress, long length) {
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

        private static boolean equal(long leftAddress, long rightAddress, long length) {
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
                long size = Math.min(SEGMENT_SIZE + 1, fileSize - position);
                long start = fileAddress + position;
                long end = start + size;

                if (segment > 0) {
                    start = next(start);
                }

                long chunk = (end - start) / 3;
                long left = next(start + chunk);
                long right = next(start + chunk + chunk);

                Chunk chunk1 = new Chunk(start, left);
                Chunk chunk2 = new Chunk(left, right);
                Chunk chunk3 = new Chunk(right, end);

                while (chunk1.has() && chunk2.has() && chunk3.has()) {
                    long word1 = word(chunk1.position);
                    long word2 = word(chunk2.position);
                    long word3 = word(chunk3.position);
                    long word4 = word(chunk1.position + 8);
                    long word5 = word(chunk2.position + 8);
                    long word6 = word(chunk3.position + 8);

                    long separator1 = separator(word1);
                    long separator2 = separator(word2);
                    long separator3 = separator(word3);
                    long separator4 = separator(word4);
                    long separator5 = separator(word5);
                    long separator6 = separator(word6);

                    long pointer1 = find(aggregates, chunk1, word1, word4, separator1, separator4);
                    long pointer2 = find(aggregates, chunk2, word2, word5, separator2, separator5);
                    long pointer3 = find(aggregates, chunk3, word3, word6, separator3, separator6);

                    long value1 = value(chunk1);
                    long value2 = value(chunk2);
                    long value3 = value(chunk3);

                    Aggregates.update(pointer1, value1);
                    Aggregates.update(pointer2, value2);
                    Aggregates.update(pointer3, value3);
                }

                while (chunk1.has()) {
                    long word1 = word(chunk1.position);
                    long word2 = word(chunk1.position + 8);

                    long separator1 = separator(word1);
                    long separator2 = separator(word2);

                    long pointer = find(aggregates, chunk1, word1, word2, separator1, separator2);
                    long value = value(chunk1);

                    Aggregates.update(pointer, value);
                }

                while (chunk2.has()) {
                    long word1 = word(chunk2.position);
                    long word2 = word(chunk2.position + 8);

                    long separator1 = separator(word1);
                    long separator2 = separator(word2);

                    long pointer = find(aggregates, chunk2, word1, word2, separator1, separator2);
                    long value = value(chunk2);

                    Aggregates.update(pointer, value);
                }

                while (chunk3.has()) {
                    long word1 = word(chunk3.position);
                    long word2 = word(chunk3.position + 8);

                    long separator1 = separator(word1);
                    long separator2 = separator(word2);

                    long pointer = find(aggregates, chunk3, word1, word2, separator1, separator2);
                    long value = value(chunk3);

                    Aggregates.update(pointer, value);
                }
            }

            while (!result.compareAndSet(null, aggregates)) {
                Aggregates rights = result.getAndSet(null);

                if (rights != null) {
                    aggregates.merge(rights);
                }
            }
        }

        private static long next(long position) {
            while (true) {
                long word = word(position);
                long match = word ^ LINE_PATTERN;
                long line = (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);

                if (line == 0) {
                    position += 8;
                    continue;
                }

                return position + length(line) + 1;
            }
        }

        private static long find(Aggregates aggregates, Chunk chunk, long word1, long word2, long separator1, long separator2) {
            boolean small = (separator1 | separator2) != 0;
            long start = chunk.position;
            long hash;
            long word;

            if (small) {
                int length1 = length(separator1);
                int length2 = length(separator2);
                word1 = mask(word1, separator1);
                word2 = mask(word2 & WORD_MASK[length1], separator2);
                hash = mix(word1 ^ word2);

                chunk.position += length1 + (length2 & LENGTH_MASK[length1]) + 1;
                long pointer = aggregates.find(word1, word2, hash);

                if (pointer != 0) {
                    return pointer;
                }

                word = (separator1 == 0) ? word2 : word1;
            }
            else {
                chunk.position += 16;
                hash = word1 ^ word2;

                while (true) {
                    word = word(chunk.position);
                    long separator = separator(word);

                    if (separator == 0) {
                        chunk.position += 8;
                        hash ^= word;
                        continue;
                    }

                    word = mask(word, separator);
                    hash = mix(hash ^ word);
                    chunk.position += length(separator) + 1;
                    break;
                }
            }

            long length = chunk.position - start;
            return aggregates.put(start, word, length, hash);
        }

        private static long value(Chunk chunk) {
            long num = word(chunk.position);
            long dot = dot(num);
            long value = value(num, dot);
            chunk.position += (dot >> 3) + 3;
            return value;
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
            return Long.numberOfTrailingZeros(separator) >>> 3;
        }

        private static long mix(long x) {
            long h = x * -7046029254386353131L;
            h ^= h >>> 35;
            return h;
            // h ^= h >>> 32;
            // return (int) (h ^ h >>> 16);
        }

        private static long dot(long num) {
            return Long.numberOfTrailingZeros(~num & DOT_BITS);
        }

        private static long value(long w, long dot) {
            long signed = (~w << 59) >> 63;
            long mask = ~(signed & 0xFF);
            long digits = ((w & mask) << (28 - dot)) & 0x0F000F0F00L;
            long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            return (abs ^ signed) - signed;
        }
    }

    private static class Chunk {
        final long limit;
        long position;

        public Chunk(long position, long limit) {
            this.position = position;
            this.limit = limit;
        }

        boolean has() {
            return position < limit;
        }
    }
}