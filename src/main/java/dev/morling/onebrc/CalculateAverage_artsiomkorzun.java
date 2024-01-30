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
    private static final long SEGMENT_SIZE = 2 * 1024 * 1024;
    private static final long COMMA_PATTERN = 0x3B3B3B3B3B3B3B3BL;
    private static final long LINE_PATTERN = 0x0A0A0A0A0A0A0A0AL;
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

    private record Aggregate(long min, long max, long sum, long cnt) {
    }

    private static class Aggregates {

        private static final long ENTRIES = 64 * 1024;
        private static final long SIZE = 256 * ENTRIES;
        private static final long MASK = (ENTRIES - 1) << 8;

        private final long pointer;

        public Aggregates() {
            long address = UNSAFE.allocateMemory(SIZE + 4096);
            pointer = (address + 4095) & (~4095);
            UNSAFE.setMemory(pointer, SIZE, (byte) 0);
        }

        public long find(long word, long hash) {
            long address = pointer + offset(hash);
            long w = word(address + 48);
            return (w == word) ? address : 0;
        }

        public long find(long word1, long word2, long hash) {
            long address = pointer + offset(hash);
            long w1 = word(address + 48);
            long w2 = word(address + 56);
            return (word1 == w1) && (word2 == w2) ? address : 0;
        }

        public long put(long reference, long word, long length, long hash) {
            for (long offset = offset(hash);; offset = next(offset)) {
                long address = pointer + offset;
                if (equal(reference, word, address + 48, length)) {
                    return address;
                }

                long len = UNSAFE.getLong(address);
                if (len == 0) {
                    alloc(reference, length, hash, address);
                    return address;
                }
            }
        }

        public static void update(long address, long value) {
            long sum = UNSAFE.getLong(address + 16) + value;
            long cnt = UNSAFE.getLong(address + 24) + 1;
            long min = UNSAFE.getLong(address + 32);
            long max = UNSAFE.getLong(address + 40);

            UNSAFE.putLong(address + 16, sum);
            UNSAFE.putLong(address + 24, cnt);

            if (value < min) {
                UNSAFE.putLong(address + 32, value);
            }

            if (value > max) {
                UNSAFE.putLong(address + 40, value);
            }
        }

        public void merge(Aggregates rights) {
            for (int rightOffset = 0; rightOffset < SIZE; rightOffset += 256) {
                long rightAddress = rights.pointer + rightOffset;
                long length = UNSAFE.getLong(rightAddress);

                if (length == 0) {
                    continue;
                }

                long hash = UNSAFE.getLong(rightAddress + 8);

                for (long offset = offset(hash);; offset = next(offset)) {
                    long address = pointer + offset;

                    if (equal(address + 48, rightAddress + 48, length)) {
                        long sum = UNSAFE.getLong(address + 16) + UNSAFE.getLong(rightAddress + 16);
                        long cnt = UNSAFE.getLong(address + 24) + UNSAFE.getLong(rightAddress + 24);
                        long min = Math.min(UNSAFE.getLong(address + 32), UNSAFE.getLong(rightAddress + 32));
                        long max = Math.max(UNSAFE.getLong(address + 40), UNSAFE.getLong(rightAddress + 40));

                        UNSAFE.putLong(address + 16, sum);
                        UNSAFE.putLong(address + 24, cnt);
                        UNSAFE.putLong(address + 32, min);
                        UNSAFE.putLong(address + 40, max);
                        break;
                    }

                    long len = UNSAFE.getLong(address);

                    if (len == 0) {
                        UNSAFE.copyMemory(rightAddress, address, length + 48);
                        break;
                    }
                }
            }
        }

        public Map<String, Aggregate> aggregate() {
            TreeMap<String, Aggregate> set = new TreeMap<>();

            for (long offset = 0; offset < SIZE; offset += 256) {
                long address = pointer + offset;
                long length = UNSAFE.getLong(address);

                if (length != 0) {
                    byte[] array = new byte[(int) length - 1];
                    UNSAFE.copyMemory(null, address + 48, array, Unsafe.ARRAY_BYTE_BASE_OFFSET, array.length);
                    String key = new String(array);

                    long sum = UNSAFE.getLong(address + 16);
                    long cnt = UNSAFE.getLong(address + 24);
                    long min = UNSAFE.getLong(address + 32);
                    long max = UNSAFE.getLong(address + 40);

                    Aggregate aggregate = new Aggregate(min, max, sum, cnt);
                    set.put(key, aggregate);
                }
            }

            return set;
        }

        private static void alloc(long reference, long length, long hash, long address) {
            UNSAFE.putLong(address, length);
            UNSAFE.putLong(address + 8, hash);
            UNSAFE.putLong(address + 32, Long.MAX_VALUE);
            UNSAFE.putLong(address + 40, Long.MIN_VALUE);
            UNSAFE.copyMemory(reference, address + 48, length);
        }

        private static long offset(long hash) {
            return hash & MASK;
        }

        private static long next(long prev) {
            return (prev + 256) & (SIZE - 1);
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

                    long separator1 = separator(word1);
                    long separator2 = separator(word2);
                    long separator3 = separator(word3);

                    long pointer1 = find(aggregates, chunk1, word1, separator1);
                    long pointer2 = find(aggregates, chunk2, word2, separator2);
                    long pointer3 = find(aggregates, chunk3, word3, separator3);

                    long value1 = value(chunk1);
                    long value2 = value(chunk2);
                    long value3 = value(chunk3);

                    Aggregates.update(pointer1, value1);
                    Aggregates.update(pointer2, value2);
                    Aggregates.update(pointer3, value3);
                }

                while (chunk1.has()) {
                    long word1 = word(chunk1.position);
                    long separator1 = separator(word1);
                    long pointer1 = find(aggregates, chunk1, word1, separator1);
                    long value1 = value(chunk1);
                    Aggregates.update(pointer1, value1);
                }

                while (chunk2.has()) {
                    long word2 = word(chunk2.position);
                    long separator2 = separator(word2);
                    long pointer2 = find(aggregates, chunk2, word2, separator2);
                    long value2 = value(chunk2);
                    Aggregates.update(pointer2, value2);
                }

                while (chunk3.has()) {
                    long word3 = word(chunk3.position);
                    long separator3 = separator(word3);
                    long pointer3 = find(aggregates, chunk3, word3, separator3);
                    long value3 = value(chunk3);
                    Aggregates.update(pointer3, value3);
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

                return position + (Long.numberOfTrailingZeros(line) >>> 3) + 1;
            }
        }

        private static long find(Aggregates aggregates, Chunk chunk, long word, long separator) {
            long start = chunk.position;
            long hash;

            if (separator != 0) {
                word = mask(word, separator);
                hash = mix(word);

                chunk.position += length(separator);
                long pointer = aggregates.find(word, hash);

                if (pointer != 0) {
                    return pointer;
                }
            }
            else {
                long word0 = word;
                word = word(start + 8);
                separator = separator(word);

                if (separator != 0) {
                    word = mask(word, separator);
                    hash = mix(word ^ word0);

                    chunk.position += length(separator) + 8;
                    long pointer = aggregates.find(word0, word, hash);

                    if (pointer != 0) {
                        return pointer;
                    }
                }
                else {
                    chunk.position += 16;
                    hash = word ^ word0;

                    while (true) {
                        word = word(chunk.position);
                        separator = separator(word);

                        if (separator == 0) {
                            chunk.position += 8;
                            hash ^= word;
                            continue;
                        }

                        word = mask(word, separator);
                        hash = mix(hash ^ word);
                        chunk.position += length(separator);
                        break;
                    }
                }
            }

            long length = chunk.position - start;
            return aggregates.put(start, word, length, hash);
        }

        private static long value(Chunk chunk) {
            long num = word(chunk.position);
            long dot = dot(num);
            chunk.position += (dot >> 3) + 3;
            return value(num, dot);
        }

        private static long separator(long word) {
            long match = word ^ COMMA_PATTERN;
            return (match - 0x0101010101010101L) & (~match & 0x8080808080808080L);
        }

        private static long mask(long word, long separator) {
            long mask = separator ^ (separator - 1);
            return word & mask;
        }

        private static long length(long separator) {
            return (Long.numberOfTrailingZeros(separator) >>> 3) + 1;
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