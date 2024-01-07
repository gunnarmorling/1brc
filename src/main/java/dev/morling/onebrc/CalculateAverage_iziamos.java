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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.LongStream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_iziamos {
    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_SIZE = 8 * 1024 * 1024;
    private static final int NAME_ARRAY_LENGTH = 103;
    private static final int NAME_ARRAY_LENGTH_POSITION = NAME_ARRAY_LENGTH - 1;
    private static final int NAME_ARRAY_HASHCODE_POSITION = NAME_ARRAY_LENGTH - 2;
    private final static ReentrantLock mergeLock = new ReentrantLock();

    public static void main(String[] args) throws Exception {
        // Thread.sleep(10000);
        final var channel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ);

        final var fileSize = channel.size();
        final long threadCount = 1 + fileSize / CHUNK_SIZE;
        final ResultSet aggregate = new ResultSet();

        final CompletableFuture<?> taskCompleteFutureThing = CompletableFuture.allOf(LongStream.range(0, threadCount)
                .mapToObj(t -> processSegment(channel, t, CHUNK_SIZE, t == threadCount - 1)
                        .thenAccept(result -> mergeResults(aggregate, result)))
                .toArray(CompletableFuture[]::new));

        taskCompleteFutureThing.join();

        final Map<String, ResultRow> output = new TreeMap<>();

        aggregate.forEach((name, max, min, sum, count) -> output.put(nameToString(name), new ResultRow(min, (double) sum / count, max)));

        System.out.println(output);
        // System.out.println(Arrays.stream(aggregate.counts).sum());
    }

    private static void mergeResults(final ResultSet aggregate, final ResultSet result) {
        mergeLock.lock();
        aggregate.merge(result);
        mergeLock.unlock();
    }

    private record ResultRow(long min, double mean, long max) {
        public String toString() {
            return STR."\{formatLong(min)}/\{round(mean)}/\{formatLong(max)}";
        }

        private double formatLong(final long value) {

            return value / 10.0;
        }
        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private static CompletableFuture<ResultSet> processSegment(final FileChannel channel,
                                                               final long chunkNumber,
                                                               final long size,
                                                               final boolean isLast) {
        final var result = new CompletableFuture<ResultSet>();

        Thread.ofVirtual().start(() -> {
            try {
                final long start = chunkNumber * size;
                final long memoryMapSize = mapsize(channel.size(), start, size, isLast);
                final ByteBuffer mmap = channel.map(READ_ONLY, start, memoryMapSize);
                skipIncomplete(mmap, start);
                result.complete(processEvents(mmap, isLast ? memoryMapSize : size));
            }
            catch (IOException e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    private static long mapsize(final long total, final long start, final long chunk, final boolean isLast) {
        final long chunkWithSomeOverlap = chunk + 128;
        if (isLast) {
            return total - start;
        }

        return chunkWithSomeOverlap;
    }

    private static void skipIncomplete(final ByteBuffer buffer, final long start) {
        if (start == 0) {
            return;
        }
        for (byte b = buffer.get();; b = buffer.get()) {
            if (b == '\n')
                return;
        }
    }

    private static ResultSet processEvents(final ByteBuffer buffer, final long limit) {
        final var result = new ResultSet();
        int[] nameBuffer = new int[NAME_ARRAY_LENGTH];
        while (buffer.hasRemaining() && buffer.position() <= limit) {
            nameBuffer = processEvent(buffer, nameBuffer, result);
        }
        return result;
    }

    private static int[] processEvent(final ByteBuffer buffer, final int[] nameBuffer, final ResultSet map) {
        parseName(buffer, nameBuffer);
        final int value = readValue(buffer);

        return map.put(nameBuffer, value) ? new int[NAME_ARRAY_LENGTH] : nameBuffer;
    }

    private static void parseName(final ByteBuffer buffer, final int[] name) {
        byte i = 0;
        int hash = 0;
        for (byte b = buffer.get(); b != ';'; b = buffer.get(), ++i) {
            writeByte(name, i, b);
            hash = 31 * hash + b;
        }
        setNameArrayLength(name, i);
        setNameArrayHash(name, hash);
    }

    private static void writeByte(final int[] name, final int i, final byte b) {
        name[i] = b;
    }

    private static int readValue(final ByteBuffer buffer) {
        final byte first = buffer.get();
        final boolean isNegative = first == '-';

        int value = digitCharToInt(isNegative ? buffer.get() : first);

        final byte second = buffer.get();
        value = addSecondDigitIfPresent(buffer, second, value);
        value = addDecimal(buffer, value);

        consumeNewLine(buffer);
        return isNegative ? -value : value;
    }

    private static void consumeNewLine(final ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            buffer.get();
        }
    }

    private static int addDecimal(final ByteBuffer buffer, int value) {
        value *= 10;
        value += digitCharToInt(buffer.get());
        return value;
    }

    private static int addSecondDigitIfPresent(final ByteBuffer buffer, final byte second, int value) {
        if (second != '.') {
            value *= 10;
            value += digitCharToInt(second);
            buffer.get();
        }
        return value;
    }

    private static int digitCharToInt(final byte b) {
        return b - '0';
    }

    private interface ResultConsumer {
        void consume(final int[] name, final long max, final long min, final long sum, final long count);
    }

    private static class ResultSet {
        private static final int MAP_SIZE = 16384;
        private static final int MASK = MAP_SIZE - 1;

        private final int[][] names = new int[MAP_SIZE][];
        private final long[] maximums = new long[MAP_SIZE];
        private final long[] minimums = new long[MAP_SIZE];
        private final long[] sums = new long[MAP_SIZE];
        private final long[] counts = new long[MAP_SIZE];

        ResultSet() {
            if (Integer.bitCount(MAP_SIZE) != 1) {
                throw new RuntimeException("blah");
            }
            Arrays.fill(maximums, Long.MIN_VALUE);
            Arrays.fill(minimums, Long.MAX_VALUE);
        }

        /**
         * @return true if the name is new
         */
        public boolean put(final int[] name, long value) {
            final int hash = name[NAME_ARRAY_HASHCODE_POSITION];
            final int slot = findSlot(hash, name);
            return insert(slot, name, value);
        }

        public void forEach(final ResultConsumer consumer) {
            for (int i = 0; i < ResultSet.MAP_SIZE; ++i) {
                final int[] name = names[i];

                if (name == null) {
                    continue;
                }

                consumer.consume(name, maximums[i], minimums[i], sums[i], counts[i]);
            }
        }

        public void merge(final ResultSet other) {
            other.forEach((name, max, min, sum, count) -> {
                final int hash = name[NAME_ARRAY_HASHCODE_POSITION];
                final int slot = findSlot(hash, name);
                mergeValues(slot, name, min, max, sum, count);
            });

        }

        private int findSlot(final int hash, final int[] name) {
            for (int slot = mask(hash);; slot = mask(++slot)) {
                if (isCorrectSlot(name, slot)) {
                    return slot;
                }
            }
        }

        private boolean isCorrectSlot(final int[] name, final int slot) {
            return names[slot] == null || nameArrayEquals(names[slot], name);
        }

        private int mask(final long key) {
            return (int) (key & MASK);
        }

        private boolean insert(final int slot, final int[] name, final long value) {
            final int[] currentValue = names[slot];
            updateValues(slot, value);
            if (currentValue == null) {
                names[slot] = name;
                return true;
            }
            return false;
        }

        private void updateValues(final int slot, final long value) {
            maximums[slot] = Math.max(maximums[slot], value);
            minimums[slot] = Math.min(minimums[slot], value);
            sums[slot] += value;
            counts[slot]++;
        }

        private void mergeValues(final int slot,
                                 final int[] name,
                                 final long min,
                                 final long max,
                                 final long sum,
                                 final long count) {
            names[slot] = name;
            maximums[slot] = Math.max(maximums[slot], max);
            minimums[slot] = Math.min(minimums[slot], min);
            sums[slot] += sum;
            counts[slot] += count;
        }
    }

    private static boolean nameArrayEquals(final int[] a, final int[] b) {
        return Arrays.equals(a, 0, getNameArrayLength(a), b, 0, getNameArrayLength(b));
    }

    private static int getNameArrayLength(final int[] name) {
        return name[NAME_ARRAY_LENGTH_POSITION];
    }

    private static void setNameArrayLength(final int[] name, int length) {
        name[NAME_ARRAY_LENGTH_POSITION] = length;
    }

    private static void setNameArrayHash(final int[] name, int hash) {
        name[NAME_ARRAY_HASHCODE_POSITION] = hash;
    }

    private static String nameToString(final int[] name) {
        final int nameArrayLength = getNameArrayLength(name);
        final byte[] bytes = new byte[nameArrayLength];
        for (int i = 0; i < nameArrayLength; ++i) {
            bytes[i] = (byte) name[i];
        }

        return new String(bytes, UTF_8);
    }
}
