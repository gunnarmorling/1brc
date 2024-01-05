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
    private static int MAXIMUM_CITIES = 10_000;
    private static int CHUNK_SIZE = 8 * 1024 * 1024;

    private static final int MAP_MISSING_VALUE = -1;
    private final static ThreadLocal<byte[]> THREAD_LOCAL_NAME_BUFFER = ThreadLocal.withInitial(() -> new byte[101]);

    private final static ReentrantLock mergeLock = new ReentrantLock();

    public static void main(String[] args) throws Exception {
        // Thread.sleep(10000);
        final var channel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ);

        final var fileSize = channel.size();
        final long threadCount = 1 + fileSize / CHUNK_SIZE;
        final PartialResult aggregate = new PartialResult();

        final CompletableFuture<?> taskCompleteFutureThing = CompletableFuture.allOf(LongStream.range(0, threadCount)
                .mapToObj(t -> processSegment(channel, t, CHUNK_SIZE, t == threadCount - 1)
                        .thenAccept(result -> mergeResults(aggregate, result)))
                .toArray(CompletableFuture[]::new));

        taskCompleteFutureThing.join();

        final Map<String, ResultRow> output = new TreeMap<>();

        aggregate.forEach((name, max, min, sum, count) -> output.put(bytesToString(name), new ResultRow(min, (double) sum / count, max)));

        System.out.println(output);
        // System.out.println(Arrays.stream(aggregate.counts).sum());
    }

    private static void mergeResults(final PartialResult aggregate, final PartialResult result) {
        mergeLock.lock();
        aggregate.merge(result);
        mergeLock.unlock();
    }

    private record ResultRow(long min, double mean, long max) {
        public String toString() {
            return formatLong(min) + "/" + round(mean) + "/" + formatLong(max);
        }

        private double formatLong(final long value) {

            return value / 10.0;
        }
        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private static CompletableFuture<PartialResult> processSegment(final FileChannel channel,
                                                                   final long chunkNumber,
                                                                   final long size,
                                                                   final boolean isLast) {

        final var result = new CompletableFuture<PartialResult>();

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

    private static PartialResult processEvents(final ByteBuffer buffer, final long limit) {
        final var result = new PartialResult();
        while (buffer.hasRemaining() && buffer.position() <= limit) {
            processEvent(buffer, result);
        }

        return result;
    }

    private static void processEvent(final ByteBuffer buffer, final PartialResult map) {
        final long hash = parseId(buffer);
        final int value = readValue(buffer);

        map.recordResult(hash, value);
    }

    private static long parseId(final ByteBuffer buffer) {
        long ret = 0;
        final byte[] name = THREAD_LOCAL_NAME_BUFFER.get();
        int i = 0;
        for (byte b = buffer.get(); b != ';'; b = buffer.get(), ++i) {
            writeByte(name, i, b);
            ret = 31 * ret + b;
        }
        name[i] = '\0';

        return ret;
    }

    private static void writeByte(final byte[] name, final int i, final byte b) {
        name[i] = b;
    }

    private static long parseId(final byte[] name) {
        long ret = 0;
        for (int i = 0; i < name.length && name[i] != '\0'; ++i) {
            final byte b = name[i];
            ret = 31 * ret + b;
        }

        return ret;
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

    private static class PartialResult {
        private int nextSlot = 0;
        final LongToIntMap idToSlotMapping = new LongToIntMap(MAP_MISSING_VALUE);

        private final long[] maximums = new long[MAXIMUM_CITIES];
        private final long[] minimums = new long[MAXIMUM_CITIES];
        private final long[] sums = new long[MAXIMUM_CITIES];
        private final long[] counts = new long[MAXIMUM_CITIES];

        private final byte[][] names = new byte[MAXIMUM_CITIES][];

        private PartialResult() {
            Arrays.fill(maximums, Long.MIN_VALUE);
            Arrays.fill(minimums, Long.MAX_VALUE);
        }

        private void recordResult(final long id, final int value) {
            int slot = idToSlotMapping.get(id);
            if (slot == MAP_MISSING_VALUE) {
                names[nextSlot] = THREAD_LOCAL_NAME_BUFFER.get();
                THREAD_LOCAL_NAME_BUFFER.set(new byte[101]);
                slot = nextSlot++;
                idToSlotMapping.put(id, slot);
            }

            maximums[slot] = Math.max(maximums[slot], value);
            minimums[slot] = Math.min(minimums[slot], value);
            sums[slot] += value;
            counts[slot]++;
        }

        private void forEach(final ResultConsumer consumer) {
            for (int i = 0; i < LongToIntMap.MAP_SIZE; ++i) {
                final int slot = idToSlotMapping.values[i];

                if (slot == idToSlotMapping.missingValue) {
                    continue;
                }

                consumer.consume(
                        names[slot],
                        maximums[slot],
                        minimums[slot],
                        sums[slot],
                        counts[slot]);
            }
        }

        private void mergeBatch(
                                final long id,
                                final long max,
                                final long min,
                                final long sum,
                                final long count,
                                final byte[] name) {
            int slot = idToSlotMapping.get(id);
            if (slot == MAP_MISSING_VALUE) {
                names[nextSlot] = name;
                slot = nextSlot++;
                idToSlotMapping.put(id, slot);
            }

            maximums[slot] = Math.max(maximums[slot], max);
            minimums[slot] = Math.min(minimums[slot], min);

            sums[slot] += sum;
            counts[slot] += count;
        }

        private void merge(final PartialResult other) {
            for (int s = 0; s < other.nextSlot; ++s) {
                final long id = parseId(other.names[s]);
                mergeBatch(id, other.maximums[s], other.minimums[s], other.sums[s], other.counts[s], other.names[s]);
            }
        }
    }

    private static String bytesToString(final byte[] name) {
        int l = 0;
        for (; l < name.length; ++l) {
            if (name[l] == '\0') {
                break;
            }
        }

        return new String(name, 0, l, UTF_8);
    }

    private interface ResultConsumer {
        void consume(final byte[] name, final long max, final long min, final long sum, final long count);
    }

    private static class LongToIntMap {
        private static final int MAP_SIZE = 16384;
        private static final int MASK = MAP_SIZE - 1;
        final long[] keys = new long[MAP_SIZE];
        final int[] values = new int[MAP_SIZE];
        private final int missingValue;

        LongToIntMap(final int missingValue) {
            if (Integer.bitCount(MAP_SIZE) != 1) {
                throw new RuntimeException("blah");
            }

            this.missingValue = missingValue;
            Arrays.fill(values, missingValue);
        }

        public void put(final long key, final int value) {
            for (int s = mask(key); !tryInsert(s, key, value); s = mask(++s)) {
            }
        }

        private int mask(final long key) {
            return (int) (key & MASK);
        }

        private boolean tryInsert(final int slot, final long key, final int value) {
            final long currentValue = values[slot];
            if (currentValue == MAP_MISSING_VALUE) {
                keys[slot] = key;
                values[slot] = value;
                return true;
            }
            return false;
        }

        public int get(final long key) {
            int slot = mask(key);
            for (; !(keys[slot] == key || values[slot] == missingValue); slot = mask(++slot)) {
            }
            return values[slot];
        }

    }
}
