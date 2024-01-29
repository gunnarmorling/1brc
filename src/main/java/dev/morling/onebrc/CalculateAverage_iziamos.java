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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;

public class CalculateAverage_iziamos {
    private static final Unsafe UNSAFE;

    private static final String FILE = "./measurements.txt";
    private static final Arena GLOBAL_ARENA = Arena.global();
    private final static MemorySegment WHOLE_FILE_SEGMENT;
    private final static long FILE_SIZE;
    private final static long BASE_POINTER;
    private final static long END_POINTER;

    static {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(Unsafe.class);

            final var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), READ);
            WHOLE_FILE_SEGMENT = fileChannel.map(READ_ONLY, 0, fileChannel.size(), GLOBAL_ARENA);

        }
        catch (final NoSuchFieldException | IllegalAccessException | IOException e) {
            throw new RuntimeException(e);
        }

        FILE_SIZE = WHOLE_FILE_SEGMENT.byteSize();
        BASE_POINTER = WHOLE_FILE_SEGMENT.address();
        END_POINTER = BASE_POINTER + FILE_SIZE;
    }

    private static final long CHUNK_SIZE = 64 * 1024 * 1024;
    // private static final long CHUNK_SIZE = Long.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        // Thread.sleep(10_000);

        final long threadCount = 1 + FILE_SIZE / CHUNK_SIZE;

        final var processingFutures = new CompletableFuture[(int) threadCount];
        for (int i = 0; i < threadCount; ++i) {
            processingFutures[i] = processSegment(i, CHUNK_SIZE);
        }

        final long aggregate = (long) processingFutures[0].get();
        for (int i = 1; i < processingFutures.length; i++) {
            final long r = (long) processingFutures[i].get();
            ByteBackedResultSet.merge(aggregate, r);
        }

        final Map<String, ResultRow> output = new TreeMap<>();
        ByteBackedResultSet.forEach(aggregate,
                (name, min, max, sum, count) -> output.put(name, new ResultRow(min, (double) sum / count, max)));

        System.out.println(output);
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

    private static CompletableFuture<Long> processSegment(final long chunkNumber, final long chunkSize) {
        final var ret = new CompletableFuture<Long>();

        Thread.ofVirtual().start(() -> {
            final long relativeStart = chunkNumber * chunkSize;
            final long absoluteStart = BASE_POINTER + relativeStart;

            final long absoluteEnd = computeAbsoluteEndWithSlack(absoluteStart + chunkSize);
            final long startOffsetAfterSkipping = skipIncomplete(WHOLE_FILE_SEGMENT.address(), absoluteStart);

            final long result = processEvents(startOffsetAfterSkipping, absoluteEnd);
            ret.complete(result);
        });

        return ret;
    }

    private static long computeAbsoluteEndWithSlack(final long chunk) {
        return Long.compareUnsigned(END_POINTER, chunk) > 0 ? chunk : END_POINTER;
    }

    private static long skipIncomplete(final long basePointer, final long start) {
        if (start == basePointer) {
            return start;
        }
        for (long i = 0;; ++i) {
            final byte b = UNSAFE.getByte(start + i);
            if (b == '\n') {
                return start + i + 1;
            }
        }
    }

    private static long processEvents(final long start, final long limit) {
        final long result = ByteBackedResultSet.createResultSet();
        scalarLoop(start, limit, result);
        return result;
    }

    private static void scalarLoop(final long start, final long limit, final long result) {
        final LoopCursor cursor = new ScalarLoopCursor(start, limit);
        while (cursor.hasMore()) {
            final long address = cursor.getCurrentAddress();
            final int length = cursor.getStringLength();
            final int hash = cursor.getHash();
            final int value = cursor.getCurrentValue();
            ByteBackedResultSet.put(result, address, length, hash, value);
        }
    }

    public interface LoopCursor {
        long getCurrentAddress();

        int getStringLength();

        int getHash();

        int getCurrentValue();

        boolean hasMore();
    }

    public static class ScalarLoopCursor implements LoopCursor {
        private long pointer;
        private final long limit;

        private int hash = 0;

        public ScalarLoopCursor(final long pointer, final long limit) {
            this.pointer = pointer;
            this.limit = limit;
        }

        public long getCurrentAddress() {
            return pointer;
        }

        public int getStringLength() {
            int strLen = 0;
            hash = 0;

            byte b = UNSAFE.getByte(pointer);
            for (; b != ';'; ++strLen, b = UNSAFE.getByte(pointer + strLen)) {
                hash = 31 * hash + b;
            }
            pointer += strLen + 1;

            return strLen;
        }

        public int getHash() {
            return hash;
        }

        public int getCurrentValue() {
            return getCurrentValueMeryKitty();
        }

        /**
         * No point rewriting what would essentially be the same code <3.
         */
        public int getCurrentValueMeryKitty() {
            long word = UNSAFE.getLong(pointer);
            if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                word = Long.reverseBytes(word);
            }

            int decimalSepPos = Long.numberOfTrailingZeros(~word & 0x10101000);
            int shift = 28 - decimalSepPos;

            long signed = (~word << 59) >> 63;
            long designMask = ~(signed & 0xFF);

            long digits = ((word & designMask) << shift) & 0x0F000F0F00L;

            long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            int increment = (decimalSepPos >>> 3) + 3;

            pointer += increment;
            return (int) ((absValue ^ signed) - signed);
        }

        public boolean hasMore() {
            return pointer < limit;
        }
    }

    public interface ResultConsumer {
        void consume(final String name, final int min, final int max, final long sum, final long count);
    }

    static class ByteBackedResultSet {
        private static final int MAP_SIZE = 16384 * 4;
        private static final int MASK = MAP_SIZE - 1;
        private static final long STRUCT_SIZE = 64;
        private static final long BYTE_SIZE = MAP_SIZE * STRUCT_SIZE;
        private static final long STRING_OFFSET = 0;
        private static final long STRING_LEN_OFFSET = 8;
        private static final long HASH_OFFSET = 12;
        private static final long MIN_OFFSET = 16;
        private static final long MAX_OFFSET = 20;
        private static final long SUM_OFFSET = 24;
        private static final long COUNT_OFFSET = 32;

        public static long createResultSet() {
            final long baseAddress = UNSAFE.allocateMemory(BYTE_SIZE);
            UNSAFE.setMemory(baseAddress, BYTE_SIZE, (byte) 0);
            return baseAddress;
        }

        public static void put(final long baseAddress, final long address, final int length, final int hash, final int value) {
            final long slot = findSlot(baseAddress, hash, address, length);
            final long structBase = baseAddress + (slot * STRUCT_SIZE);

            final int min = UNSAFE.getInt(structBase + MIN_OFFSET);
            final int max = UNSAFE.getInt(structBase + MAX_OFFSET);
            final long sum = UNSAFE.getLong(structBase + SUM_OFFSET);
            final long count = UNSAFE.getLong(structBase + COUNT_OFFSET);

            UNSAFE.putLong(structBase, address);
            UNSAFE.putInt(structBase + STRING_LEN_OFFSET, length);
            UNSAFE.putInt(structBase + HASH_OFFSET, hash);

            UNSAFE.putInt(structBase + MIN_OFFSET, Math.min(value, min));
            UNSAFE.putInt(structBase + MAX_OFFSET, Math.max(value, max));
            UNSAFE.putLong(structBase + SUM_OFFSET, sum + value);
            UNSAFE.putLong(structBase + COUNT_OFFSET, count + 1);
        }

        public static void forEach(final long baseAddress, final ResultConsumer resultConsumer) {
            for (long i = 0; i < BYTE_SIZE; i += STRUCT_SIZE) {
                final long structBase = baseAddress + i;
                final long stringBase = UNSAFE.getLong(structBase);
                if (stringBase == 0) {
                    continue;
                }

                final int min = UNSAFE.getInt(structBase + MIN_OFFSET);
                final int max = UNSAFE.getInt(structBase + MAX_OFFSET);
                final long sum = UNSAFE.getLong(structBase + SUM_OFFSET);
                final long count = UNSAFE.getLong(structBase + COUNT_OFFSET);

                final int strLen = UNSAFE.getInt(structBase + STRING_LEN_OFFSET);
                final byte[] bytes = new byte[strLen];
                for (int j = 0; j < strLen; ++j) {
                    bytes[j] = UNSAFE.getByte(stringBase + j);
                }

                resultConsumer.consume(new String(bytes, UTF_8), min, max, sum, count);
            }
        }

        public static void merge(final long baseAddress, final long other) {
            for (long i = 0; i < BYTE_SIZE; i += STRUCT_SIZE) {
                final long otherStructBase = other + i;
                if (UNSAFE.getLong(otherStructBase) == 0) {
                    continue;
                }

                final long otherStringStart = UNSAFE.getLong(otherStructBase);
                final int otherStringLength = UNSAFE.getInt(otherStructBase + STRING_LEN_OFFSET);
                final int otherStringHash = UNSAFE.getInt(otherStructBase + HASH_OFFSET);

                final long slot = findSlot(baseAddress, otherStringHash, otherStringStart, otherStringLength);

                final long thisStructBase = baseAddress + (slot * STRUCT_SIZE);

                final int min = UNSAFE.getInt(thisStructBase + MIN_OFFSET);
                final int max = UNSAFE.getInt(thisStructBase + MAX_OFFSET);
                final long sum = UNSAFE.getLong(thisStructBase + SUM_OFFSET);
                final long count = UNSAFE.getLong(thisStructBase + COUNT_OFFSET);

                final int otherMin = UNSAFE.getInt(otherStructBase + MIN_OFFSET);
                final int otherMax = UNSAFE.getInt(otherStructBase + MAX_OFFSET);
                final long otherSum = UNSAFE.getLong(otherStructBase + SUM_OFFSET);
                final long otherCount = UNSAFE.getLong(otherStructBase + COUNT_OFFSET);

                UNSAFE.putLong(thisStructBase, otherStringStart);
                UNSAFE.putInt(thisStructBase + STRING_LEN_OFFSET, otherStringLength);
                UNSAFE.putInt(thisStructBase + HASH_OFFSET, otherStringHash);

                UNSAFE.putInt(thisStructBase + MIN_OFFSET, Math.min(otherMin, min));
                UNSAFE.putInt(thisStructBase + MAX_OFFSET, Math.max(otherMax, max));
                UNSAFE.putLong(thisStructBase + SUM_OFFSET, sum + otherSum);
                UNSAFE.putLong(thisStructBase + COUNT_OFFSET, count + otherCount);
            }
        }

        private static int findSlot(final long baseAddress,
                                    final int hash,
                                    final long otherStringAddress,
                                    final int otherStringLength) {

            for (int slot = mask(hash);; slot = mask(++slot)) {
                final long structBase = baseAddress + ((long) slot * STRUCT_SIZE);
                final long nameStart = UNSAFE.getLong(structBase);
                if (nameStart == 0) {
                    UNSAFE.putInt(structBase + MIN_OFFSET, Integer.MAX_VALUE);
                    UNSAFE.putInt(structBase + MAX_OFFSET, Integer.MIN_VALUE);
                    return slot;
                }

                final int nameLength = UNSAFE.getInt(structBase + STRING_LEN_OFFSET);
                if (stringEquals(nameStart, nameLength, otherStringAddress, otherStringLength)) {
                    return slot;
                }
            }
        }

        private static boolean stringEquals(final long thisNameAddress,
                                            final int thisStringLength,
                                            final long otherNameAddress,
                                            final long otherNameLength) {
            if (thisStringLength != otherNameLength) {
                return false;
            }

            int i = 0;
            for (; i < thisStringLength - 7; i += 8) {
                if (UNSAFE.getLong(thisNameAddress + i) != UNSAFE.getLong(otherNameAddress + i)) {
                    return false;
                }
            }

            final long remainingToCheck = thisStringLength - i;
            final long finalBytesMask = ((1L << remainingToCheck * 8)) - 1;
            final long thisLastWord = UNSAFE.getLong(thisNameAddress + i);
            final long otherLastWord = UNSAFE.getLong(otherNameAddress + i);

            return 0 == ((thisLastWord ^ otherLastWord) & finalBytesMask);
        }

        public static int mask(final int value) {
            return MASK & value;
        }
    }
}
