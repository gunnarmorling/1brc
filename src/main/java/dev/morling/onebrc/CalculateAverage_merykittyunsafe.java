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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_merykittyunsafe {
    private static final String FILE = "./measurements.txt";
    private static final Unsafe UNSAFE;
    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 32
            ? ByteVector.SPECIES_256
            : ByteVector.SPECIES_128;
    private static final long KEY_MAX_SIZE = 100;

    private static class Aggregator {
        private long min = Integer.MAX_VALUE;
        private long max = Integer.MIN_VALUE;
        private long sum;
        private long count;

        public String toString() {
            return round(min / 10.) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    // An open-address map that is specialized for this task
    private static class PoorManMap {
        // 100-byte key + 4-byte hash + 4-byte size +
        // 2-byte min + 2-byte max + 8-byte sum + 8-byte count
        private static final int ENTRY_SIZE = 128;
        private static final int SIZE_OFFSET = 0;
        private static final int MIN_OFFSET = 4;
        private static final int MAX_OFFSET = 6;
        private static final int SUM_OFFSET = 8;
        private static final int COUNT_OFFSET = 16;
        private static final int KEY_OFFSET = 24;

        // There is an assumption that map size <= 10000;
        private static final int CAPACITY = 1 << 17;
        private static final int ENTRY_MASK = ENTRY_SIZE * CAPACITY - 1;

        final byte[] data;

        PoorManMap() {
            this.data = new byte[CAPACITY * ENTRY_SIZE];
        }

        void observe(long entryOffset, long value) {
            long baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + entryOffset;
            if (UNSAFE.getShort(this.data, baseOffset + MIN_OFFSET) > value) {
                UNSAFE.putShort(this.data, baseOffset + MIN_OFFSET, (short) value);
            }
            if (UNSAFE.getShort(this.data, baseOffset + MAX_OFFSET) < value) {
                UNSAFE.putShort(this.data, baseOffset + MAX_OFFSET, (short) value);
            }
            UNSAFE.putLong(this.data, baseOffset + SUM_OFFSET,
                    value + UNSAFE.getLong(this.data, baseOffset + SUM_OFFSET));
            UNSAFE.putLong(this.data, baseOffset + COUNT_OFFSET,
                    1 + UNSAFE.getLong(this.data, baseOffset + COUNT_OFFSET));
        }

        long indexSimple(long address, int size) {
            int x;
            int y;
            if (size >= Integer.BYTES) {
                x = UNSAFE.getInt(address);
                y = UNSAFE.getInt(address + size - Integer.BYTES);
            }
            else {
                x = UNSAFE.getByte(address);
                y = UNSAFE.getByte(address + size - Byte.BYTES);
            }
            int hash = hash(x, y);
            long entryOffset = (hash * ENTRY_SIZE) & ENTRY_MASK;
            for (;; entryOffset = (entryOffset + ENTRY_SIZE) & ENTRY_MASK) {
                int nodeSize = UNSAFE.getInt(this.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + entryOffset + SIZE_OFFSET);
                if (nodeSize == 0) {
                    insertInto(entryOffset, address, size);
                    return entryOffset;
                }
                else if (keyEqualScalar(entryOffset, address, size)) {
                    return entryOffset;
                }
            }
        }

        void insertInto(long entryOffset, long address, int size) {
            long baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + entryOffset;
            UNSAFE.putInt(this.data, baseOffset + SIZE_OFFSET, size);
            UNSAFE.putShort(this.data, baseOffset + MIN_OFFSET, Short.MAX_VALUE);
            UNSAFE.putShort(this.data, baseOffset + MAX_OFFSET, Short.MIN_VALUE);
            try (var arena = Arena.ofConfined()) {
                var segment = MemorySegment.ofAddress(address)
                        .reinterpret(size + 1, arena, null);
                MemorySegment.copy(segment, 0, MemorySegment.ofArray(this.data), entryOffset + KEY_OFFSET, size + 1);
            }
        }

        void mergeInto(Map<String, Aggregator> target) {
            for (int entryOffset = 0; entryOffset < data.length; entryOffset += ENTRY_SIZE) {
                long baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + entryOffset;
                int size = UNSAFE.getInt(this.data, baseOffset + SIZE_OFFSET);
                if (size == 0) {
                    continue;
                }

                String key = new String(this.data, entryOffset + KEY_OFFSET, size, StandardCharsets.UTF_8);
                target.compute(key, (k, v) -> {
                    if (v == null) {
                        v = new Aggregator();
                    }

                    v.min = Math.min(v.min, UNSAFE.getShort(this.data, baseOffset + MIN_OFFSET));
                    v.max = Math.max(v.max, UNSAFE.getShort(this.data, baseOffset + MAX_OFFSET));
                    v.sum += UNSAFE.getLong(this.data, baseOffset + SUM_OFFSET);
                    v.count += UNSAFE.getLong(this.data, baseOffset + COUNT_OFFSET);
                    return v;
                });
            }
        }

        static int hash(int x, int y) {
            int seed = 0x9E3779B9;
            int rotate = 5;
            return (Integer.rotateLeft(x * seed, rotate) ^ y) * seed; // FxHash
        }

        private boolean keyEqualScalar(long entryOffset, long address, int size) {
            long baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + entryOffset;
            if (UNSAFE.getInt(this.data, baseOffset + SIZE_OFFSET) != size) {
                return false;
            }

            // Be simple
            for (long i = 0; i < size; i++) {
                int c1 = UNSAFE.getByte(this.data, baseOffset + KEY_OFFSET + i);
                int c2 = UNSAFE.getByte(address + i);
                if (c1 != c2) {
                    return false;
                }
            }
            return true;
        }
    }

    // Parse a number that may/may not contain a minus sign followed by a decimal with
    // 1 - 2 digits to the left and 1 digits to the right of the separator to a
    // fix-precision format. It returns the offset of the next line (presumably followed
    // the final digit and a '\n')
    private static long parseDataPoint(PoorManMap aggrMap, long entryOffset, long address) {
        long word = UNSAFE.getLong(address);
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
            word = Long.reverseBytes(word);
        }
        // The 4th binary digit of the ascii of a digit is 1 while
        // that of the '.' is 0. This finds the decimal separator
        // The value can be 12, 20, 28
        int decimalSepPos = Long.numberOfTrailingZeros(~word & 0x10101000);
        int shift = 28 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~word << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii code
        // to actual digit value in each byte
        long digits = ((word & designMask) << shift) & 0x0F000F0F00L;

        // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
        // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
        // 0x000000UU00TTHH00 +
        // 0x00UU00TTHH000000 * 10 +
        // 0xUU00TTHH00000000 * 100
        // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
        // This results in our value lies in the bit 32 to 41 of this product
        // That was close :)
        long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        long value = (absValue ^ signed) - signed;
        aggrMap.observe(entryOffset, value);
        return address + (decimalSepPos >>> 3) + 3;
    }

    // Tail processing version of the above, do not over-fetch and be simple
    private static long parseDataPointSimple(PoorManMap aggrMap, long entryOffset, long address) {
        int value = 0;
        boolean negative = false;
        if (UNSAFE.getByte(address) == '-') {
            negative = true;
            address++;
        }
        for (;; address++) {
            int c = UNSAFE.getByte(address);
            if (c == '.') {
                c = UNSAFE.getByte(address + 1);
                value = value * 10 + (c - '0');
                address += 3;
                break;
            }

            value = value * 10 + (c - '0');
        }
        value = negative ? -value : value;
        aggrMap.observe(entryOffset, value);
        return address;
    }

    // An iteration of the main parse loop, parse a line starting from offset.
    // This requires offset to be the start of the line and there is spare space so
    // that we have relative freedom in processing
    // It returns the offset of the next line that it needs processing
    private static long iterate(PoorManMap aggrMap, long address) {
        ByteVector line;
        try (var arena = Arena.ofConfined()) {
            var segment = MemorySegment.ofAddress(address)
                    .reinterpret(BYTE_SPECIES.vectorByteSize(), arena, null);
            line = ByteVector.fromMemorySegment(BYTE_SPECIES, segment, 0, ByteOrder.nativeOrder());
        }

        // Find the delimiter ';'
        long semicolons = line.compare(VectorOperators.EQ, ';').toLong();

        // If we cannot find the delimiter in the vector, that means the key is
        // longer than the vector, fall back to scalar processing
        if (semicolons == 0) {
            int keySize = BYTE_SPECIES.length();
            while (UNSAFE.getByte(address + keySize) != ';') {
                keySize++;
            }
            var node = aggrMap.indexSimple(address, keySize);
            return parseDataPoint(aggrMap, node, address + 1 + keySize);
        }

        // We inline the searching of the value in the hash map
        int keySize = Long.numberOfTrailingZeros(semicolons);
        int x;
        int y;
        if (keySize >= Integer.BYTES) {
            x = UNSAFE.getInt(address);
            y = UNSAFE.getInt(address + keySize - Integer.BYTES);
        }
        else {
            x = UNSAFE.getByte(address);
            y = UNSAFE.getByte(address + keySize - Byte.BYTES);
        }
        int hash = PoorManMap.hash(x, y);
        long entryOffset = (hash * PoorManMap.ENTRY_SIZE) & PoorManMap.ENTRY_MASK;
        for (;; entryOffset = (entryOffset + PoorManMap.ENTRY_SIZE) & PoorManMap.ENTRY_MASK) {
            var nodeSize = UNSAFE.getInt(aggrMap.data, Unsafe.ARRAY_BYTE_BASE_OFFSET
                    + entryOffset + PoorManMap.SIZE_OFFSET);
            if (nodeSize == 0) {
                aggrMap.insertInto(entryOffset, address, keySize);
                break;
            }

            if (nodeSize != keySize) {
                continue;
            }

            var nodeKey = ByteVector.fromArray(BYTE_SPECIES, aggrMap.data, (int) (entryOffset + PoorManMap.KEY_OFFSET));
            long eqMask = line.compare(VectorOperators.EQ, nodeKey).toLong();
            long validMask = semicolons ^ (semicolons - 1);
            if ((eqMask & validMask) == validMask) {
                break;
            }
        }

        return parseDataPoint(aggrMap, entryOffset, address + keySize + 1);
    }

    private static long findOffset(long base, long offset, long limit) {
        if (offset == 0) {
            return offset;
        }

        offset--;
        while (offset < limit) {
            if (UNSAFE.getByte(base + (offset++)) == '\n') {
                break;
            }
        }
        return offset;
    }

    // Process all lines that start in [offset, limit)
    private static PoorManMap processFile(MemorySegment data, long offset, long limit) {
        var aggrMap = new PoorManMap();
        if (offset == limit) {
            return aggrMap;
        }
        long base = data.address();
        int batches = 2;
        long batchSize = Math.ceilDiv(limit - offset, batches);
        long offset0 = offset;
        long offset1 = offset + batchSize;
        long limit0 = Math.min(offset1, limit);
        long limit1 = limit;

        // Find the start of a new line
        offset0 = findOffset(base, offset0, limit0);
        offset1 = findOffset(base, offset1, limit1);

        long begin;
        long end = base + limit;
        long mainLoopMinWidth = Math.max(BYTE_SPECIES.vectorByteSize(), KEY_MAX_SIZE + 1 + Long.BYTES);
        if (limit1 - offset1 < mainLoopMinWidth) {
            begin = base + findOffset(base, offset, limit);
            while (begin < end - mainLoopMinWidth) {
                begin = iterate(aggrMap, begin);
            }
        }
        else {
            long begin0 = base + offset0;
            long begin1 = base + offset1;
            long end0 = base + limit0;
            long end1 = base + limit1;
            while (true) {
                boolean finish = false;
                if (begin0 < end0) {
                    begin0 = iterate(aggrMap, begin0);
                }
                else {
                    finish = true;
                }
                if (begin1 < end1 - mainLoopMinWidth) {
                    begin1 = iterate(aggrMap, begin1);
                }
                else {
                    if (finish) {
                        break;
                    }
                }
            }
            begin = begin1;
        }

        // Now we are at the tail, just be simple
        while (begin < end) {
            int keySize = 0;
            while (UNSAFE.getByte(begin + keySize) != ';') {
                keySize++;
            }
            long entryOffset = aggrMap.indexSimple(begin, keySize);
            begin = parseDataPointSimple(aggrMap, entryOffset, begin + 1 + keySize);
        }

        return aggrMap;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        int processorCnt = Runtime.getRuntime().availableProcessors();
        var res = new TreeMap<String, Aggregator>();
        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
                var arena = Arena.ofShared()) {
            var data = file.map(MapMode.READ_ONLY, 0, file.size(), arena);
            long chunkSize = Math.ceilDiv(data.byteSize(), processorCnt);
            var threadList = new Thread[processorCnt];
            var resultList = new PoorManMap[processorCnt];
            for (int i = 0; i < processorCnt; i++) {
                int index = i;
                long offset = i * chunkSize;
                long limit = Math.min((i + 1) * chunkSize, data.byteSize());
                var thread = new Thread(() -> resultList[index] = processFile(data, offset, limit));
                threadList[index] = thread;
                thread.start();
            }
            for (var thread : threadList) {
                thread.join();
            }

            // Collect the results
            for (var aggrMap : resultList) {
                aggrMap.mergeInto(res);
            }
        }

        System.out.println(res);
    }
}
