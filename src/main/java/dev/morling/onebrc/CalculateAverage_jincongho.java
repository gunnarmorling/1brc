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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Changelog (based on Macbook Pro Intel i7 6-cores 2.6GHz):
 *
 * Initial                          40000 ms
 * Parse key as byte vs string      30000 ms
 * Parse temp as fixed vs double    15000 ms
 * HashMap optimization             10000 ms
 *
 */
public class CalculateAverage_jincongho {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Measurement Hash Table (for each partition)
     * Uses contiguous byte array to optimize for cache-line (hopefully)
     *
     * Each entry:
     * - KEYS: keyLength (2 bytes) + key (100 bytes)
     * - VALUES: min (2 bytes) + max (2 bytes) + count (4 bytes) + sum ( 8 bytes)
     */
    protected static class PartitionAggr {

        private static int MAP_SIZE = 1 << 14; // 2^14 = 16384, closes to 10000
        private static int KEY_SIZE = 128; // key length (2 bytes) + key (100 bytes)
        private static int KEY_MASK = (MAP_SIZE - 1);
        private static int VALUE_SIZE = 16; // min (2 bytes) + max ( 2 bytes) + count (4 bytes) + sum (8 bytes)

        private byte[] KEYS = new byte[MAP_SIZE * KEY_SIZE];
        private byte[] VALUES = new byte[MAP_SIZE * VALUE_SIZE];

        public PartitionAggr() {
            // init min and max
            for (int offset = UNSAFE.ARRAY_BYTE_BASE_OFFSET; offset < UNSAFE.ARRAY_BYTE_BASE_OFFSET + (MAP_SIZE * VALUE_SIZE); offset += VALUE_SIZE) {
                UNSAFE.putShort(VALUES, offset, Short.MAX_VALUE);
                UNSAFE.putShort(VALUES, offset + 2, Short.MIN_VALUE);
            }
        }

        public void update(byte[] key, int hash, short keyLength, short value) {
            int index = hash & KEY_MASK;
            int keyOffset = UNSAFE.ARRAY_BYTE_BASE_OFFSET + (index * KEY_SIZE);
            while (((UNSAFE.getShort(KEYS, keyOffset) != keyLength) ||
                    !equals(KEYS, ((index * KEY_SIZE) + 2), key, 0, keyLength))) {
                if (UNSAFE.getShort(KEYS, keyOffset) == 0) {
                    // put key
                    UNSAFE.putShort(KEYS, keyOffset, keyLength);
                    UNSAFE.copyMemory(key, UNSAFE.ARRAY_BYTE_BASE_OFFSET, KEYS, keyOffset + 2, keyLength);
                    break;
                }
                else {
                    index = (index + 1) & KEY_MASK;
                    keyOffset += KEY_SIZE;
                }
            }

            long valueOffset = UNSAFE.ARRAY_BYTE_BASE_OFFSET + (index * VALUE_SIZE);
            UNSAFE.putShort(VALUES, valueOffset, (short) Math.min(UNSAFE.getShort(VALUES, valueOffset), value));
            valueOffset += 2;
            UNSAFE.putShort(VALUES, valueOffset, (short) Math.max(UNSAFE.getShort(VALUES, valueOffset), value));
            valueOffset += 2;
            UNSAFE.putInt(VALUES, valueOffset, UNSAFE.getInt(VALUES, valueOffset) + 1);
            valueOffset += 4;
            UNSAFE.putLong(VALUES, valueOffset, UNSAFE.getLong(VALUES, valueOffset) + value);
        }

        private boolean equals(byte[] a, int aOffset, byte[] b, int bOffset, int len) {
            while (bOffset < len)
                if (a[aOffset++] != b[bOffset++])
                    return false;
            return true;
        }

        public void mergeTo(ResultAggr result) {
            long keyOffset;
            short keyLength;
            for (int i = 0; i < MAP_SIZE; i++) {
                // extract key
                keyOffset = UNSAFE.ARRAY_BYTE_BASE_OFFSET + (i * KEY_SIZE);
                if ((keyLength = UNSAFE.getShort(KEYS, keyOffset)) == 0)
                    continue;

                // extract values (if key is not null)
                final long valueOffset = UNSAFE.ARRAY_BYTE_BASE_OFFSET + (i * VALUE_SIZE);
                result.compute(new String(KEYS, (i * KEY_SIZE) + 2, keyLength, StandardCharsets.UTF_8), (k, v) -> {
                    short min = UNSAFE.getShort(VALUES, valueOffset);
                    short max = UNSAFE.getShort(VALUES, valueOffset + 2);
                    int count = UNSAFE.getInt(VALUES, valueOffset + 4);
                    long sum = UNSAFE.getLong(VALUES, valueOffset + 8);

                    if (v == null) {
                        return new ResultAggr.Measurement(min, max, count, sum);
                    }
                    else {
                        return v.update(min, max, count, sum);
                    }
                });
            }
        }

    }

    /**
     * Measurement Aggregation (for all partitions)
     * Simple Concurrent Hash Table so all partitions can merge concurrently
     */
    protected static class ResultAggr extends ConcurrentHashMap<String, ResultAggr.Measurement> {

        protected static class Measurement {
            public short min;
            public short max;
            public int count;
            public long sum;

            public Measurement(short min, short max, int count, long sum) {
                this.min = min;
                this.max = max;
                this.count = count;
                this.sum = sum;
            }

            public ResultAggr.Measurement update(short min, short max, int count, long sum) {
                this.min = (short) Math.min(min, this.min);
                this.max = (short) Math.max(max, this.max);
                this.count += count;
                this.sum += sum;

                return this;
            }

            @Override
            public String toString() {
                return ((double) min / 10) + "/" + (Math.round((1.0 * sum) / count) / 10.0) + "/" + ((double) max / 10);
            }

        }

        public Map toSorted() {
            return new TreeMap(this);
        }

    }

    protected static class Partition implements Runnable {

        private final MemorySegment data;
        private long offset;
        private final long limit;
        private final ResultAggr result;

        public Partition(MemorySegment data, long offset, long limit, ResultAggr result) {
            this.data = data;
            this.offset = data.address() + offset;
            this.limit = data.address() + limit;
            this.result = result;
        }

        @Override
        public void run() {
            // measurement parsing
            PartitionAggr aggr = new PartitionAggr();
            byte[] stationName = new byte[128];
            short stationLength;
            int hash;
            byte tempBuffer;
            while (offset < limit) {
                // find station name upto ";"
                hash = 1;
                stationLength = 0;
                while ((stationName[stationLength] = UNSAFE.getByte(offset++)) != ';')
                    hash = hash * 31 + stationName[stationLength++];

                // find measurement upto "\n"
                tempBuffer = UNSAFE.getByte(offset++);
                boolean isNegative = (tempBuffer == '-');
                short fixed = (short) (isNegative ? 0 : (tempBuffer - '0'));
                while (true) {
                    tempBuffer = UNSAFE.getByte(offset++);
                    if (tempBuffer == '.') {
                        fixed = (short) (fixed * 10 + (UNSAFE.getByte(offset) - '0'));
                        offset += 2;
                        break;
                    }
                    fixed = (short) (fixed * 10 + (tempBuffer - '0'));
                }
                fixed = isNegative ? (short) -fixed : fixed;

                // update measurement
                aggr.update(stationName, hash, stationLength, fixed);
            }

            // measurement result collection
            aggr.mergeTo(result);
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        // long startTime = System.currentTimeMillis();

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), EnumSet.of(StandardOpenOption.READ));
                Arena arena = Arena.ofShared()) {

            // scan data
            MemorySegment data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
            final int processors = Runtime.getRuntime().availableProcessors();

            // partition split
            long[] partition = new long[processors + 1];
            long partitionSize = Math.ceilDiv(data.byteSize(), processors);
            for (int i = 0; i < processors; i++) {
                partition[i + 1] = partition[i] + partitionSize;
                if (partition[i + 1] >= data.byteSize()) {
                    partition[i + 1] = data.byteSize();
                    break;
                }
                while (UNSAFE.getByte(data.address() + partition[i + 1]++) != '\n')
                    ;
            }

            // partition aggregation
            var threadList = new Thread[processors];
            ResultAggr result = new ResultAggr();
            for (int i = 0; i < processors; i++) {
                threadList[i] = new Thread(new Partition(data, partition[i], partition[i + 1], result));
                threadList[i].start();
            }
            for (var thread : threadList) {
                thread.join();
            }

            System.out.println(result.toSorted());
        }

        // long elapsed = System.currentTimeMillis() - startTime;
        // System.out.println("Elapsed: " + ((double) elapsed / 1000.0));

    }

}
