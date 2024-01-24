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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Changelog (based on Macbook Pro Intel i7 6-cores 2.6GHz):
 *
 * Initial                          40000 ms
 * Parse key as byte vs string      30000 ms
 * Parse temp as fixed vs double    15000 ms
 * HashMap optimization             10000 ms
 * Simd + reduce memory copy         8000 ms
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
     * Vectorization utilities with 1BRC-specific optimizations
     */
    protected static class VectorUtils {

        // key length is usually less than 32 bytes, having more is just expensive
        public static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;

        /** Vectorized field delimiter search **/

        public static int findDelimiter(MemorySegment data, long offset) {
            return ByteVector.fromMemorySegment(VectorUtils.BYTE_SPECIES, data, offset, ByteOrder.nativeOrder())
                    .compare(VectorOperators.EQ, ';')
                    .firstTrue();
        }

        /** Vectorized Hashing (explicit vectorization seems slower, overkill?) **/

        // private static int[] HASH_ARRAY = initHashArray();
        // private static final IntVector HASH_VECTOR = IntVector.fromArray(IntVector.SPECIES_256, HASH_ARRAY, 0);
        // private static final int HASH_ACCUM = HASH_ARRAY[0] * 31;
        //
        // private static int[] initHashArray() {
        // int[] x = new int[IntVector.SPECIES_256.length()];
        // x[x.length - 1] = 1;
        // for (int i = x.length - 2; i >= 0; i--)
        // x[i] = x[i + 1] * 31;
        //
        // return x;
        // }

        /**
         * Ref: https://github.com/PaulSandoz/vector-api-dev-live-10-2021/blob/main/src/main/java/jmh/BytesHashcode.java
         *
         * Essentially we are doing this calculation:
         * h = h * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 +
         *         a[i + 0] * 31 * 31 * 31 * 31 * 31 * 31 * 31 +
         *         a[i + 1] * 31 * 31 * 31 * 31 * 31 * 31 +
         *         a[i + 2] * 31 * 31 * 31 * 31 * 31 +
         *         a[i + 3] * 31 * 31 * 31 * 31 +
         *         a[i + 4] * 31 * 31 * 31 +
         *         a[i + 5] * 31 * 31 +
         *         a[i + 6] * 31 +
         *         a[i + 7];
         */
        // public static int hashCode(MemorySegment array, long offset, short length) {
        // int h = 1;
        // long i = offset, loopBound = offset + ByteVector.SPECIES_64.loopBound(length), tailBound = offset + length;
        // for (; i < loopBound; i += ByteVector.SPECIES_64.length()) {
        // // load 8 bytes, into a 64-bit vector
        // ByteVector b = ByteVector.fromMemorySegment(ByteVector.SPECIES_64, array, i, ByteOrder.nativeOrder());
        // // convert 8 bytes into 8 ints (hashing calculation needs int!)
        // IntVector x = (IntVector) b.castShape(IntVector.SPECIES_256, 0);
        // h = h * HASH_ACCUM + x.mul(HASH_VECTOR).reduceLanes(VectorOperators.ADD);
        // }
        //
        // for (; i < tailBound; i++) {
        // h = 31 * h + array.get(ValueLayout.JAVA_BYTE, i);
        // }
        // return h;
        // }

        // scalar implementation
        // public static int hashCode(final MemorySegment array, final long offset, final short length) {
        // final long limit = offset + length;
        // int h = 1;
        // for (long i = offset; i < limit; i++) {
        // h = 31 * h + UNSAFE.getByte(array.address() + i);
        // }
        // return h;
        // }

        // fxhash
        public static int hashCode(final MemorySegment array, final long offset, final short length) {
            final int seed = 0x9E3779B9;
            final int rotate = 5;

            int x, y;
            if (length >= Integer.BYTES) {
                x = UNSAFE.getInt(array.address() + offset);
                y = UNSAFE.getInt(array.address() + offset + length - Integer.BYTES);
            }
            else {
                x = UNSAFE.getByte(array.address() + offset);
                y = UNSAFE.getByte(array.address() + offset + length - Byte.BYTES);
            }

            return (Integer.rotateLeft(x * seed, rotate) ^ y) * seed;
        }

        /** Vectorized Key Comparison **/

        private static boolean notEquals(MemorySegment a, long aOffset, MemorySegment b, long bOffset, short length, VectorSpecies BYTE_SPECIES) {
            final long aLimit = aOffset + length, bLimit = bOffset + length;

            // main loop
            long loopBound = bOffset + BYTE_SPECIES.loopBound(length);
            for (; bOffset < loopBound; aOffset += BYTE_SPECIES.length(), bOffset += BYTE_SPECIES.length()) {
                ByteVector av = ByteVector.fromMemorySegment(BYTE_SPECIES, a,
                        aOffset, ByteOrder.nativeOrder() /* , BYTE_SPECIES.indexInRange(aOffset, Math.min(aOffset + BYTE_SPECIES.length(), aLimit)) */);
                ByteVector bv = ByteVector.fromMemorySegment(BYTE_SPECIES, b,
                        bOffset, ByteOrder.nativeOrder() /* , BYTE_SPECIES.indexInRange(bOffset, Math.min(bOffset + BYTE_SPECIES.length(), bLimit)) */);
                if (av.compare(VectorOperators.NE, bv).anyTrue())
                    return true;
            }

            // tail cleanup - load last N bytes with mask
            if (bOffset < bLimit) {
                ByteVector av = ByteVector.fromMemorySegment(BYTE_SPECIES, a, aOffset, ByteOrder.nativeOrder(), BYTE_SPECIES.indexInRange(aOffset, aLimit));
                ByteVector bv = ByteVector.fromMemorySegment(BYTE_SPECIES, b, bOffset, ByteOrder.nativeOrder(), BYTE_SPECIES.indexInRange(bOffset, bLimit));
                if (av.compare(VectorOperators.NE, bv).anyTrue())
                    return true;
            }

            return false;
        }

        // scalar implementation
        // private static boolean equals(byte[] a, int aOffset, byte[] b, int bOffset, int len) {
        // while (bOffset < len)
        // if (a[aOffset++] != b[bOffset++])
        // return false;
        // return true;
        // }

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

        private MemorySegment KEYS = Arena.ofShared().allocate(MAP_SIZE * KEY_SIZE, 64);
        private MemorySegment VALUES = Arena.ofShared().allocate(MAP_SIZE * VALUE_SIZE, 16);

        public PartitionAggr() {
            // init min and max
            final long limit = VALUES.address() + (MAP_SIZE * VALUE_SIZE);
            for (long offset = VALUES.address(); offset < limit; offset += VALUE_SIZE) {
                UNSAFE.putShort(offset, Short.MAX_VALUE);
                UNSAFE.putShort(offset + 2, Short.MIN_VALUE);
            }
        }

        public void update(MemorySegment key, long keyStart, short keyLength, int keyHash, short value) {
            int index = keyHash & KEY_MASK;
            long keyOffset = KEYS.address() + (index * KEY_SIZE);
            while (((UNSAFE.getShort(keyOffset) != keyLength) ||
                    VectorUtils.notEquals(KEYS, ((index * KEY_SIZE) + 2), key, keyStart, keyLength, VectorUtils.BYTE_SPECIES))) {
                if (UNSAFE.getShort(keyOffset) == 0) {
                    // put key
                    UNSAFE.putShort(keyOffset, keyLength);
                    MemorySegment.copy(key, keyStart, KEYS, (index * KEY_SIZE) + 2, keyLength);
                    break;
                }
                else {
                    index = (index + 1) & KEY_MASK;
                    keyOffset = KEYS.address() + (index * KEY_SIZE);
                }
            }

            long valueOffset = VALUES.address() + (index * VALUE_SIZE);
            UNSAFE.putShort(valueOffset, (short) Math.min(UNSAFE.getShort(valueOffset), value));
            valueOffset += 2;
            UNSAFE.putShort(valueOffset, (short) Math.max(UNSAFE.getShort(valueOffset), value));
            valueOffset += 2;
            UNSAFE.putInt(valueOffset, UNSAFE.getInt(valueOffset) + 1);
            valueOffset += 4;
            UNSAFE.putLong(valueOffset, UNSAFE.getLong(valueOffset) + value);
        }

        public void mergeTo(ResultAggr result) {
            long keyOffset;
            short keyLength;
            for (int i = 0; i < MAP_SIZE; i++) {
                // extract key
                keyOffset = KEYS.address() + (i * KEY_SIZE);
                if ((keyLength = UNSAFE.getShort(keyOffset)) == 0)
                    continue;

                // extract values (if key is not null)
                final long valueOffset = VALUES.address() + (i * VALUE_SIZE);
                result.compute(new ResultAggr.ByteKey(KEYS, (i * KEY_SIZE) + 2, keyLength), (k, v) -> {
                    if (v == null) {
                        v = new ResultAggr.Measurement();
                    }
                    v.min = (short) Math.min(UNSAFE.getShort(valueOffset), v.min);
                    v.max = (short) Math.max(UNSAFE.getShort(valueOffset + 2), v.max);
                    v.count += UNSAFE.getInt(valueOffset + 4);
                    v.sum += UNSAFE.getLong(valueOffset + 8);

                    return v;
                });
            }
        }

    }

    /**
     * Measurement Aggregation (for all partitions)
     * Simple Concurrent Hash Table so all partitions can merge concurrently
     */
    protected static class ResultAggr extends HashMap<ResultAggr.ByteKey, ResultAggr.Measurement> {

        public static class ByteKey implements Comparable<ByteKey> {
            private final MemorySegment data;
            private final long offset;
            private final short length;
            private String str;

            public ByteKey(MemorySegment data, long offset, short length) {
                this.data = data;
                this.offset = offset;
                this.length = length;
            }

            @Override
            public boolean equals(Object other) {
                return (length == ((ByteKey) other).length)
                        && !VectorUtils.notEquals(data, offset, ((ByteKey) other).data, ((ByteKey) other).offset, length, VectorUtils.BYTE_SPECIES);
            }

            @Override
            public int hashCode() {
                return VectorUtils.hashCode(data, offset, length);
            }

            @Override
            public String toString() {
                if (str == null) {
                    // finally has to do a copy!
                    byte[] copy = new byte[length];
                    MemorySegment.copy(data, offset, MemorySegment.ofArray(copy), 0, length);
                    str = new String(copy, StandardCharsets.UTF_8);
                }
                return str;
            }

            @Override
            public int compareTo(ByteKey o) {
                return toString().compareTo(o.toString());
            }
        }

        protected static class Measurement {
            public short min = Short.MAX_VALUE;
            public short max = Short.MIN_VALUE;
            public int count = 0;
            public long sum = 0;

            @Override
            public String toString() {
                return ((double) min / 10) + "/" + (Math.round((1.0 * sum) / count) / 10.0) + "/" + ((double) max / 10);
            }

        }

        public ResultAggr(int initialCapacity, float loadFactor) {
            super(initialCapacity, loadFactor);
        }

        public Map toSorted() {
            return new TreeMap(this);
        }

    }

    protected static class Partition implements Runnable {

        private final MemorySegment data;
        private long offset;
        private final long limit;
        private final PartitionAggr result;

        public Partition(MemorySegment data, long offset, long limit, PartitionAggr result) {
            this.data = data;
            this.offset = offset;
            this.limit = limit;
            this.result = result;
        }

        @Override
        public void run() {
            // measurement parsing
            final PartitionAggr aggr = this.result;

            // main loop (vectorized)
            final long loopLimit = limit - (VectorUtils.BYTE_SPECIES.length() * Math.ceilDiv(100, VectorUtils.BYTE_SPECIES.length()) + Long.BYTES);
            while (offset < loopLimit) {
                long offsetStart = offset;

                // find station name upto ";"
                int found;
                do {
                    found = VectorUtils.findDelimiter(data, offset);
                    offset += found;
                } while (found == VectorUtils.BYTE_SPECIES.length());
                short stationLength = (short) (offset - offsetStart);
                int stationHash = VectorUtils.hashCode(data, offsetStart, stationLength);

                // find measurement upto "\n" (credit: merykitty)
                long numberBits = UNSAFE.getLong(data.address() + ++offset);
                final long invNumberBits = ~numberBits;
                final int decimalSepPos = Long.numberOfTrailingZeros(invNumberBits & 0x10101000);

                int shift = 28 - decimalSepPos;
                long signed = (invNumberBits << 59) >> 63;
                long designMask = ~(signed & 0xFF);
                long digits = ((numberBits & designMask) << shift) & 0x0F000F0F00L;
                long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;

                short fixed = (short) ((absValue ^ signed) - signed);
                offset += (decimalSepPos >>> 3) + 3;

                // update measurement
                aggr.update(data, offsetStart, stationLength, stationHash, fixed);
            }

            // tail loop (simple)
            while (offset < limit) {
                long offsetStart = offset;

                // find station name upto ";"
                short stationLength = 0;
                while (UNSAFE.getByte(data.address() + offset++) != ';')
                    stationLength++;
                int stationHash = VectorUtils.hashCode(data, offsetStart, stationLength);

                // find measurement upto "\n"
                byte tempBuffer = UNSAFE.getByte(data.address() + offset++);
                boolean isNegative = (tempBuffer == '-');
                short fixed = (short) (isNegative ? 0 : (tempBuffer - '0'));
                while (true) {
                    tempBuffer = UNSAFE.getByte(data.address() + offset++);
                    if (tempBuffer == '.') {
                        fixed = (short) (fixed * 10 + (UNSAFE.getByte(data.address() + offset) - '0'));
                        offset += 2;
                        break;
                    }
                    fixed = (short) (fixed * 10 + (tempBuffer - '0'));
                }
                fixed = isNegative ? (short) -fixed : fixed;

                // update measurement
                aggr.update(data, offsetStart, stationLength, stationHash, fixed);
            }

            // measurement result collection
            // aggr.mergeTo(result);
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

                // note: vectorize this made performance worse :(
                while (UNSAFE.getByte(data.address() + partition[i + 1]++) != '\n')
                    ;
            }

            // partition aggregation
            var threadList = new Thread[processors];
            PartitionAggr[] partAggrs = new PartitionAggr[processors];
            for (int i = 0; i < processors; i++) {
                if (partition[i] == data.byteSize())
                    break;

                partAggrs[i] = new PartitionAggr();
                threadList[i] = new Thread(new Partition(data, partition[i], partition[i + 1], partAggrs[i]));
                threadList[i].start();
            }

            // result
            ResultAggr result = new ResultAggr(1 << 14, 1);
            for (int i = 0; i < processors; i++) {
                if (partition[i] == data.byteSize())
                    break;

                threadList[i].join();
                partAggrs[i].mergeTo(result);
            }
            System.out.println(result.toSorted());
        }

        // long elapsed = System.currentTimeMillis() - startTime;
        // System.out.println("Elapsed: " + ((double) elapsed / 1000.0));

    }

    /** Unit Tests **/

    public static void testMain(String[] args) {
        testHashCode();
        testNotEquals();
    }

    private static void testHashCode() {
        // test key length from 1 to 100
        for (int i = 1; i <= 100; i++) {
            byte[] array = new byte[i];
            for (int j = 0; j < i; j++)
                array[j] = (byte) j;

            // compare with java default implementation
            assertTrue(VectorUtils.hashCode(MemorySegment.ofArray(array), 0, (short) i) == Arrays.hashCode(array));
        }
    }

    private static void testNotEquals() {
        byte[] a = new byte[128];
        byte[] b = new byte[128];

        // all equals
        for (int i = 1; i < 100; i++) {
            a[(i + 2) - 1] = 0;
            b[i - 1] = 0;
            a[(i + 2)] = 10;
            b[i] = 10;
            assertTrue(!VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_64));
            assertTrue(!VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_128));
            assertTrue(!VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_256));
            assertTrue(!VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_512));
        }

        // one el not equals
        for (int i = 1; i < 100; i++) {
            a[(i + 2) - 1] = 0;
            b[i - 1] = 0;
            a[(i + 2)] = 20;
            b[i] = 10;
            assertTrue(VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_64));
            assertTrue(VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_128));
            assertTrue(VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_256));
            assertTrue(VectorUtils.notEquals(MemorySegment.ofArray(a), 2, MemorySegment.ofArray(b), 0, (short) 100, ByteVector.SPECIES_512));
        }
    }

    private static void assertTrue(boolean condition) {
        if (!condition) {
            throw new RuntimeException("Failed test");
        }
    }

}
