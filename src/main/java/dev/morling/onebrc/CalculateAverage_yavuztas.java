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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import java.util.function.Consumer;

public class CalculateAverage_yavuztas {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final Unsafe UNSAFE = unsafe();

    // I compared all three: MappedByteBuffer, MemorySegment and Unsafe.
    // Accessing the memory using Unsafe is still the fastest in my experience.
    // However, I would never use it in production, single programming error will crash your app.
    private static Unsafe unsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Only one object, both for measurements and keys, less object creation in hotpots is always faster
    static class Record {

        final long start; // memory address of the underlying data
        final byte length;
        final int hash;

        private int min; // calculations over int is faster than double, we convert to double in the end only once
        private int max;
        private long sum;
        private int count;

        public Record(long start, byte length, int hash, int temp) {
            this.start = start;
            this.length = length;
            this.hash = hash;
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        @Override
        public boolean equals(Object o) {
            final Record record = (Record) o;
            return equals(record.start, record.length);
        }

        private static long partial(long word, int length) {
            final long mask = ~0L << (length << 3);
            return word & ~mask;
        }

        private static boolean equalsFor8Bytes(long start1, long start2, byte length) {
            return partial(UNSAFE.getLong(start1), length) == partial(UNSAFE.getLong(start2), length);
        }

        private static boolean equalsFor16Bytes(long start1, long start2, byte length) {
            return UNSAFE.getLong(start1) == UNSAFE.getLong(start2) &&
                    partial(UNSAFE.getLong(start1 + 8), length - 8) == partial(UNSAFE.getLong(start2 + 8), length - 8);
        }

        private static boolean equalsForBiggerThan16Bytes(long start1, long start2, byte length) {
            int i = 0;
            int step = 0;
            while (length > 8) { // scan bytes
                length -= 8;
                step = i++ << 3; // 8 bytes
                if (UNSAFE.getLong(start1 + step) != UNSAFE.getLong(start2 + step)) {
                    return false;
                }
            }
            // check the last part
            return equalsFor8Bytes(start1 + step, start2 + step, length);
        }

        public boolean equals(long start, byte length) {
            // equals check is done by comparing longs instead of byte by byte check
            // this is slightly faster
            if (length <= 8) {
                // smaller than long, check special
                return equalsFor8Bytes(this.start, start, length);
            }
            if (length <= 16) {
                // smaller than two longs, check special
                return equalsFor16Bytes(this.start, start, length);
            }
            // check the bigger ones via traverse
            return equalsForBiggerThan16Bytes(this.start, start, length);
        }

        @Override
        public String toString() {
            final byte[] bytes = new byte[this.length];
            UNSAFE.copyMemory(null, this.start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, this.length);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        public void collect(int temp) {
            if (temp < this.min) {
                this.min = (short) temp;
            }
            if (temp > this.max) {
                this.max = (short) temp;
            }
            this.sum += temp;
            this.count++;
        }

        public void merge(Record that) {
            if (that.min < this.min) {
                this.min = that.min;
            }
            if (that.max > this.max) {
                this.max = that.max;
            }
            this.sum += that.sum;
            this.count += that.count;
        }

        public String measurements() {
            // here is only executed once for each unique key, so StringBuilder creation doesn't harm
            final StringBuilder sb = new StringBuilder(14);
            sb.append(this.min / 10.0);
            sb.append("/");
            sb.append(round((this.sum / 10.0) / this.count));
            sb.append("/");
            sb.append(this.max / 10.0);
            return sb.toString();
        }
    }

    // Inspired by @spullara - customized hashmap on purpose
    // The main difference is we hold only one array instead of two, fewer objects is faster
    static class RecordMap {

        // Bigger bucket size less collisions, but you have to find a sweet spot otherwise it is becoming slower.
        // Also works good enough for 10K stations
        static final int SIZE = 1 << 16; // 64kb
        static final int BITMASK = SIZE - 1;
        Record[] keys = new Record[SIZE];

        static int hashBucket(int hash) {
            hash = hash ^ (hash >>> 16); // naive bit spreading but surprisingly decreases collision :)
            return hash & BITMASK; // fast modulo, to find bucket
        }

        void putAndCollect(long start, byte length, int hash, int temp) {
            int bucket = hashBucket(hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                this.keys[bucket] = new Record(start, length, hash, temp);
                return;
            }

            if (!existing.equals(start, length)) { // collision
                int step = 0; // quadratic probing helps to decrease collision in large data set of 10K
                while ((existing = this.keys[bucket = ((++bucket + ++step) & BITMASK)]) != null && !existing.equals(start, length)) {
                    // can be stuck here if all the buckets are full :(
                    // However, since the data set is max 10K (unique) this shouldn't happen
                    // So, I'm happily leave here branchless :)
                }
                if (existing == null) {
                    this.keys[bucket] = new Record(start, length, hash, temp);
                    return;
                }
            }
            existing.collect(temp);
        }

        void putOrMerge(Record key) {
            int bucket = hashBucket(key.hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                this.keys[bucket] = key;
                return;
            }

            if (!existing.equals(key)) { // collision
                int step = 0; // quadratic probing helps to decrease collision in large data set of 10K
                while ((existing = this.keys[bucket = ((++bucket + ++step) & BITMASK)]) != null && !existing.equals(key)) {
                    // can be stuck here if all the buckets are full :(
                    // However, since the data set is max 10K (unique keys) this shouldn't happen
                    // So, I'm happily leave here branchless :)
                }
                if (existing == null) {
                    this.keys[bucket] = key;
                    return;
                }
            }
            existing.merge(key);
        }

        void forEach(Consumer<Record> consumer) {
            int pos = 0;
            Record key;
            while (pos < SIZE) {
                if ((key = this.keys[pos++]) == null) {
                    continue;
                }
                consumer.accept(key);
            }
        }

        void merge(RecordMap other) {
            other.forEach(this::putOrMerge);
        }

    }

    // One actor for one thread, no synchronization
    static class RegionActor extends Thread {

        final FileChannel channel;
        final long startPos;
        final int size;
        long regionMemoryAddress;

        final RecordMap map = new RecordMap();

        public RegionActor(FileChannel channel, long startPos, int size) throws IOException {
            this.channel = channel;
            this.startPos = startPos;
            this.size = size;
            // get the segment memory address, this is the only thing we need for Unsafe
            this.regionMemoryAddress = this.channel.map(FileChannel.MapMode.READ_ONLY, startPos, size, Arena.global()).address();
        }

        @Override
        public void run() {
            int keyHash;
            byte length;
            byte b;
            byte b2;
            byte b3;
            byte b4;
            byte b5;
            byte b6;
            byte b7;
            long start;
            long pointer = this.regionMemoryAddress;
            final long size = pointer + this.size;
            while (pointer < size) { // line start
                start = pointer; // save line start position
                keyHash = UNSAFE.getByte(pointer++); // first byte is guaranteed not to be ';'

                while (true) { // read to the death :)
                    if ((b = UNSAFE.getByte(pointer++)) != ';') { // unroll level 1
                        if ((b2 = UNSAFE.getByte(pointer++)) != ';') { // unroll level 2
                            if ((b3 = UNSAFE.getByte(pointer++)) != ';') { // unroll level 3
                                if ((b4 = UNSAFE.getByte(pointer++)) != ';') { // unroll level 4
                                    if ((b5 = UNSAFE.getByte(pointer++)) != ';') { // unroll level 5
                                        if ((b6 = UNSAFE.getByte(pointer++)) != ';') { // unroll level 6
                                            if ((b7 = UNSAFE.getByte(pointer++)) != ';') { // unroll level 7. It's not getting faster anymore, let's stop it :)
                                                keyHash = calculateHash(keyHash, b, b2, b3, b4, b5, b6, b7);
                                            }
                                            else {
                                                keyHash = calculateHash(keyHash, b, b2, b3, b4, b5, b6);
                                                break;
                                            }
                                        }
                                        else {
                                            keyHash = calculateHash(keyHash, b, b2, b3, b4, b5);
                                            break;
                                        }
                                    }
                                    else {
                                        keyHash = calculateHash(keyHash, b, b2, b3, b4);
                                        break;
                                    }
                                }
                                else {
                                    keyHash = calculateHash(keyHash, b, b2, b3);
                                    break;
                                }
                            }
                            else {
                                keyHash = calculateHash(keyHash, b, b2);
                                break;
                            }
                        }
                        else {
                            keyHash = calculateHash(keyHash, b);
                            break;
                        }
                    }
                    else {
                        break;
                    }
                }

                // station length
                length = (byte) (pointer - start - 1); // "-1" for ';'

                // read temparature
                final long numberPart = UNSAFE.getLong(pointer);
                final int decimalPos = Long.numberOfTrailingZeros(~numberPart & 0x10101000);
                final int temp = convertIntoNumber(decimalPos, numberPart);
                pointer += (decimalPos >>> 3) + 3;

                this.map.putAndCollect(start, length, keyHash, temp);
            }
        }

        /*
         * Unrolled hash functions
         */
        static int calculateHash(int hash, int b) {
            return 31 * hash + b;
        }

        static int calculateHash(int hash, int b1, int b2) {
            return (31 * 31 * hash) + (31 * b1) + b2;
        }

        static int calculateHash(int hash, int b1, int b2, int b3) {
            return (31 * 31 * 31 * hash) + (31 * 31 * b1) + (31 * b2) + b3;
        }

        static int calculateHash(int hash, int b1, int b2, int b3, int b4) {
            return (31 * 31 * 31 * 31 * hash) + (31 * 31 * 31 * b1) + (31 * 31 * b2) + (31 * b3) + b4;
        }

        static int calculateHash(int hash, int b1, int b2, int b3, int b4, int b5) {
            return (31 * 31 * 31 * 31 * 31 * hash) + (31 * 31 * 31 * 31 * b1) + (31 * 31 * 31 * b2) + (31 * 31 * b3) + (31 * b4) + b5;
        }

        static int calculateHash(int hash, int b1, int b2, int b3, int b4, int b5, int b6) {
            return (31 * 31 * 31 * 31 * 31 * 31 * hash) + (31 * 31 * 31 * 31 * 31 * b1) + (31 * 31 * 31 * 31 * b2) + (31 * 31 * 31 * b3) + (31 * 31 * b4) + (31 * b5)
                    + b6;
        }

        static int calculateHash(int hash, int b1, int b2, int b3, int b4, int b5, int b6, int b7) {
            return (31 * 31 * 31 * 31 * 31 * 31 * 31 * hash) + (31 * 31 * 31 * 31 * 31 * 31 * b1) + (31 * 31 * 31 * 31 * 31 * b2) + (31 * 31 * 31 * 31 * b3)
                    + (31 * 31 * 31 * b4) + (31 * 31 * b5) + (31 * b6) + b7;
        }

        // Credits to @merrykitty. Magical solution to parse temparature values branchless!
        // Taken as without modification, comments belong to @merrykitty
        private static int convertIntoNumber(int decimalSepPos, long numberWord) {
            int shift = 28 - decimalSepPos;
            // signed is -1 if negative, 0 otherwise
            long signed = (~numberWord << 59) >> 63;
            long designMask = ~(signed & 0xFF);
            // Align the number to a specific position and transform the ascii code
            // to actual digit value in each byte
            long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;
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
            return (int) value;
        }

        /**
         * blocks until the map is fully collected
         */
        RecordMap get() throws InterruptedException {
            join();
            return this.map;
        }
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    /**
     * Scans the given buffer to the left
     */
    private static long findClosestLineEnd(long start, int size, FileChannel channel) throws IOException {
        final long position = start + size;
        final long left = Math.max(position - 101, 0);
        final ByteBuffer buffer = ByteBuffer.allocate(101); // enough size to find at least one '\n'
        if (channel.read(buffer.clear(), left) != -1) {
            int bufferPos = buffer.position() - 1;
            while (buffer.get(bufferPos) != '\n') {
                bufferPos--;
                size--;
            }
        }
        return size;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        var concurrency = 2 * Runtime.getRuntime().availableProcessors();
        final long fileSize = Files.size(FILE);
        long regionSize = fileSize / concurrency;

        // handling extreme cases
        while (regionSize > Integer.MAX_VALUE) {
            concurrency *= 2;
            regionSize /= 2;
        }
        if (fileSize <= 1 << 20) { // small file (1mb), no need concurrency
            concurrency = 1;
            regionSize = fileSize;
        }

        long startPos = 0;
        final FileChannel channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ);
        final RegionActor[] actors = new RegionActor[concurrency];
        for (int i = 0; i < concurrency; i++) {
            // calculate boundaries
            long maxSize = (startPos + regionSize > fileSize) ? fileSize - startPos : regionSize;
            // shift position to back until we find a linebreak
            maxSize = findClosestLineEnd(startPos, (int) maxSize, channel);

            final RegionActor region = (actors[i] = new RegionActor(channel, startPos, (int) maxSize));
            region.start(); // start processing

            startPos += maxSize;
        }

        final RecordMap output = new RecordMap(); // output to merge all records
        for (RegionActor actor : actors) {
            final RecordMap partial = actor.get(); // blocks until get the result
            output.merge(partial);
        }

        // sort and print the result
        final TreeMap<String, String> sorted = new TreeMap<>();
        output.forEach(key -> sorted.put(key.toString(), key.measurements()));
        System.out.println(sorted);
    }

}
