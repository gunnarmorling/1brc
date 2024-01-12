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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import sun.misc.Unsafe;

public final class CalculateAverage_fwbrasil {

    static final Charset CHARSET = Charset.forName("UTF-8");
    static final Unsafe UNSAFE = initUnsafe();

    static final int PARTITIONS = (int) (Runtime.getRuntime().availableProcessors() * 1.5);
    static final long PRIME = (long) (Math.pow(2, 31) - 1);

    public static void main(String[] args) throws Throwable {
        new CalculateAverage_fwbrasil().run();
    }

    CalculateAverage_fwbrasil() throws Throwable {
    }

    final RandomAccessFile file = new RandomAccessFile("measurements.txt", "r");
    final long length = file.length();
    final MemorySegment segment = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
    final long address = segment.address();

    final long partitionSize = length / PARTITIONS;
    final Partition[] partitions = new Partition[PARTITIONS];
    final CountDownLatch cdl = new CountDownLatch(PARTITIONS);

    void run() throws Throwable {
        (new Preload()).start();
        runPartitions();
        Results results = mergeResults();
        System.out.println(results);
    }

    void runPartitions() throws Throwable {
        for (int i = 0; i < PARTITIONS; i++) {
            var p = new Partition();
            p.id = i;
            partitions[i] = p;
        }
        var i = 0;
        var start = 0L;
        var pos = partitionSize;
        while (pos < length) {
            pos = findNextEntry(pos);
            if (pos > length) {
                pos = length;
            }
            partitions[i].run(start, pos);
            start = pos;
            pos += partitionSize;
            i++;
        }
        if (start < length) {
            partitions[i].run(start, length);
            i++;
        }
        for (int j = 0; j < PARTITIONS - i; j++) {
            cdl.countDown();
        }
        cdl.await();
    }

    long findNextEntry(long pos) {
        while (pos < length && read(pos) != '\n') {
            pos++;
        }
        pos++;
        return pos;
    }

    Results mergeResults() {
        Results results = partitions[0].results;
        for (int i = 1; i < PARTITIONS; i++) {
            results.merge(partitions[i].results);
        }
        return results;
    }

    byte read(long pos) {
        return UNSAFE.getByte(address + pos);
    }

    final class Partition implements Runnable {
        Results results = new Results();

        int id;
        long start = 0L;
        long end = 0L;
        long pos = 0L;

        public void run(long start, long end) {
            this.start = start;
            this.end = end;
            (new Thread(this)).start();
        }

        @Override
        public void run() {
            pos = start - 1;
            while (pos < end - 1) {
                pos++;
                readEntry();
            }
            cdl.countDown();
        }

        void readEntry() {
            var keyStart = pos;
            var hash = readKeyHash();
            var keyEnd = pos;
            pos++;
            var value = readValue();
            results.add(keyStart, keyEnd, hash, value);
        }

        long readKeyHash() {
            long hash = 0L;
            byte b = 0;
            var i = 0;
            var pos = this.pos;
            for (; (b = read(pos + i)) != ';'; i++) {
                hash = (b + hash + i) * 257;
            }
            this.pos = pos + i;
            return Math.abs(hash * PRIME);
        }

        int readValue() {
            var value = Integer.MAX_VALUE;
            var negative = false;
            if (read(pos) == '-') {
                negative = true;
                pos++;
            }
            byte maybeDot = read(pos + 1);
            if (maybeDot == '.') {
                value = read(pos) * 10 + read(pos + 2) - 528;
                pos += 3;
            }
            else {
                value = read(pos) * 100 + maybeDot * 10 + read(pos + 3) - 5328;
                pos += 4;
            }
            return negative ? -value : value;
        }
    }

    final class Results {

        static final long TABLE_BASE_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
        static final long TABLE_SCALE = UNSAFE.arrayIndexScale(long[].class);
        static final int KEYS_SIZE = (int) Math.pow(2, 15);
        static final int KEYS_MASK = KEYS_SIZE * 2 - 1;
        static final int ENTRIES_SIZE = 10_000;

        // keyHash | stateIdx
        final long[] keys = new long[KEYS_SIZE * 2];

        // keyStart | keyEnd
        final long[] meta = new long[ENTRIES_SIZE * 2];

        // sum | packed(min | max | count)
        final long[] values = new long[ENTRIES_SIZE * 2];

        final Packed packed = new Packed();

        int nextEntry = 0;

        public void add(long keyStart, long keyEnd, long hash, int value) {
            var idx = findKey(keyStart, keyEnd, hash);
            update(value, idx);
        }

        private void update(int value, int idx) {
            add(values, idx, value);
            long packed2 = get(values, idx + 1);
            packed.decode(packed2);
            packed.update(value);
            set(values, idx + 1, packed.encode());
        }

        public void merge(Results other) {
            for (int i = 0; i < keys.length; i += 2) {
                var hash = 0L;
                if ((hash = get(other.keys, i)) != 0) {
                    var idx = (int) get(other.keys, i + 1);
                    var keyStart = get(other.meta, idx);
                    var keyEnd = get(other.meta, idx + 1);
                    var sum = get(other.values, idx);
                    other.packed.decode(get(other.values, idx + 1));

                    idx = findKey(keyStart, keyEnd, hash);
                    add(values, idx, sum);
                    packed.decode(get(values, idx + 1));
                    packed.merge(other.packed);
                    set(values, idx + 1, packed.encode());
                }
            }
        }

        int findKey(long keyStart, long keyEnd, long hash) {
            var h = 0L;
            int idx = (int) ((hash * 2) & KEYS_MASK);
            if ((h = get(keys, idx)) != hash && h != 0) {
                idx = (idx + 2) & KEYS_MASK;
                if ((h = get(keys, idx)) != hash && h != 0) {
                    idx = (idx + 2) & KEYS_MASK;
                    while ((h = get(keys, idx)) != hash && h != 0) {
                        idx = (idx + 2) & KEYS_MASK;
                    }
                }
            }
            if (h != 0) {
                return (int) get(keys, idx + 1);
            }
            else {
                var entryIdx = nextEntry++ * 2;
                set(keys, idx, hash);
                set(keys, idx + 1, entryIdx);
                set(meta, entryIdx, keyStart);
                set(meta, entryIdx + 1, keyEnd);
                set(values, entryIdx, 0L);
                set(values, entryIdx + 1, packed.init);
                return entryIdx;
            }
        }

        String readKey(int idx) {
            var start = get(meta, idx);
            var end = get(meta, idx + 1);
            var size = (int) (end - start);
            var bytes = new byte[size];
            UNSAFE.copyMemory(null, address + start, bytes,
                    Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
            try {
                return new String(bytes, CHARSET);
            }
            catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        }

        final static void set(long[] arr, int idx, long value) {
            UNSAFE.putLong(arr, offset(idx), value);
        }

        final static long get(long[] arr, int idx) {
            return UNSAFE.getLong(arr, offset(idx));
        }

        final static void add(long[] arr, int idx, long value) {
            set(arr, idx, get(arr, idx) + value);
        }

        final static long offset(int idx) {
            return TABLE_BASE_OFFSET + idx * TABLE_SCALE;
        }

        @Override
        public String toString() {
            var results = new TreeMap<String, String>();
            try {
                for (int i = 0; i < keys.length; i += 2) {
                    if (get(keys, i) != 0) {
                        var idx = (int) get(keys, i + 1);
                        var sum = get(values, idx);
                        packed.decode(get(values, idx + 1));
                        var res = round(packed.min / 10.0) + "/" +
                                round((sum / 10.0) / packed.count) + "/"
                                + round(packed.max / 10.0);
                        results.put(readKey(idx), res);
                    }
                }
            }
            catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
            return results.toString();
        }
    }

    static final class Packed {
        int min = Short.MAX_VALUE;
        int max = Short.MIN_VALUE;
        int count = 0;

        long init = encode();

        public long encode() {
            return (((long) min & 0xFFFF) << 48) | (((long) max & 0xFFFF) << 32) | (count & 0xFFFFFFFFL);
        }

        public void decode(long packed) {
            min = (short) ((packed >> 48) & 0xFFFF);
            max = (short) ((packed >> 32) & 0xFFFF);
            count = (int) packed;
        }

        public void init(int value) {
            min = value;
            max = value;
            count = 0;
        }

        public void update(int value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            count++;
        }

        public void merge(Packed other) {
            if (other.min < min) {
                min = other.min;
            }
            if (other.max > max) {
                max = other.max;
            }
            count += other.count;
        }
    }

    class Preload extends Thread {

        @Override
        public void run() {
            for (long i = 0; i < length; i += 4096) {
                segment.get(JAVA_BYTE, i);
            }
        }
    }

    static final Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static final double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

}
