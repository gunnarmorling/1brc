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

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import sun.misc.Unsafe;

public final class CalculateAverage_fwbrasil {

    static final Unsafe UNSAFE = initUnsafe();

    static final long TABLE_BASE_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    static final long TABLE_SCALE = UNSAFE.arrayIndexScale(long[].class);
    static final int TABLE_SIZE = (int) Math.pow(2, 15);
    static final int TABLE_MASK = TABLE_SIZE - 1;

    static final int PARTITIONS = (int) (Runtime.getRuntime().availableProcessors());
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
            for (; (b = read(pos)) != ';'; pos++) {
                hash += b;
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }
            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);
            var r = Math.abs(hash * PRIME);
            return r;
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

        int idx = 0;

        // keyHash | keyStart | keyEnd | 0 | min | max | sum | count
        final long[] table = new long[TABLE_SIZE * 8];

        public void add(long keyStart, long keyEnd, long hash, int value) {
            findKey(keyStart, keyEnd, hash);
            var o = offset(4);
            if (value < get(o)) {
                put(o, value);
            }
            o = next(o);
            if (value > get(o)) {
                put(o, value);
            }
            o = next(o);
            put(o, get(o) + value);
            o = next(o);
            put(o, get(o) + 1);
        }

        public void merge(Results other) {
            for (int i = 0; i < TABLE_SIZE * 8; i += 8) {
                other.idx = i;
                if (!other.empty()) {
                    idx = i;
                    while (keyHash() != other.keyHash() && !empty()) {
                        next();
                    }
                    if (empty()) {
                        UNSAFE.copyMemory(other.table, other.offset(), table, offset(), TABLE_SCALE * 8);
                    }
                    else {
                        var o1 = offset(4);
                        var o2 = other.offset(4);
                        var v = 0L;
                        if ((v = other.get(o2)) < get(o1)) {
                            put(o1, v);
                        }
                        o1 = next(o1);
                        o2 = next(o2);
                        if ((v = other.get(o2)) > get(o1)) {
                            put(o1, v);
                        }
                        o1 = next(o1);
                        o2 = next(o2);
                        put(o1, get(o1) + other.get(o2));
                        o1 = next(o1);
                        put(o1, get(o1) + 1);
                    }
                }
            }
        }

        @Override
        public String toString() {
            var results = new TreeMap<String, String>();
            try {
                for (int i = 0; i < TABLE_SIZE * 8; i += 8) {
                    idx = i;
                    if (!empty()) {
                        var res = round(min() / 10.0) + "/" +
                                round((sum() / 10.0) / count()) + "/"
                                + round(max() / 10.0);
                        results.put(readKey(), res);
                    }
                }
            }
            catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
            return results.toString();
        }

        double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        String readKey() {
            var start = keyStart();
            var size = (int) (keyEnd() - start);
            var bytes = new byte[size];
            UNSAFE.copyMemory(null, address + start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
            try {
                return new String(bytes, "UTF-8");
            }
            catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        }

        void findKey(long keyStart, long keyEnd, long hash) {
            idx = (int) (hash & TABLE_MASK) * 8;
            if (keyHash() != hash && !empty()) {
                next();
                while (keyHash() != hash && !empty()) {
                    next();
                }
            }
            if (empty()) {
                var o = offset();
                put(o, hash);
                o = next(o);
                put(o, keyStart);
                o = next(o);
                put(o, keyEnd);
                o = next(o);
                o = next(o);
                put(o, Long.MAX_VALUE);
                o = next(o);
                put(o, Long.MIN_VALUE);
            }
        }

        long get(long offset) {
            return UNSAFE.getLong(table, offset);
        }

        void put(long offset, long v) {
            UNSAFE.putLong(table, offset, v);
        }

        long next(long offset) {
            return offset + TABLE_SCALE;
        }

        long offset() {
            return offset(0);
        }

        long offset(long pos) {
            return TABLE_BASE_OFFSET + ((idx + pos) * TABLE_SCALE);
        }

        void next() {
            idx = (idx + 8) & TABLE_MASK;
        }

        boolean empty() {
            return keyEnd() == 0;
        }

        long keyHash() {
            return get(offset(0));
        }

        long keyStart() {
            return get(offset(1));
        }

        long keyEnd() {
            return get(offset(2));
        }

        long min() {
            return get(offset(4));
        }

        long max() {
            return get(offset(5));
        }

        long sum() {
            return get(offset(6));
        }

        long count() {
            return get(offset(7));
        }
    }

    static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
