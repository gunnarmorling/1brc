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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.TreeMap;

public class CalculateAverage_ebarlas {

    private static final int MAX_KEY_SIZE = 100;
    private static final int HASH_FACTOR = 433;
    private static final int HASH_TBL_SIZE = 16_383; // range of allowed hash values, inclusive

    private static final Unsafe UNSAFE = makeUnsafe();

    private static Unsafe makeUnsafe() {
        try {
            var f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        var path = Paths.get("measurements.txt");
        var numPartitions = Math.max(8, Runtime.getRuntime().availableProcessors());
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        var partitionSize = channel.size() / numPartitions;
        var partitions = new Partition[numPartitions];
        var threads = new Thread[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            var pIdx = i;
            var pStart = pIdx * partitionSize;
            var pEnd = pIdx == numPartitions - 1
                    ? channel.size() // last partition might be slightly larger
                    : pStart + partitionSize;
            var pSize = pEnd - pStart;
            Runnable r = () -> {
                try {
                    var buffer = channel.map(FileChannel.MapMode.READ_ONLY, pStart, pSize).order(ByteOrder.LITTLE_ENDIAN);
                    partitions[pIdx] = processBuffer(buffer, pIdx == 0);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }
        for (var thread : threads) {
            thread.join();
        }
        var partitionList = List.of(partitions);
        foldFootersAndHeaders(partitionList);
        printResults(foldStats(partitionList));
    }

    private static void printResults(Stats[] stats) { // adheres to Gunnar's reference code
        var result = new TreeMap<String, String>();
        for (var st : stats) {
            if (st != null) {
                var key = new String(convert(st.keyAddr, st.keyLen, st.lastBytes), StandardCharsets.UTF_8);
                result.put(key, format(st));
            }
        }
        System.out.println(result);
    }

    private static byte[] convert(long keyAddr, int keyLen, int keyLastBytes) {
        var len = keyLastBytes == 4
                ? keyLen * 4 // fully packed
                : (keyLen - 1) * 4 + keyLastBytes; // last int partially packed
        var bytes = new byte[len];
        var idx = 0;
        for (long i = 0; i < keyLen; i++) {
            var offset = i << 2;
            var n = UNSAFE.getInt(keyAddr + offset);
            var bound = i == keyLen - 1 ? keyLastBytes : 4;
            for (int j = 0; j < bound; j++) {
                bytes[idx++] = (byte) (n & 0xFF);
                n >>>= 8;
            }
        }
        return bytes;
    }

    private static String format(Stats st) { // adheres to expected output format
        return round(st.min / 10.0) + "/" + round((st.sum / 10.0) / st.count) + "/" + round(st.max / 10.0);
    }

    private static double round(double value) { // Gunnar's round function
        return Math.round(value * 10.0) / 10.0;
    }

    private static Stats[] foldStats(List<Partition> partitions) { // fold stats from all partitions into first partition
        var target = partitions.getFirst().stats;
        for (int i = 1; i < partitions.size(); i++) {
            var current = partitions.get(i).stats;
            for (int j = 0; j < current.length; j++) {
                if (current[j] != null) {
                    var t = findInTable(target, current[j].hash, current[j].keyAddr, current[j].keyLen, current[j].lastBytes);
                    t.min = Math.min(t.min, current[j].min);
                    t.max = Math.max(t.max, current[j].max);
                    t.sum += current[j].sum;
                    t.count += current[j].count;
                }
            }
        }
        return target;
    }

    private static void foldFootersAndHeaders(List<Partition> partitions) { // fold footers and headers into prev partition
        for (int i = 1; i < partitions.size(); i++) {
            var pNext = partitions.get(i);
            var pPrev = partitions.get(i - 1);
            var merged = mergeFooterAndHeader(pPrev.footer, pNext.header);
            if (merged != null && merged.length != 0) {
                if (merged[merged.length - 1] == '\n') { // fold into prev partition
                    doProcessBuffer(ByteBuffer.wrap(merged).order(ByteOrder.LITTLE_ENDIAN), true, pPrev.stats);
                }
                else { // no newline appeared in partition, carry forward
                    pNext.footer = merged;
                }
            }
        }
    }

    private static byte[] mergeFooterAndHeader(byte[] footer, byte[] header) {
        if (footer == null) {
            return header;
        }
        if (header == null) {
            return footer;
        }
        var merged = new byte[footer.length + header.length];
        System.arraycopy(footer, 0, merged, 0, footer.length);
        System.arraycopy(header, 0, merged, footer.length, header.length);
        return merged;
    }

    private static Partition processBuffer(ByteBuffer buffer, boolean first) {
        return doProcessBuffer(buffer, first, new Stats[HASH_TBL_SIZE + 1]);
    }

    private static Partition doProcessBuffer(ByteBuffer buffer, boolean first, Stats[] stats) {
        var header = first ? null : readHeader(buffer);
        var keyStart = reallyDoProcessBuffer(buffer, stats);
        var footer = keyStart < buffer.limit() ? readFooter(buffer, keyStart) : null;
        return new Partition(header, footer, stats);
    }

    private static int reallyDoProcessBuffer(ByteBuffer buffer, Stats[] stats) {
        long keyBaseAddr = UNSAFE.allocateMemory(MAX_KEY_SIZE);
        int keyStart = 0; // start of key in buffer used for footer calc
        try { // abort with exception to allow optimistic line processing
            while (true) { // one line per iteration
                keyStart = buffer.position(); // preserve line start
                int keyHash = 0; // key hash code
                long keyAddr = keyBaseAddr; // address for next int
                int keyArrLen = 0; // number of key 4-byte ints
                int keyLastBytes; // occupancy in last byte (1, 2, 3, or 4)
                int val; // temperature value
                while (true) {
                    int n = buffer.getInt();
                    byte b0 = (byte) (n & 0xFF);
                    byte b1 = (byte) ((n >> 8) & 0xFF);
                    byte b2 = (byte) ((n >> 16) & 0xFF);
                    byte b3 = (byte) ((n >> 24) & 0xFF);
                    if (b0 == ';') { // ...;1.1
                        val = getVal(buffer, b1, b2, b3, buffer.get());
                        keyLastBytes = 4;
                        break;
                    }
                    else if (b1 == ';') { // ...a;1.1
                        val = getVal(buffer, b2, b3, buffer.get(), buffer.get());
                        UNSAFE.putInt(keyAddr, b0);
                        keyLastBytes = 1;
                        keyArrLen++;
                        keyHash = HASH_FACTOR * keyHash + b0;
                        break;
                    }
                    else if (b2 == ';') { // ...ab;1.1
                        val = getVal(buffer, b3, buffer.get(), buffer.get(), buffer.get());
                        UNSAFE.putInt(keyAddr, n & 0x0000FFFF);
                        keyLastBytes = 2;
                        keyArrLen++;
                        keyHash = HASH_FACTOR * (HASH_FACTOR * keyHash + b0) + b1;
                        break;
                    }
                    else if (b3 == ';') { // ...abc;1.1
                        UNSAFE.putInt(keyAddr, n & 0x00FFFFFF);
                        keyLastBytes = 3;
                        keyArrLen++;
                        keyHash = HASH_FACTOR * (HASH_FACTOR * (HASH_FACTOR * keyHash + b0) + b1) + b2;
                        n = buffer.getInt();
                        b0 = (byte) (n & 0xFF);
                        b1 = (byte) ((n >> 8) & 0xFF);
                        b2 = (byte) ((n >> 16) & 0xFF);
                        b3 = (byte) ((n >> 24) & 0xFF);
                        val = getVal(buffer, b0, b1, b2, b3);
                        break;
                    }
                    else {
                        UNSAFE.putInt(keyAddr, n);
                        keyArrLen++;
                        keyAddr += 4;
                        keyHash = HASH_FACTOR * (HASH_FACTOR * (HASH_FACTOR * (HASH_FACTOR * keyHash + b0) + b1) + b2) + b3;
                    }
                }
                var idx = keyHash & HASH_TBL_SIZE;
                var st = stats[idx];
                if (st == null) { // nothing in table, eagerly claim spot
                    st = stats[idx] = newStats(keyBaseAddr, keyArrLen, keyLastBytes, keyHash);
                }
                else if (!equals(st.keyAddr, st.keyLen, keyBaseAddr, keyArrLen)) {
                    st = findInTable(stats, keyHash, keyBaseAddr, keyArrLen, keyLastBytes);
                }
                st.min = Math.min(st.min, val);
                st.max = Math.max(st.max, val);
                st.sum += val;
                st.count++;
            }
        }
        catch (BufferUnderflowException ignore) {

        }
        return keyStart;
    }

    private static boolean equals(long key1, int len1, long key2, int len2) {
        if (len1 != len2) {
            return false;
        }
        for (long i = 0; i < len1; i++) {
            var offset = i << 2;
            if (UNSAFE.getInt(key1 + offset) != UNSAFE.getInt(key2 + offset)) {
                return false;
            }
        }
        return true;
    }

    private static int getVal(ByteBuffer buffer, byte b0, byte b1, byte b2, byte b3) {
        if (b0 == '-') {
            if (b2 != '.') { // 6 bytes: -dd.dn
                var b = buffer.get();
                buffer.get(); // newline
                return -(((b1 - '0') * 10 + (b2 - '0')) * 10 + (b - '0'));
            }
            else { // 5 bytes: -d.dn
                buffer.get(); // newline
                return -((b1 - '0') * 10 + (b3 - '0'));
            }
        }
        else {
            if (b1 != '.') { // 5 bytes: dd.dn
                buffer.get(); // newline
                return ((b0 - '0') * 10 + (b1 - '0')) * 10 + (b3 - '0');
            }
            else { // 4 bytes: d.dn
                return (b0 - '0') * 10 + (b2 - '0');
            }
        }
    }

    private static Stats findInTable(Stats[] stats, int hash, long keyAddr, int keyLen, int keyLastBytes) { // open-addressing scan
        var idx = hash & HASH_TBL_SIZE;
        var st = stats[idx];
        while (st != null && !equals(st.keyAddr, st.keyLen, keyAddr, keyLen)) {
            idx = (idx + 1) % (HASH_TBL_SIZE + 1);
            st = stats[idx];
        }
        if (st != null) {
            return st;
        }
        return stats[idx] = newStats(keyAddr, keyLen, keyLastBytes, hash);
    }

    private static Stats newStats(long keyAddr, int keyLen, int keyLastBytes, int hash) {
        var bytes = keyLen << 2;
        long k = UNSAFE.allocateMemory(bytes);
        UNSAFE.copyMemory(keyAddr, k, bytes);
        return new Stats(k, keyLen, keyLastBytes, hash);
    }

    private static byte[] readFooter(ByteBuffer buffer, int lineStart) { // read from line start to current pos (end-of-input)
        var footer = new byte[buffer.limit() - lineStart];
        buffer.get(lineStart, footer, 0, footer.length);
        return footer;
    }

    private static byte[] readHeader(ByteBuffer buffer) { // read up to and including first newline (or end-of-input)
        while (buffer.hasRemaining() && buffer.get() != '\n')
            ;
        var header = new byte[buffer.position()];
        buffer.get(0, header, 0, header.length);
        return header;
    }

    private static class Partition {
        byte[] header;
        byte[] footer;
        Stats[] stats;

        Partition(byte[] header, byte[] footer, Stats[] stats) {
            this.header = header;
            this.footer = footer;
            this.stats = stats;
        }
    }

    private static class Stats { // min, max, and sum values are modeled with integral types that represent tenths of a unit
        final long keyAddr; // address of 4-byte integer array
        final int keyLen; // number of 4-byte integers starting at address
        final int lastBytes; // number of bytes packed into last key int (1, 2, 3 or 4)
        final int hash;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum;
        long count;

        Stats(long keyAddr, int keyLen, int lastBytes, int hash) {
            this.keyAddr = keyAddr;
            this.keyLen = keyLen;
            this.lastBytes = lastBytes;
            this.hash = hash;
        }
    }
}
