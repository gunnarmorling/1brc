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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

public class CalculateAverage_ebarlas {

    private static final int MAX_KEY_SIZE = 100;
    private static final int HASH_FACTOR = 433;
    private static final int HASH_TBL_SIZE = 16_383; // range of allowed hash values, inclusive

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: java CalculateAverage <input-file> <partitions>");
            System.exit(1);
        }
        var path = Paths.get(args[0]);
        var numPartitions = Integer.parseInt(args[1]);
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
                var key = new String(st.key, StandardCharsets.UTF_8);
                result.put(key, format(st));
            }
        }
        System.out.println(result);
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
                    var t = findInTable(target, current[j].hash, current[j].key, current[j].key.length);
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
            if (merged != null) {
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
        var keyBuf = new byte[MAX_KEY_SIZE]; // buffer for key
        int keyStart = 0; // start of key in buffer used for footer calc
        try { // abort with exception to allow optimistic line processing
            while (true) { // one line per iteration
                keyStart = buffer.position(); // preserve line start
                int n = buffer.getInt(); // first four bytes of key
                byte b1 = (byte) (n & 0xFF);
                byte b2 = (byte) ((n >> 8) & 0xFF);
                byte b3 = (byte) ((n >> 16) & 0xFF);
                byte b = (byte) ((n >> 24) & 0xFF);
                int keyPos;
                int keyHash = keyBuf[0] = b1;
                if (b2 != ';' && b3 != ';') { // true for keys of length 3 or more
                    keyBuf[1] = b2;
                    keyBuf[2] = b3;
                    keyHash = HASH_FACTOR * (HASH_FACTOR * keyHash + b2) + b3;
                    keyPos = 3;
                    while (b != ';') {
                        keyHash = HASH_FACTOR * keyHash + b;
                        keyBuf[keyPos++] = b;
                        b = buffer.get();
                    }
                }
                else { // slow path, rewind and consume byte-by-byte
                    buffer.position(keyStart + 1);
                    keyPos = 1;
                    while ((b = buffer.get()) != ';') {
                        keyHash = HASH_FACTOR * keyHash + b;
                        keyBuf[keyPos++] = b;
                    }
                }
                var idx = keyHash & HASH_TBL_SIZE;
                var st = stats[idx];
                if (st == null) { // nothing in table, eagerly claim spot
                    st = stats[idx] = newStats(keyBuf, keyPos, keyHash);
                }
                else if (!Arrays.equals(st.key, 0, st.key.length, keyBuf, 0, keyPos)) {
                    st = findInTable(stats, keyHash, keyBuf, keyPos);
                }
                var value = buffer.getInt();
                b = (byte) (value & 0xFF); // digit or dash
                int val;
                if (b == '-') { // dash branch
                    val = ((byte) ((value >> 8) & 0xFF)) - '0'; // digit after dash
                    b = (byte) ((value >> 16) & 0xFF); // second digit or decimal
                    if (b != '.') { // second digit
                        val = val * 10 + (b - '0'); // calc second digit
                        // skip decimal (at >> 24)
                        b = buffer.get(); // digit after decimal
                        val = val * 10 + (b - '0'); // calc digit after decimal
                    }
                    else { // decimal branch
                           // skip decimal (at >> 16)
                        b = (byte) ((value >> 24) & 0xFF); // digit after decimal
                        val = val * 10 + (b - '0'); // calc digit after decimal
                    }
                    buffer.get(); // newline
                    val = -val;
                }
                else { // first digit branch
                    val = b - '0'; // calc first digit
                    b = (byte) ((value >> 8) & 0xFF); // second digit or decimal
                    if (b != '.') { // second digit branch
                        val = val * 10 + (b - '0'); // calc second digit
                        // skip decimal (at >> 16)
                        b = (byte) ((value >> 24) & 0xFF); // digit after decimal
                        val = val * 10 + (b - '0'); // calc digit after decimal
                        buffer.get(); // newline
                    }
                    else { // decimal branch
                        b = (byte) ((value >> 16) & 0xFF); // digit after decimal
                        val = val * 10 + (b - '0'); // calc digit after decimal
                        // skip newline (at >> 24)
                    }
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

    private static Stats findInTable(Stats[] stats, int hash, byte[] key, int len) { // open-addressing scan
        var idx = hash & HASH_TBL_SIZE;
        var st = stats[idx];
        while (st != null && !Arrays.equals(st.key, 0, st.key.length, key, 0, len)) {
            idx = (idx + 1) % (HASH_TBL_SIZE + 1);
            st = stats[idx];
        }
        if (st != null) {
            return st;
        }
        return stats[idx] = newStats(key, len, hash);
    }

    private static Stats newStats(byte[] buffer, int len, int hash) {
        var k = new byte[len];
        System.arraycopy(buffer, 0, k, 0, len);
        return new Stats(k, hash);
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
        final byte[] key;
        final int hash;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum;
        long count;

        Stats(byte[] key, int hash) {
            this.key = key;
            this.hash = hash;
        }
    }
}
