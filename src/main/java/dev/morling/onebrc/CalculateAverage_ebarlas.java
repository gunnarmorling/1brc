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
                    var buffer = channel.map(FileChannel.MapMode.READ_ONLY, pStart, pSize);
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
                    doProcessBuffer(ByteBuffer.wrap(merged), true, pPrev.stats);
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
        var readingKey = true; // reading key or value?
        var keyBuf = new byte[MAX_KEY_SIZE]; // buffer for key
        var keyPos = 0; // current position in key buffer
        var keyHash = 0; // accumulating hash of key
        var keyStart = buffer.position(); // start of key in buffer used for footer calc
        var negative = false; // is value negative?
        var val = 0; // accumulating value
        Stats st = null;
        while (buffer.hasRemaining()) {
            var b = buffer.get();
            if (readingKey) {
                if (b != ';') {
                    keyHash = HASH_FACTOR * keyHash + b;
                    keyBuf[keyPos++] = b;
                }
                else {
                    var idx = keyHash & HASH_TBL_SIZE;
                    st = stats[idx];
                    if (st == null) { // nothing in table, eagerly claim spot
                        st = stats[idx] = newStats(keyBuf, keyPos, keyHash);
                    }
                    else if (!Arrays.equals(st.key, 0, st.key.length, keyBuf, 0, keyPos)) {
                        st = findInTable(stats, keyHash, keyBuf, keyPos);
                    }
                    readingKey = false;
                }
            }
            else {
                if (b == '\n') {
                    var v = negative ? -val : val;
                    st.min = Math.min(st.min, v);
                    st.max = Math.max(st.max, v);
                    st.sum += v;
                    st.count++;
                    readingKey = true;
                    keyHash = 0;
                    val = 0;
                    negative = false;
                    keyStart = buffer.position();
                    keyPos = 0;
                }
                else if (b == '-') {
                    negative = true;
                }
                else if (b != '.') { // skip '.' since fractional tenth unit after decimal point is assumed
                    val = val * 10 + (b - '0');
                }
            }
        }
        var footer = keyStart < buffer.position() ? readFooter(buffer, keyStart) : null;
        return new Partition(header, footer, stats);
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
        var footer = new byte[buffer.position() - lineStart];
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
