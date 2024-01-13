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
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.lang.foreign.ValueLayout;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

public class CalculateAverage_parkertimmins {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./full_measurements.no_license";

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    static class OpenHashTable {
        static class Entry {
            byte[] key;
            short min;
            short max;
            long sum = 0;
            long count = 0;
            int hash;

            void merge(OpenHashTable.Entry other) {
                min = (short) Math.min(min, other.min);
                max = (short) Math.max(max, other.max);
                sum += other.sum;
                count += other.count;
            }
        }

        static final int bits = 14;
        static final int tableSize = 1 << bits; // 16k
        static final int mask = tableSize - 1;
        final Entry[] entries = new Entry[tableSize];

        void add(byte[] buf, int sLen, short val, int hash) {
            int idx = hash & mask;

            while (true) {
                Entry entry = entries[idx];

                // key not present, so add it
                if (entry == null) {
                    entry = entries[idx] = new Entry();
                    entry.key = Arrays.copyOf(buf, sLen);
                    entry.min = entry.max = val;
                    entry.sum += val;
                    entry.count++;
                    entry.hash = hash;
                    break;
                }
                else {
                    if (entry.hash == hash && entry.key.length == sLen && Arrays.equals(entry.key, 0, sLen, buf, 0, sLen)) {
                        entry.min = (short) Math.min(entry.min, val);
                        entry.max = (short) Math.max(entry.max, val);
                        entry.sum += val;
                        entry.count++;
                        break;
                    }
                    else {
                        idx = (idx + 1) & mask;
                    }
                }
            }
        }
    }

    static long findNextEntryStart(MemorySegment ms, long offset) {
        long curr = offset;
        while (ms.get(ValueLayout.JAVA_BYTE, curr) != '\n') {
            curr++;
        }
        curr++;
        return curr;
    }

    static short[] digits10s = { 0, 100, 200, 300, 400, 500, 600, 700, 800, 900 };
    static short[] digits1s = { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 };

    static void processRangeScalar(MemorySegment ms, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128];

        long curr = start;
        long limit = end;

        while (curr < limit) {
            int i = 0;
            byte val = ms.get(ValueLayout.JAVA_BYTE, curr);
            while (val != ';') {
                buf[i++] = val;
                curr++;
                val = ms.get(ValueLayout.JAVA_BYTE, curr);
            }

            int sLen = i;
            int hash = hash(buf, sLen);

            curr++; // skip semicolon

            long tempIdx = curr;
            boolean neg = ms.get(ValueLayout.JAVA_BYTE, tempIdx) == '-';
            boolean twoDig = ms.get(ValueLayout.JAVA_BYTE, tempIdx + 1 + (neg ? 1 : 0)) == '.';
            int len = 3 + (neg ? 1 : 0) + (twoDig ? 0 : 1);
            int d0 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 1)) - '0';
            int d1 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 3)) - '0';
            int base = d0 + digits1s[d1] + (twoDig ? 0 : digits10s[((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 4)) - '0']);
            short temp = (short) (neg ? -base : base);

            localAgg.add(buf, sLen, temp, hash);
            curr = tempIdx + len + 1;
        }
    }

    static int hash(byte[] buf, int sLen) {
        // TODO find a hash that works directly from byte array
        // if shorter than 8 chars, mask out upper bits
        long mask = sLen < 8 ? -(1L << ((8 - sLen) << 3)) : 0xFFFFFFFFL;
        long val = ((buf[0] & 0xffL) << 56) | ((buf[1] & 0xffL) << 48) | ((buf[2] & 0xffL) << 40) | ((buf[3] & 0xffL) << 32) | ((buf[4] & 0xffL) << 24)
                | ((buf[5] & 0xffL) << 16) | ((buf[6] & 0xFFL) << 8) | (buf[7] & 0xffL);
        val &= mask;

        // also worth trying: https://lemire.me/blog/2015/10/22/faster-hashing-without-effort/
        // lemire: https://lemire.me/blog/2023/07/14/recognizing-string-prefixes-with-simd-instructions/
        int hash = (int) (((((val >> 32) ^ val) & 0xffffffffL) * 3523216699L) >> 32);
        return hash;
    }

    static void processRangeSIMD(MemorySegment ms, boolean frontPad, boolean backPad, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128];

        long curr = frontPad ? findNextEntryStart(ms, start) : start;
        long limit = end - padding;

        var needle = ByteVector.broadcast(ByteVector.SPECIES_256, ';');
        while (curr < limit) {

            int segStart = 0;
            int sLen;

            while (true) {
                var section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr + segStart, ByteOrder.LITTLE_ENDIAN);
                section.intoArray(buf, segStart);
                VectorMask<Byte> matches = section.compare(VectorOperators.EQ, needle);
                int idx = matches.firstTrue();
                if (idx != 32) {
                    sLen = segStart + idx;
                    break;
                }
                segStart += 32;
            }

            int hash = hash(buf, sLen);

            curr += sLen;
            curr++; // semicolon

            long tempIdx = curr;
            boolean neg = ms.get(ValueLayout.JAVA_BYTE, tempIdx) == '-';
            boolean twoDig = ms.get(ValueLayout.JAVA_BYTE, tempIdx + 1 + (neg ? 1 : 0)) == '.';
            int len = 3 + (neg ? 1 : 0) + (twoDig ? 0 : 1);
            int d0 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 1)) - '0';
            int d1 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 3)) - '0';
            int base = d0 + digits1s[d1] + (twoDig ? 0 : digits10s[((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 4)) - '0']);
            short temp = (short) (neg ? -base : base);

            localAgg.add(buf, sLen, temp, hash);
            curr = tempIdx + len + 1;
        }

        // last batch is near end of file, process without SIMD to avoid out-of-bounds
        if (!backPad) {
            processRangeScalar(ms, curr, end, localAgg);
        }
    }

    /**
     *  For debugging issues with hash function
      */
    static void checkHashDistributionQuality(ArrayList<OpenHashTable> localAggs) {
        HashSet<Integer> uniquesHashValues = new HashSet<Integer>();
        HashSet<String> uniqueCities = new HashSet<String>();
        HashMap<String, HashSet<Integer>> cityToHash = new HashMap<>();

        for (var agg : localAggs) {
            for (OpenHashTable.Entry entry : agg.entries) {
                if (entry == null) {
                    continue;
                }
                uniquesHashValues.add(entry.hash);
                String station = new String(entry.key, StandardCharsets.UTF_8); // for UTF-8 encoding
                uniqueCities.add(station);

                if (!cityToHash.containsKey(station)) {
                    cityToHash.put(station, new HashSet<>());
                }
                cityToHash.get(station).add(entry.hash);
            }
        }

        for (var pair : cityToHash.entrySet()) {
            if (pair.getValue().size() > 1) {
                System.err.println("multiple hashes: " + pair.getKey() + " " + pair.getValue());
            }
        }

        System.err.println("Unique stations: " + uniqueCities.size() + ", unique hash values: " + uniquesHashValues.size());
    }

    /**
     * Combine thread local values
     */
    static HashMap<String, OpenHashTable.Entry> mergeAggregations(ArrayList<OpenHashTable> localAggs) {
        HashMap<String, OpenHashTable.Entry> global = new HashMap<>();
        for (var agg : localAggs) {
            for (OpenHashTable.Entry entry : agg.entries) {
                if (entry == null) {
                    continue;
                }
                String station = new String(entry.key, StandardCharsets.UTF_8); // for UTF-8 encoding
                var currentVal = global.get(station);
                if (currentVal != null) {
                    currentVal.merge(entry);
                }
                else {
                    global.put(station, entry);
                }
            }
        }
        return global;
    }

    static final long batchSize = 10_000_000;

    static final int padding = 200; // max entry size is 107ish == 100 (station) + 1 (semicolon) + 5 (temp, eg -99.9) + 1 (newline)

    public static void main(String[] args) throws IOException, InterruptedException {
        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        int numThreads = Runtime.getRuntime().availableProcessors();

        final long fileSize = channel.size();
        final MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
        final ArrayList<OpenHashTable> localAggs = new ArrayList<>(numThreads);
        Thread[] threads = new Thread[numThreads];
        final AtomicLong progress = new AtomicLong(0);

        class Task implements Runnable {
            final int threadId;

            Task(int threadId) {
                this.threadId = threadId;
            }

            @Override
            public void run() {
                var localAgg = localAggs.get(threadId);
                while (true) {
                    final long startBatch = progress.getAndAdd(batchSize);
                    if (startBatch >= fileSize) {
                        break;
                    }
                    final long endBatch = Math.min(startBatch + batchSize, fileSize);
                    final boolean first = startBatch == 0;
                    final boolean frontPad = !first;
                    final boolean last = endBatch == fileSize;
                    final boolean backPad = !last;
                    processRangeSIMD(ms, frontPad, backPad, startBatch, endBatch, localAgg);
                }
            }
        }

        for (int t = 0; t < numThreads; t++) {
            localAggs.add(new OpenHashTable());
            threads[t] = new Thread(new Task(t), "Thread-" + t);
            threads[t].start();
        }

        for (var thread : threads) {
            thread.join();
        }

        var globalAggs = mergeAggregations(localAggs);

        Map<String, ResultRow> res = new TreeMap<>();
        for (Map.Entry<String, OpenHashTable.Entry> entry : globalAggs.entrySet()) {
            final var ma = entry.getValue();
            res.put(entry.getKey(), new ResultRow(ma.min / 10.0, (ma.sum / 10.0) / ma.count, ma.max / 10.0));
        }
        System.out.println(res);
    }
}
