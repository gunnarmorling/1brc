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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_parkertimmins {
    private static final String FILE = "./measurements.txt";

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

            // key always stored as multiple of 32 bytes
            byte[] key;
            byte keyLen;
            short min = Short.MAX_VALUE;
            short max = Short.MIN_VALUE;
            long sum = 0;
            long count = 0;

            void merge(Entry other) {
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

                    int rem = sLen % 32;
                    int arrayLen = rem == 0 ? sLen : sLen + 32 - rem;
                    entry.key = Arrays.copyOf(buf, arrayLen);
                    Arrays.fill(entry.key, sLen, arrayLen, (byte) 0);
                    entry.keyLen = (byte) sLen;

                    entry.min = entry.max = val;
                    entry.sum += val;
                    entry.count++;
                    break;
                }
                else {
                    if (entry.keyLen == sLen && eq(buf, entry.key, entry.keyLen)) {
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

    static boolean eq(byte[] buf, byte[] entryKey, int sLen) {
        int needed = sLen;
        for (int offset = 0; offset <= 96; offset += 32) {
            var a = ByteVector.fromArray(ByteVector.SPECIES_256, buf, offset);
            var b = ByteVector.fromArray(ByteVector.SPECIES_256, entryKey, offset);
            int matches = a.eq(b).not().firstTrue();
            if (needed <= 32) {
                return matches >= needed;
            }
            else if (matches < 32) {
                return false;
            }
            needed -= 32;
        }
        return false;
    }

    static long findNextEntryStart(MemorySegment ms, long offset) {
        long curr = offset;
        while (ms.get(ValueLayout.JAVA_BYTE, curr) != '\n') {
            curr++;
        }
        curr++;
        return curr;
    }

    static short[] digits2s = new short[256];
    static short[] digits1s = new short[256];
    static short[] digits0s = new short[256];

    static {
        for (int i = 0; i < 10; ++i) {
            digits2s[i + ((int) '0')] = (short) (i * 100);
            digits1s[i + ((int) '0')] = (short) (i * 10);
            digits0s[i + ((int) '0')] = (short) i;
        }
    }

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
            int d0 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 1));
            int d1 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 3));
            int d2 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 4)); // could be - or \n
            int base = digits0s[d0] + digits1s[d1] + digits2s[d2];
            short temp = (short) (neg ? -base : base);

            localAgg.add(buf, sLen, temp, hash);
            curr = tempIdx + len + 1;
        }
    }

    static int hash(byte[] buf, int sLen) {
        int shift = Math.max(0, 8 - sLen) << 3;
        long mask = (~0L) >>> shift;
        long val = ((buf[7] & 0xffL) << 56) | ((buf[6] & 0xffL) << 48) | ((buf[5] & 0xffL) << 40) | ((buf[4] & 0xffL) << 32) | ((buf[3] & 0xffL) << 24)
                | ((buf[2] & 0xffL) << 16) | ((buf[1] & 0xFFL) << 8) | (buf[0] & 0xffL);
        val &= mask;
        // lemire: https://lemire.me/blog/2023/07/14/recognizing-string-prefixes-with-simd-instructions/
        int hash = (int) (((((val >> 32) ^ val) & 0xffffffffL) * 3523216699L) >> 32);
        return hash;
    }

    static void processRangeSIMD(MemorySegment ms, boolean isFirst, boolean isLast, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128];

        long curr = isFirst ? start : findNextEntryStart(ms, start);
        long limit = isLast ? end - padding : end;

        while (curr < limit) {
            int nl = 0;
            for (int offset = 0; offset < 128; offset += 32) {
                ByteVector section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr + offset, ByteOrder.LITTLE_ENDIAN);
                section.intoArray(buf, offset);
                var idx = section.eq((byte) '\n').firstTrue();
                if (idx != 32) {
                    nl = offset + idx;
                    break;
                }
            }

            int nl1 = buf[nl - 1];
            int nl3 = buf[nl - 3];
            int nl4 = buf[nl - 4];
            int nl5 = buf[nl - 5];
            int base = (nl1 - '0') + 10 * (nl3 - '0') + digits2s[nl4];
            boolean neg = nl4 == '-' || (nl4 != ';' && nl5 == '-');
            short temp = (short) (neg ? -base : base);
            int tempLen = 4 + (neg ? 1 : 0) + (base >= 100 ? 1 : 0);
            int semi = nl - tempLen;

            int hash = hash(buf, semi);
            localAgg.add(buf, semi, temp, hash);
            curr += (nl + 1);
        }

        // last batch is near end of file, process without SIMD to avoid out-of-bounds
        if (isLast) {
            processRangeScalar(ms, curr, end, localAgg);
        }
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
                String station = new String(entry.key, 0, entry.keyLen, StandardCharsets.UTF_8); // for UTF-8 encoding
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

    static final int padding = 200; // max entry size is 107ish == 100 (station) + 1 (semicolon) + 5 (temp, eg -99.9) + 1 (newline)

    public static void main(String[] args) throws IOException, InterruptedException {
        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        int numThreads = Runtime.getRuntime().availableProcessors();

        final long batchSize = 10_000_000;

        final long fileSize = channel.size();
        // final long batchSize = fileSize / numThreads + 1;
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
                    final boolean isFirstBatch = startBatch == 0;
                    final boolean isLastBatch = endBatch == fileSize;
                    processRangeSIMD(ms, isFirstBatch, isLastBatch, startBatch, endBatch, localAgg);
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
