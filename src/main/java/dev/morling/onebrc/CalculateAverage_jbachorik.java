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

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class CalculateAverage_jbachorik {
    private static final class Key {
        final ByteBuffer bb;
        final int offset;
        final int len;
        final long v0, v1;
        final int hash;

        Key(ByteBuffer bb, int offset, int len, long v0, long v1, int hash) {
            this.bb = bb;
            this.offset = offset;
            this.len = len;
            this.v0 = v0;
            this.v1 = v1;
            this.hash = hash;
        }

        public boolean equals(int offset, int len, long v0, long v1) {
            // byte[] bytes = new byte[len];
            // bb.get(offset, bytes);
            // String str = new String(bytes);

            if (((this.len ^ len) | (this.v0 ^ v0) | (this.v1 ^ v1)) != 0) {
                return false;
            }
            for (int i = 0; i < len - 8; i += 8) {
                if (bb.getLong(this.offset + i) != bb.getLong(offset + i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            byte[] bytes = new byte[len];
            bb.get(offset, bytes);
            return new String(bytes);
        }
    }

    private static final class Stats {
        long min;
        long max;
        long count;
        long sum;

        Stats() {
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            count = 0;
            sum = 0;
        }

        Stats add(long value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
            sum += value;
            return this;
        }

        Stats merge(Stats other) {
            synchronized (this) {
                min = Math.min(min, other.min);
                max = Math.max(max, other.max);
                count += other.count;
                sum += other.sum;
            }
            return this;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0d, sum / (double) count / 10.0d, max / 10.0d);
        }
    }

    private static final class StatsMap {
        private static class StatsHolder {
            private final Key key;
            private final Stats stats;

            StatsHolder(Key slice, Stats stats) {
                this.key = slice;
                this.stats = stats;
            }

            @Override
            public String toString() {
                return "StatsHolder{" +
                        "key=" + key +
                        ", stats=" + stats +
                        '}';
            }
        }

        private static final int BUCKETS = 65536;
        private static final int BUCKET_SIZE = 16;
        private final StatsHolder[][] map = new StatsHolder[BUCKETS][BUCKET_SIZE];

        public Stats getOrInsert(ByteBuffer buffer, int offset, int len, int idx, long v0, long v1) {
            StatsHolder[] bucket = map[idx];
            int bucketOffset = 0;
            do {
                StatsHolder statsHolder = bucket[bucketOffset];
                if (statsHolder == null) {
                    Stats stats = new Stats();
                    bucket[bucketOffset] = new StatsHolder(new Key(buffer, offset, len, v0, v1, idx), stats);
                    return stats;
                }
                if (statsHolder.key.equals(offset, len, v0, v1)) {
                    return statsHolder.stats;
                }
                bucketOffset++;
            } while (bucketOffset < BUCKET_SIZE - 1);
            throw new Error("Bucket overflow");
        }

        public void forEach(BiConsumer<Key, Stats> consumer) {
            for (StatsHolder[] bucket : map) {
                for (StatsHolder statsHolder : bucket) {
                    if (statsHolder != null) {
                        consumer.accept(statsHolder.key, statsHolder.stats);
                    }
                }
            }
        }
    }

    private static final long newLinePattern = compilePattern((byte) '\n');
    private static final long semiPattern = compilePattern((byte) ';');

    public static void main(String[] args) throws Exception {
        int workers = Runtime.getRuntime().availableProcessors();
        if (args.length == 1) {
            workers = Integer.parseInt(args[0]);
        }
        Map<String, Stats> map = new TreeMap<>();
        File f = new File("measurements.txt");

        try (FileInputStream fis = new FileInputStream(f)) {
            FileChannel fc = fis.getChannel();
            int granularity = 32 * 1024 * 1024;
            int targetWorkers = Math.min(Math.max(1, (int) (fc.size() / granularity)), workers);
            long chunkSize = fc.size() / targetWorkers;
            ExecutorService workerPool = Executors.newFixedThreadPool(workers);
            // System.out.println("Chunk size: " + chunkSize + ", workers: " + targetWorkers);
            for (ByteBuffer bb : mmap(fc, (int) chunkSize)) {
                workerPool.submit(() -> {
                    try {
                        StatsMap data = processChunk(bb);
                        synchronized (map) {
                            data.forEach((k, v) -> {
                                String str = k.toString();
                                map.merge(str, v, Stats::merge);
                            });
                        }
                    }
                    catch (Throwable t) {
                        t.printStackTrace();
                    }
                });
            }
            workerPool.shutdown();
            workerPool.awaitTermination(1, TimeUnit.HOURS);
        }
        finally {
            // System.out.println("Keys: " + map.size());
            System.out.println(map);
        }
    }

    // unrolled FNV-1a 64 bit hash
    private static final long fnv64OffsetBasis = 0xCBF29CE484222325L;
    private static final long fnv64Prime = 0x100000001B3L;

    private static StatsMap processChunk(ByteBuffer bb) {
        StatsMap map = new StatsMap();

        int offset = 0;
        int limit = bb.limit();
        int readLimit = limit - 8;
        long v0 = 0;
        long v1 = 0;
        long hashCode = fnv64OffsetBasis;
        int lastNewLine = -1;

        while (offset < limit) {
            if (offset > readLimit) {
                int over = offset - readLimit;
                v1 = bb.getLong(limit - 8);
                v1 = v1 << (over * 8);
            }
            else {
                v1 = bb.getLong(offset);
            }
            long x = preprocess(v1, newLinePattern);
            if (x != 0) {
                long value = 0;
                int valueLen = 0;
                int pos = Long.numberOfLeadingZeros(x) >>> 3;
                int yoffset = offset;
                int semiPos = firstInstance(v1, semiPattern);
                if (semiPos >= pos) {
                    yoffset -= 8;
                    semiPos = firstInstance(v0, semiPattern);
                    // semiPos will be at least 3 (new line is in the upper word and the value has at most 5 bytes)
                    // a 64 bit value can not be rotated by 64 bits to 'clear' the bits
                    // instead, it must be rotated by at most 56 bits and then, if 64 bit rotation was requested, by 8 bits more
                    int rot2 = (8 - semiPos) >>> 3;
                    int rot1 = (7 - semiPos) + (~rot2 & 0x1);
                    long mask = ((0xFFFFFFFFFFFFFFFFL << (rot1 * 8)) << (rot2 * 8));
                    rot2 = (8 - pos) >>> 3;
                    rot1 = (7 - pos) + (~rot2 & 0x1);
                    long newlineMask = ((0xFFFFFFFFFFFFFFFFL << (rot1 * 8)) << (rot2 * 8));
                    value = semiPos == 7 ? 0L : (v0 << (semiPos + 1) * 8);
                    value |= ((v1 & newlineMask) >> (7 - semiPos) * 8);
                    // right-align the value bytes
                    // getting the number of trailing zeros is the easiest way to figure out the shift
                    // should be sufficiently fast but ...
                    int zeros = (Long.numberOfTrailingZeros(value) >>> 3);
                    value = value >>> zeros * 8;
                    valueLen = 8 - zeros;
                    v0 = v0 & mask;
                }
                else {
                    hashCode ^= v0;
                    hashCode *= fnv64Prime;
                    long valMask = (0xFFFFFFFFFFFFFFFFL << (7 - semiPos) * 8);
                    v0 = v1 & valMask;
                    value = v1 & ~valMask;
                    value = value >> (8 - pos) * 8;
                    valueLen = pos - semiPos - 1;
                }
                v1 = 0;
                hashCode ^= v0;
                hashCode *= fnv64Prime;

                int len = (yoffset + semiPos - 1) - lastNewLine;
                hashCode ^= len;
                hashCode *= fnv64Prime;

                // byte[] strBuf = new byte[len];
                // bb.get(lastNewLine + 1, strBuf);
                // String str = new String(strBuf);
                // System.out.println("===> " + str + ": " + Long.toHexString(value) + " :: " + fastParse(value, valueLen));
                // projection of the hash code to 32 bits -> 65k buckets
                long idx = ((hashCode & 0xFFFFFFFF00000000L) >> 32) ^ (hashCode & 0x00000000FFFFFFFFL);
                idx = ((idx & 0x00000000FFFF0000L) >> 16) ^ (idx & 0x000000000000FFFFL);
                map.getOrInsert(bb, lastNewLine + 1, len, (int) idx, v0, v1).add(fastParse(value, valueLen));

                offset += pos + 1;
                lastNewLine = offset - 1;
                // reset the previous value
                v0 = 0;
                // reset the hash
                hashCode = fnv64OffsetBasis;
            }
            else {
                offset += 8;
                hashCode ^= v0;
                hashCode *= fnv64Prime;
                v0 = v1;
            }
        }
        return map;
    }

    private static final long fastParserMask = 0x3030303030303030L;

    private static int fastParse(long word, int len) {
        assert (len <= 5);

        int signChar = (int) (word >> ((len - 1) * 8)) & 0xFF;
        int sign = signChar ^ 0x2d;
        int base = ~(sign | -sign);
        int offset = (base >> 7) & 0x01;
        int multiplier = -(~((sign - 1) >> 31) | 0x1);
        int shift = (8 - len + offset) * 8;
        long mask = (0xFFFFFFFFFFFFFFFFL >>> shift);
        word = (word ^ fastParserMask) & mask;

        int v1 = (int) word & 0xff;
        // skip decimal point
        // int v2 = 10 * ((int) (word >> 8) & 0xff);
        int v3 = 10 * ((int) (word >> 16) & 0xff);
        int v4 = 100 * ((int) (word >> 24) & 0xff);
        // v5 is either the sign or not used

        return ((v1 + v3 + v4) * multiplier);
    }

    private static ByteBuffer[] mmap(FileChannel fc, int splitSize) throws Exception {
        if (fc.size() > splitSize && splitSize < 128) {
            throw new IllegalArgumentException("Split size must be at least 128 bytes");
        }

        byte[] byteBuffer = new byte[128];
        int chunks = (int) (fc.size() / splitSize) + 1;
        ByteBuffer[] buffers = new ByteBuffer[chunks];
        long remaining = fc.size();
        int count = 0;
        for (int j = 0; j < chunks; j++) {
            if (remaining > splitSize) {
                ByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, fc.size() - remaining, splitSize);
                buffer.get(splitSize - 128, byteBuffer, 0, 128);
                int adjust = -1;
                for (int i = 0; i < 128; i++) {
                    if (byteBuffer[127 - i] == '\n') {
                        adjust = i;
                        break;
                    }
                }
                assert (adjust != -1);
                int size = splitSize - adjust;
                buffers[j] = fc.map(FileChannel.MapMode.READ_ONLY, fc.size() - remaining, size);
                remaining -= size;
                count = j + 1;
            }
            else {
                count = j + 1;
                if (fc.size() < 8) {
                    // slow-path
                    ByteBuffer bb = ByteBuffer.allocate(8);
                    fc.read(bb, 0);
                    buffers[j] = bb;
                    break;
                }
                buffers[j] = fc.map(FileChannel.MapMode.READ_ONLY, fc.size() - remaining, remaining);
                break;
            }
        }
        // System.out.println("Chunks: " + count);
        return count < chunks ? Arrays.copyOf(buffers, count) : buffers;
    }

    private static long compilePattern(byte byteToFind) {
        long pattern = byteToFind & 0xFFL;
        return pattern
                | (pattern << 8)
                | (pattern << 16)
                | (pattern << 24)
                | (pattern << 32)
                | (pattern << 40)
                | (pattern << 48)
                | (pattern << 56);
    }

    private static int firstInstance(long word, long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
        return Long.numberOfLeadingZeros(tmp) >>> 3;
    }

    private static long preprocess(long word, long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
        return tmp;
    }
}
