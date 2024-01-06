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
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class CalculateAverage_jbachorik {
    interface Sliceable {
        void reset();

        void get(byte[] bytes);

        byte get();

        int getInt();

        short getShort();

        long getLong();

        int len();
    }

    private static final class ByteBufferSlice implements Sliceable {
        private final ByteBuffer buffer;
        private final int len;

        public ByteBufferSlice(ByteBuffer buffer, int offset, int len) {
            this.buffer = buffer.slice(offset, len);
            this.len = len;
        }

        @Override
        public void reset() {
            buffer.rewind();
        }

        @Override
        public void get(byte[] bytes) {
            buffer.get(bytes);
        }

        @Override
        public byte get() {
            return buffer.get();
        }

        @Override
        public int getInt() {
            return buffer.getInt();
        }

        @Override
        public long getLong() {
            return buffer.getLong();
        }

        @Override
        public int len() {
            return len;
        }

        @Override
        public short getShort() {
            return buffer.getShort();
        }
    }

    private static final class FastSlice implements Sliceable {
        final ByteBuffer buffer;
        final int offset;
        final int len;

        public FastSlice(ByteBuffer buffer, int offset, int len) {
            this.buffer = buffer;
            this.offset = offset;
            this.len = len;
        }

        public void reset() {
            buffer.position(offset);
        }

        public void get(byte[] bytes) {
            buffer.get(bytes);
        }

        public byte get() {
            return buffer.get();
        }

        public int getInt() {
            return buffer.getInt();
        }

        public long getLong() {
            return buffer.getLong();
        }

        public int len() {
            return len;
        }

        @Override
        public short getShort() {
            return buffer.getShort();
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
            private final Sliceable slice;
            private final Stats stats;

            StatsHolder(Sliceable slice, Stats stats) {
                this.slice = slice;
                this.stats = stats;
            }
        }

        private static final int BUCKETS = 1264532;
        private static final int BUCKET_SIZE = 4;
        private final StatsHolder[][] map = new StatsHolder[BUCKETS][BUCKET_SIZE];

        public Stats getOrInsert(ByteBuffer buffer, int len) {
            int idx = bucketIndex(buffer, len);
            int target = buffer.position();
            Sliceable slice = new FastSlice(buffer, buffer.position() - len, len);
            try {
                StatsHolder[] bucket = map[idx];
                if (bucket[0] == null) {
                    Stats stats = new Stats();
                    bucket[0] = new StatsHolder(slice, stats);
                    return stats;
                }
                int offset = 0;
                while (offset < BUCKET_SIZE && bucket[offset] != null && !equals(bucket[offset].slice, slice)) {
                    offset++;
                }
                assert (offset <= BUCKET_SIZE);
                if (bucket[offset] != null) {
                    return bucket[offset].stats;
                } else {
                    Stats stats = new Stats();
                    bucket[offset] = new StatsHolder(slice, stats);
                    return stats;
                }
            } finally {
                buffer.position(target);
            }
        }

        private final long[] leftBuffer = new long[16]; // max 128 bytes
        private final long[] rightBuffer = new long[16]; // max 128 bytes

        private boolean equals(Sliceable leftSlice, Sliceable rightSlice) {
            if (leftSlice.len() != rightSlice.len()) {
                return false;
            }

            int pos = 0;

            int limit = leftSlice.len();
            int lpos = pos;
            int rpos = pos;
            int bufPos = 0;
            leftSlice.reset();
            while (lpos < limit - 7) {
                leftBuffer[bufPos] = leftSlice.getLong();
                lpos += 8;
                bufPos++;
            }
            if (lpos < limit - 4) {
                leftBuffer[bufPos] = (long)leftSlice.getInt() << 32;
                lpos += 4;
            }
            if (lpos < limit -2) {
                leftBuffer[bufPos] |= (long)leftSlice.getShort() << 16;
                lpos += 2;
            }
            if (lpos < limit) {
                leftBuffer[bufPos] |= (long)leftSlice.get() << 8;
            }
            rightSlice.reset();
            bufPos = 0;
            while (rpos < limit - 7) {
                rightBuffer[bufPos] = rightSlice.getLong();
                rpos += 8;
                bufPos++;
            }
            if (rpos < limit - 4) {
                rightBuffer[bufPos] = (long)rightSlice.getInt() << 32;
                rpos += 4;
            }
            if (rpos < limit -2) {
                rightBuffer[bufPos] |= (long)rightSlice.getShort() << 16;
                rpos += 2;
            }
            if (rpos < limit) {
                rightBuffer[bufPos] |= (long)rightSlice.get() << 8;
            }
//
//            long val = (((bufferMask[0] & leftBuffer[0]) ^ (bufferMask[0] & rightBuffer[0])) &
//                        ((bufferMask[1] & leftBuffer[1]) ^ (bufferMask[1] & rightBuffer[1])) |
//                        ((bufferMask[2] & leftBuffer[2]) ^ (bufferMask[2] & rightBuffer[2])) |
//                        ((bufferMask[3] & leftBuffer[3]) ^ (bufferMask[3] & rightBuffer[3])) |
//                        ((bufferMask[4] & leftBuffer[4]) ^ (bufferMask[4] & rightBuffer[4])) |
//                        ((bufferMask[5] & leftBuffer[5]) ^ (bufferMask[5] & rightBuffer[5])) |
//                        ((bufferMask[6] & leftBuffer[6]) ^ (bufferMask[6] & rightBuffer[6])) |
//                        ((bufferMask[7] & leftBuffer[7]) ^ (bufferMask[7] & rightBuffer[7])) |
//                        ((bufferMask[8] & leftBuffer[8]) ^ (bufferMask[8] & rightBuffer[8])) |
//                        ((bufferMask[9] & leftBuffer[9]) ^ (bufferMask[9] & rightBuffer[9])) |
//                        ((bufferMask[10] & leftBuffer[10]) ^ (bufferMask[10] & rightBuffer[10])) |
//                        ((bufferMask[11] & leftBuffer[11]) ^ (bufferMask[11] & rightBuffer[11])) |
//                        ((bufferMask[12] & leftBuffer[12]) ^ (bufferMask[12] & rightBuffer[12])) |
//                        ((bufferMask[13] & leftBuffer[13]) ^ (bufferMask[13] & rightBuffer[13])) |
//                        ((bufferMask[14] & leftBuffer[14]) ^ (bufferMask[14] & rightBuffer[14])) |
//                        ((bufferMask[15] & leftBuffer[15]) ^ (bufferMask[15] & rightBuffer[15])));
//            return val == 0;
            for (int i = 0; i < bufPos; i++) {
                if (leftBuffer[i] != rightBuffer[i]) {
                    return false;
                }
            }
            return true;
        }

        private static int bucketIndex(ByteBuffer buffer, int len) {
            long hashCode = hashCode(buffer, len);

            return (int) (hashCode % BUCKETS);
        }

        private static long hashCode(ByteBuffer buffer, int len) {
            int i = 0;
            long h = 0;
            for (; i + 7 < len; i += 8) {
                long l = buffer.getLong();
                h = 31L * 31 * 31 * 31 * 31 * 31 * 31 * 31 * h
                        + 31L * 31 * 31 * 31 * 31 * 31 * 31 * ((l >> 56 & 0xFF))
                        + 31 * 31 * 31 * 31 * 31 * 31 * ((l >> 48 & 0xFF))
                        + 31 * 31 * 31 * 31 * 31 * ((l >> 40 & 0xFF))
                        + 31 * 31 * 31 * 31 * ((l >> 32 & 0xFF))
                        + 31 * 31 * 31 * ((l >> 24 & 0xFF))
                        + 31 * 31 * ((l >> 16) & 0xFF)
                        + 31 * ((l >> 8) & 0xFF)
                        + (l & 0xFF);
            }
            for (; i < len; i++) {
                h = 31 * h + buffer.get();
            }
            return h & 0xFFFFFFFFL;
        }

        public void forEach(BiConsumer<Sliceable, Stats> consumer) {
            for (StatsHolder[] bucket : map) {
                for (StatsHolder statsHolder : bucket) {
                    if (statsHolder != null) {
                        consumer.accept(statsHolder.slice, statsHolder.stats);
                    }
                }
            }
        }
    }

    private static long newLinePattern = compilePattern((byte) '\n');
    private static long semiPattern = compilePattern((byte) ';');

    private static int GRANULARITY = 32 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        int workers = Runtime.getRuntime().availableProcessors() - 1;
        if (args.length == 1) {
            workers = Integer.parseInt(args[0]);
        }
        Map<String, Stats> map = new TreeMap<>();
        File f = new File("measurements.txt");
        ExecutorService workerPool = Executors.newFixedThreadPool(workers);
        ExecutorService mergerPool = Executors.newSingleThreadExecutor();
        try (FileInputStream fis = new FileInputStream(f)) {
            FileChannel fc = fis.getChannel();
            if ((fc.size() / workers) < GRANULARITY) {
                workers = (int) (fc.size() / GRANULARITY) + 1;
            }
            int chunkSize = (int) Math.min(fc.size() / workers, Integer.MAX_VALUE);
            chunkSize = ((chunkSize / GRANULARITY) + 1) * GRANULARITY;
            // System.out.println("Chunk size: " + chunkSize);
            for (ByteBuffer bb : mmap(fc, chunkSize)) {
                workerPool.submit(() -> {
                    try {
                        StatsMap data = processChunk(bb);
                        synchronized (map) {
                            data.forEach((k, v) -> {
                                String str = stringFromBuffer(k);
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
            mergerPool.shutdown();
            mergerPool.awaitTermination(1, TimeUnit.HOURS);
        }
        finally {
            // System.out.println("Keys: " + map.size());
            System.out.println(map);
        }
    }

    private static String stringFromBuffer(Sliceable slice) {
        slice.reset();
        byte[] bytes = new byte[slice.len()];
        slice.get(bytes);
        return new String(bytes);
    }

    private static StatsMap processChunk(ByteBuffer bb) {
        StatsMap map = new StatsMap();

        LongBuffer lb = bb.asLongBuffer();

        long ptr = 0;
        long limit = lb.limit();
        long backstop = limit - 1;
        int remainder = bb.limit() % 8;
        byte[] tmp = new byte[remainder];
        long currentWord = 0;
        int offset = 8;
        long keyLen = 0;
        long valLen = 0;
        boolean fastParser = true;
        long lineCnt = 0;
        while (ptr < limit) {
            bb.mark();
            int byteIndex = 8;
            if (offset == 8) {
                currentWord = lb.get();
                offset = 0;
                ptr++;
            }

            if ((byteIndex = firstInstance(currentWord, semiPattern)) == 8) {
                long pos = ptr;
                while (ptr++ < limit && ((byteIndex = firstInstance((currentWord = lb.get()), semiPattern)) == 8))
                    ;
                if (byteIndex == 8) {
                    break;
                }
                keyLen = (8 - offset + byteIndex) + (ptr - pos - 1) * 8;
            }
            else {
                keyLen = byteIndex - offset;
            }

            currentWord &= ~(0xFFL << (7 - byteIndex) * 8);
            offset = byteIndex + 1;

            byteIndex = 8;
            fastParser = ptr < backstop;
            if ((byteIndex = firstInstance(currentWord, newLinePattern)) == 8) {
                long pos = ptr;
                if (ptr == backstop) {
                    bb.get((int) ptr * 8, tmp);
                    for (int i = 0; i < remainder; i++) {
                        if (tmp[i] == '\n') {
                            byteIndex = i;
                            break;
                        }
                    }
                    ptr++;
                }
                else {
                    while (ptr++ < limit && (byteIndex = firstInstance(currentWord = lb.get(), newLinePattern)) == 8)
                        ;
                }
                if (byteIndex == 8) {
                    break;
                }
                valLen = (8 - offset + byteIndex) + (ptr - pos - 1) * 8;
            }
            else {
                valLen = byteIndex - offset;
            }
            currentWord &= ~(0xFFL << (7 - byteIndex) * 8);
            offset = byteIndex + 1;

            bb.reset();
            Stats stats = map.getOrInsert(bb, (int) keyLen);
            bb.get();
            short val = fastParse(bb, (int) valLen, fastParser);
            bb.get();

            lineCnt++;
            stats.add(val);
        }
        // System.out.println("Remaining: " + lb.remaining());
        // System.out.println("Lines: " + lineCnt);
        return map;
    }

    private static final long fastParserMask = 0x3030303030303030L;
    private static final long minusPattern = compilePattern((byte) ('-' ^ 0x30));
    private static final long dotPattern = compilePattern((byte) ('.' ^ 0x30));

    private static short fastParse(ByteBuffer bb, int len, boolean fast) {
        assert (len <= 5);
        int targetPos = bb.position() + len;
        long word;
        if (!fast) {
            byte[] bytes = new byte[8];
            bb.get(bytes, 0, len);
            word = ((long) bytes[0] << 56)
                    | ((long) bytes[1] & 0xFF) << 48
                    | ((long) bytes[2] & 0xFF) << 40
                    | ((long) bytes[3] & 0xFF) << 32
                    | ((long) bytes[4] & 0xFF) << 24
                    | ((long) bytes[5] & 0xFF) << 16
                    | ((long) bytes[6] & 0xFF) << 8
                    | ((long) bytes[7] & 0xFF);
        }
        else {
            word = bb.getLong();
        }
        word ^= fastParserMask;
        bb.position(targetPos);

        short val = 0;
        short multiplier = 1;
        byte negative = 0;

        int negPos = firstInstance(word, minusPattern);
        if (negPos == 0) {
            negative = 1;
        }
        assert (negPos == 8);

        int dotPos = firstInstance(word, dotPattern);
        if (dotPos == 8 || (dotPos + negative) >= len) {
            multiplier = 10;
        }

        for (int i = 0; i < len; i++) {
            int digit = (int) ((word >>> (7 - i) * 8) & 0xFF);
            if (digit > 9) {
                continue;
            }
            val = (short) (val * 10 + digit);
        }
        short ret = (short) ((val * multiplier) * (negative == 1 ? -1 : 1));
        return ret;
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
                // System.out.println("===> chunk: " + (fc.size() - remaining) + " - " + (fc.size() - remaining + size - 1));
                buffers[j] = fc.map(FileChannel.MapMode.READ_ONLY, fc.size() - remaining, size);
                remaining -= size;
                count = j + 1;
            }
            else {
                count = j + 1;
                // System.out.println("===> chunk: " + (fc.size() - remaining) + " - " + fc.size());
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
}
