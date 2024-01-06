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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class CalculateAverage_shipilev {

    // This might not be the fastest implementation one can do.
    // When working on this implementation, I set the bar like this:
    //
    //  1. Using only vanilla Java, without Unsafe tricks;
    //  2. Using only standard Java, without preview features like FFM;
    //  3. Leaving out hard-to-understand hacks, the correctness of which is not obvious;
    //  4. PARALLELISM, to feed my hungry TR 3970X.
    //

    // ========================= Tunables =========================

    // Workload data file.
    private static final String FILE = "./measurements.txt";

    // Enable dataset-specific optimizations. This would not pass all the tests,
    // but would work for the "target" workload data. In other words, this
    // enables the workload-specific cheating. Should normally be "false",
    // and serve as marker for the places where such cheating is possible.
    private static final boolean WORKLOAD_CHEATING = false;

    // The largest slice as unit of work, processed serially by a worker.
    // Set it too low and there would be more tasks and less batching, but
    // more parallelism. Set it too high, and the reverse would be true.
    private static final int UNIT_SLICE_SIZE = 1 * 1024 * 1024;

    // Max distance to search for line separator when scanning for line
    // boundaries.
    private static final int MAX_LINE_LENGTH = WORKLOAD_CHEATING ? 32 : 1024;

    // ========================= Storage =========================

    // Thread-local measurement maps, each thread gets one.
    // Even though crude, avoid lambdas here to alleviate startup costs.
    private static final ThreadLocal<MeasurementsMap> MAPS = ThreadLocal.withInitial(new Supplier<>() {
        @Override
        public MeasurementsMap get() {
            MeasurementsMap m = new MeasurementsMap();
            synchronized (ALL_MAPS) {
                ALL_MAPS.add(m);
            }
            return m;
        }
    });

    // After worker threads finish, the data is available here. One just needs
    // to merge it a little.
    private static final ArrayList<MeasurementsMap> ALL_MAPS = new ArrayList<>();

    // Line buffers for workers to do scan for line boundaries.
    // Even though crude, avoid lambdas here to alleviate startup costs.
    private static final ThreadLocal<ByteBuffer> LINE_BUFFERS = ThreadLocal.withInitial(new Supplier<>() {
        @Override
        public ByteBuffer get() {
            ByteBuffer bb = ByteBuffer.allocateDirect(MAX_LINE_LENGTH);
            bb.order(ByteOrder.nativeOrder());
            return bb;
        }
    });

    // Chunk buffers for workers to do read stuff into.
    // Even though crude, avoid lambdas here to alleviate startup costs.
    private static final ThreadLocal<ByteBuffer> CHUNK_BUFFERS = ThreadLocal.withInitial(new Supplier<>() {
        @Override
        public ByteBuffer get() {
            ByteBuffer bb = ByteBuffer.allocateDirect(UNIT_SLICE_SIZE);
            bb.order(ByteOrder.nativeOrder());
            return bb;
        }
    });

    // ========================= MEATY GRITTY PARTS: PARSE AND AGGREGATE =========================

    // Quick and dirty linear-probing hash map. YOLO.
    public static class MeasurementsMap {
        private int mapSize = 4096;
        private int mapSizeMask = mapSize - 1;

        private Bucket[] map;

        public MeasurementsMap() {
            map = new Bucket[mapSize];
        }

        public Bucket bucket(ByteBuffer name, int begin, int end, int hash) {
            int idx = hash & mapSizeMask;

            // Lucky fast path, most of the time we complete here. Smaller method
            // allows better inlining.
            Bucket cur = map[idx];
            if (cur != null && cur.hash == hash &&
                    (WORKLOAD_CHEATING || arraysEquals(cur.name, name, begin, end))) {
                // Existing bucket hit. On the off-chance it was a hash collision,
                // we need to check the array contents. But on sample data, checking
                // array contents is superfluous, as we never hash collide.
                return cur;
            }

            // Unlucky, slow path.
            return bucketSlow(name, begin, end, hash);
        }

        private Bucket bucketSlow(ByteBuffer name, int begin, int end, int hash) {
            int origIdx = hash & mapSizeMask;
            int idx = origIdx;

            while (true) {
                Bucket cur = map[idx];
                if (cur == null) {
                    // No bucket yet, lucky us. Copy out the name from the buffer,
                    // and create the bucket with it. We already know the hash.
                    byte[] copy = new byte[end - begin];
                    name.get(begin, copy, 0, end - begin);
                    map[idx] = cur = new Bucket(copy, hash);
                    return cur;
                }
                else if (cur.hash == hash &&
                        (WORKLOAD_CHEATING || arraysEquals(cur.name, name, begin, end))) {
                    // Same as bucket fastpath.
                    return cur;
                }
                else {
                    // No dice. Keep searching until we hit the same index.
                    // This would need rehash. Again, on sample data, we do not ever need a rehash.
                    idx = (idx + 1) & mapSizeMask;
                    if (!WORKLOAD_CHEATING && idx == origIdx) {
                        rehash();
                        idx = hash & mapSizeMask;
                    }
                }
            }
        }

        // Little helper method to compare the array with given bytebuffer range.
        private static boolean arraysEquals(byte[] orig, ByteBuffer cand, int begin, int end) {
            int origLen = orig.length;
            int candLen = end - begin;
            if (origLen != candLen) {
                return false;
            }
            for (int i = 0; i < origLen; i++) {
                if (orig[i] != cand.get(begin + i)) {
                    return false;
                }
            }
            return true;
        }

        public void merge(MeasurementsMap otherMap) {
            merge(otherMap.map, true);
        }

        private void rehash() {
            // Double-up and remerge from ourselves.
            Bucket[] oldMap = map;
            mapSize *= 2;
            mapSizeMask = mapSize - 1;
            map = new Bucket[mapSize];
            merge(oldMap, false);
        }

        // Same as bucket(), really, but for merging maps. See the comments there.
        private void merge(Bucket[] buckets, boolean allowRehash) {
            for (Bucket other : buckets) {
                if (other == null)
                    continue;
                int origIdx = other.hash & mapSizeMask;
                int idx = origIdx;
                while (true) {
                    Bucket cur = map[idx];
                    if (cur == null) {
                        map[idx] = other;
                        break;
                    }
                    else if (cur.hash == other.hash &&
                            (WORKLOAD_CHEATING || Arrays.equals(cur.name, other.name))) {
                        cur.merge(other);
                        break;
                    }
                    else {
                        idx = (idx + 1) & mapSizeMask;
                        if (!WORKLOAD_CHEATING && idx == origIdx) {
                            if (allowRehash) {
                                rehash();
                                idx = other.hash & mapSizeMask;
                            }
                            else {
                                throw new IllegalStateException("Cannot happen");
                            }
                        }
                    }
                }
            }
        }

        // Convert from internal representation to the rows.
        // This does several major things: filters away null-s, instantates full Strings,
        // and computes stats.
        public Row[] rows() {
            Row[] rows = new Row[mapSize];
            int idx = 0;
            for (Bucket bucket : map) {
                if (bucket == null)
                    continue;
                rows[idx++] = bucket.toRow();
            }

            return Arrays.copyOf(rows, idx);
        }

        // Individual map bucket. It might be useful to try and inline the arrays
        // of these straight into map, but on the other hand, the map is likely
        // sparse, and we would lose, not gain locality (have not checked).
        private static class Bucket {
            // Raw station name and its hash. Always available.
            private final byte[] name;
            private final int hash;

            // Temperature values, in 10x scale.
            private int min;
            private int max;
            private long sum;
            private long count;

            public Bucket(byte[] name, int hash) {
                this.min = Integer.MAX_VALUE;
                this.max = Integer.MIN_VALUE;
                this.name = name;
                this.hash = hash;
            }

            public void merge(int value) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                count++;
            }

            public void merge(Bucket s) {
                min = Math.min(min, s.min);
                max = Math.max(max, s.max);
                sum += s.sum;
                count += s.count;
            }

            public Row toRow() {
                return new Row(
                        new String(name),
                        Math.round((double) min) / 10.0,
                        Math.round((double) sum / count) / 10.0,
                        Math.round((double) max) / 10.0);
            }
        }
    }

    // The heavy-weight, where most of the magic happens.
    public static class ParsingTask extends RecursiveAction {
        private final FileChannel fc;
        private final long start;
        private final long end;

        public ParsingTask(FileChannel fc, long start, long end) {
            this.fc = fc;
            this.start = start;
            this.end = end;
        }

        @Override
        protected void compute() {
            try {
                internalCompute();
            }
            catch (Exception e) {
                // Meh, YOLO.
                throw new IllegalStateException("Internal error", e);
            }
        }

        private void internalCompute() throws Exception {
            if (end - start > UNIT_SLICE_SIZE) {
                // Split in half.
                long mid = start + (end - start) / 2;

                // Read a little chunk into a little buffer.
                long minBound = mid - MAX_LINE_LENGTH;
                ByteBuffer bb = LINE_BUFFERS.get();
                bb.rewind();
                fc.read(bb, minBound);

                // Figure out the boundary that does not split the line.
                int w = MAX_LINE_LENGTH;
                while (bb.get(w - 1) != '\n') {
                    w--;
                }
                mid = minBound + w;

                // Fork out! The stack depth would be shallow enough for us to
                // execute one of the computations directly.
                ParsingTask t1 = new ParsingTask(fc, start, mid);
                t1.fork();
                ParsingTask t2 = new ParsingTask(fc, mid, end);
                t2.compute();
                t1.join();
            }
            else {
                // The call to seqCompute would normally be non-inlined.
                // Do setup stuff here to save inlining budget.
                MeasurementsMap map = MAPS.get();

                // Read the chunk from file.
                ByteBuffer slice = CHUNK_BUFFERS.get();
                slice.rewind();
                fc.read(slice, start);

                // At this point, we know the length fits in integer.
                int len = (int) (end - start);

                // Go!
                seqCompute(map, slice, len);
            }
        }

        private void seqCompute(MeasurementsMap map, ByteBuffer slice, int length) throws IOException {
            int idx = 0;
            while (idx < length) {
                // Parse out the name, computing the hash on the fly.
                // Note the hash includes ';', but we do not really care,
                // and it saves a bit of code. The code is in separate scope
                // to optimize bytecode a little for interpreter's benefit.
                MeasurementsMap.Bucket bucket = null;
                {
                    int nameBegin = idx;
                    int nameHash = 0;
                    byte b = 0;
                    do {
                        b = slice.get(idx);
                        nameHash *= 31;
                        nameHash += b;
                        idx++;
                    } while (b != ';');
                    int nameEnd = idx - 1;

                    // We have enough now to figure out which bucket in the map
                    // we belong to. Doing this early limits the need to keep
                    // name* variables alive.
                    bucket = map.bucket(slice, nameBegin, nameEnd, nameHash);
                }

                // Parse out the temperature. We implicitly look ahead for
                // negative sign and carry the negative multiplier, if found.
                // We also implicitly look for decimal point, which is _below_
                // '0' and would yield a "negative" digit. After that, we only
                // care about a single digit after the point. The aggregation
                // code expects temperatures at 10x scale.
                {
                    int temp = 0;
                    int neg = 1;
                    if (slice.get(idx) == '-') {
                        neg = -1;
                        idx++;
                    }
                    int digit = 0;
                    do {
                        temp = temp * 10 + digit;
                        byte b = slice.get(idx);
                        digit = b - '0';
                        idx++;
                    } while (digit >= 0);

                    // Past decimal point, parse another digit and exit.
                    temp = temp * 10 + (slice.get(idx) - '0');
                    temp = temp * neg;
                    idx++;

                    // Time to update!
                    bucket.merge(temp);
                }

                // Eat the rest of the digits and the EOL.
                while (slice.get(idx) != '\n') {
                    idx++;
                }
                idx++;
            }
        }
    }

    // ========================= Invocation =========================

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r");
                FileChannel fc = file.getChannel()) {
            // This little line carries the whole world.
            new ParsingTask(fc, 0, fc.size()).invoke();

            // Merge all results from thread-local maps...
            MeasurementsMap map = new MeasurementsMap();
            for (MeasurementsMap m : ALL_MAPS) {
                map.merge(m);
            }

            // ...and report them
            System.out.println(report(map));
        }
    }

    // ========================= Reporting =========================

    private static String report(MeasurementsMap map) {
        Row[] rows = map.rows();
        Arrays.sort(rows);

        StringBuilder sb = new StringBuilder(16384);
        sb.append("{");
        boolean first = true;
        for (Row r : rows) {
            if (first) {
                first = false;
            }
            else {
                sb.append(", ");
            }
            r.printTo(sb);
        }
        sb.append("}");
        return sb.toString();
    }

    private static class Row implements Comparable<Row> {
        private final String name;
        private final double min;
        private final double max;
        private final double avg;

        public Row(String name, double min, double avg, double max) {
            this.name = name;
            this.min = min;
            this.max = max;
            this.avg = avg;
        }

        @Override
        public int compareTo(Row o) {
            return name.compareTo(o.name);
        }

        public void printTo(StringBuilder sb) {
            sb.append(name);
            sb.append("=");
            sb.append(min);
            sb.append("/");
            sb.append(avg);
            sb.append("/");
            sb.append(max);
        }
    }

}
