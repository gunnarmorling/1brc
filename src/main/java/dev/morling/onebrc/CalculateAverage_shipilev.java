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
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountedCompleter;
import java.util.function.Supplier;

public class CalculateAverage_shipilev {

    // This might not be the fastest implementation one can do.
    // When working on this implementation, I set the bar like this:
    //
    // 1. Use only vanilla Java, without defaulting to Unsafe tricks;
    // 2. Use only standard Java, without preview features like FFM;
    // 3. Leave out hard-to-understand hacks, the correctness of which is not obvious;
    // 4. Use PARALLELISM, to feed my hungry TR 3970X.
    //

    // ========================= Tunables =========================

    // Workload data file.
    private static final String FILE = "./measurements.txt";

    // The largest mmap-ed chunk. This should normally be Integer.MAX_VALUE,
    // but can be tuned down to allow smaller mmaped regions, if needed.
    private static final int MMAP_CHUNK_SIZE = Integer.MAX_VALUE;

    // The largest slice as unit of work, processed serially by a worker.
    // Set it too low and there would be more tasks and less batching, but
    // more parallelism. Set it too high, and the reverse would be true.
    private static final int UNIT_SLICE_SIZE = 1 * 1024 * 1024;

    // Max distance to search for line separator when scanning for line
    // boundaries.
    private static final int MAX_LINE_LENGTH = 1024;

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
                    arraysEquals(cur.name, name, begin, end)) {
                // Existing bucket hit. On the off-chance it was a hash collision,
                // we need to check the array contents.
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
                        arraysEquals(cur.name, name, begin, end)) {
                    // Same as bucket fastpath.
                    return cur;
                }
                else {
                    // No dice. Keep searching until we hit the same index.
                    // This would need rehash. Again, on sample data, we do not ever need a rehash.
                    idx = (idx + 1) & mapSizeMask;
                    if (idx == origIdx) {
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
                            Arrays.equals(cur.name, other.name)) {
                        cur.merge(other);
                        break;
                    }
                    else {
                        idx = (idx + 1) & mapSizeMask;
                        if (idx == origIdx) {
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
            private short min;
            private short max;
            private long sum;
            private int count;

            public Bucket(byte[] name, int hash) {
                this.min = Short.MAX_VALUE;
                this.max = Short.MIN_VALUE;
                this.name = name;
                this.hash = hash;
            }

            public void merge(short value) {
                if (value < min) {
                    min = value;
                }
                if (value > max) {
                    max = value;
                }
                sum += value;
                count++;
            }

            public void merge(Bucket s) {
                min = (short) Math.min(min, s.min);
                max = (short) Math.max(max, s.max);
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

    // The heavy-weight, where most of the magic happens. This is not a usual
    // RecursiveAction, but rather a CountedCompleter in order to be more robust
    // in presence of I/O stalls and other scheduling irregularities.
    public static class ParsingTask extends CountedCompleter<Void> {
        private final ByteBuffer buf;

        public ParsingTask(CountedCompleter<Void> p, ByteBuffer buf) {
            super(p);
            this.buf = buf;
        }

        @Override
        public void compute() {
            try {
                internalCompute();
            }
            catch (Exception e) {
                // Meh, YOLO.
                throw new IllegalStateException("Internal error", e);
            }
        }

        private void internalCompute() throws Exception {
            int len = buf.limit();
            if (len > UNIT_SLICE_SIZE) {
                // Split in half.
                int mid = len / 2;

                // Read a little chunk into a little buffer.
                int minBound = mid - MAX_LINE_LENGTH;

                // Figure out the boundary that does not split the line.
                int w = mid + MAX_LINE_LENGTH;
                while (buf.get(w - 1) != '\n') {
                    w--;
                }
                mid = w;

                // Fork out! The stack depth would be shallow enough for us to
                // execute one of the computations directly.
                // FJP API: Tell there is a pending task.
                setPendingCount(1);
                new ParsingTask(this, buf.slice(0, mid)).fork();

                // The stack depth would be shallow enough for us to
                // execute one of the computations directly.
                new ParsingTask(this, buf.slice(mid, len - mid)).compute();
            }
            else {
                // The call to seqCompute would normally be non-inlined.
                // Do setup stuff here to save inlining budget.
                MeasurementsMap map = MAPS.get();

                // Go!
                seqCompute(map, buf, len);

                // FJP API: Notify that this task have completed.
                tryComplete();
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

                // Parse out the temperature. The rules specify temperatures
                // are within -99.9..99.9. We implicitly look ahead for
                // negative sign and carry the negative multiplier, if found.
                // We also implicitly look for decimal point, which is _below_
                // '0' and would yield a "negative" digit. After that, we only
                // expect a single digit after the point. The aggregation code
                // expects temperatures at 10x scale.
                {
                    byte first = slice.get(idx);

                    // Is it a sign? Record it and read the true next digit.
                    int neg = 1;
                    if (first == '-') {
                        neg = -1;
                        idx++;
                        first = slice.get(idx);
                    }

                    // First digit is always present, start with it.
                    int temp = first - '0';
                    idx++;

                    // Second digit might be missing, check if it is a decimal point.
                    int digit = slice.get(idx) - '0';
                    if (digit >= 0) {
                        // Nope, legit digit.
                        temp = temp * 10 + digit;
                        // Skip over decimal point.
                        idx += 2;
                    }
                    else {
                        // This was decimal point, skip it.
                        idx++;
                    }

                    // The left-over is single digit after the point.
                    temp = temp * 10 + (slice.get(idx) - '0');
                    temp = temp * neg;

                    // Eat the last digit and EOL.
                    idx += 2;

                    // Time to update!
                    bucket.merge((short) temp);
                }
            }
        }
    }

    // Fork out the initial tasks. We would normally just fork out one large
    // task and let it split, but unfortunately buffer API does not allow us
    // "long" start-s and length-s. So we have to chunk at least by mmap-ed
    // size first. It is a CountedCompleter for the same reason ParsingTask is.
    public static class RootTask extends CountedCompleter<Void> {
        private final FileChannel fc;

        public RootTask(CountedCompleter<Void> parent, FileChannel fc) {
            super(parent);
            this.fc = fc;
        }

        @Override
        public void compute() {
            try {
                internalCompute();
            }
            catch (Exception e) {
                // Meh, YOLO.
                throw new IllegalStateException("Internal error", e);
            }
        }

        private void internalCompute() throws IOException {
            ByteBuffer buf = ByteBuffer.allocateDirect(MAX_LINE_LENGTH);
            buf.order(ByteOrder.nativeOrder());

            long start = 0;
            long size = fc.size();
            while (start < size) {
                long end = Math.min(size, start + MMAP_CHUNK_SIZE);

                // Read a little chunk into a little buffer.
                long minEnd = Math.max(0, end - MAX_LINE_LENGTH);
                buf.rewind();
                fc.read(buf, minEnd);

                // Figure out the boundary that does not split the line.
                int w = MAX_LINE_LENGTH;
                while (buf.get(w - 1) != '\n') {
                    w--;
                }
                end = minEnd + w;

                // Fork out the large slice
                long len = end - start;
                ByteBuffer slice = fc.map(FileChannel.MapMode.READ_ONLY, start, len);
                start += len;

                // FJP API: Announce we have a pending task before forking.
                addToPendingCount(1);

                // ...and fork it
                new ParsingTask(this, slice).fork();
            }

            // FJP API: We have finished, try to complete the whole task tree.
            tryComplete();
        }
    }

    // ========================= Invocation =========================

    public static void main(String[] args) throws IOException {
        try (FileChannel fc = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            // This little line carries the whole world
            new RootTask(null, fc).invoke();

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
