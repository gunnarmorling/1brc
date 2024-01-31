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
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class CalculateAverage_shipilev {

    // Detour: This implementation tries to balance the speed and readability.
    //
    // While the original contest suggests we pull off every trick in the
    // book to get the peak performance, here we set a more pragmatic goal:
    // how fast we can get without going too far into hacks. Or, putting it
    // in another way, what would be the reasonably fast implementation that
    // would *also* pass a code review in a reasonable project, would be usable
    // in production without waking people up in the middle of the night, and
    // would work through JDK updates, upgrades, and migrations.
    //
    // To that end, this implementation uses vanilla and standard Java as much
    // as possible, without relying on Unsafe tricks and preview features.
    // When any non-standard things are used, they are guarded by a feature flag,
    // which allows to cleanly turn them off when anything goes off the rails.
    //
    // For performance reasons, the implementation takes more care to be reliably
    // parallel to survive I/O stalls and scheduling oddities. This would not
    // show up in laboratory conditions, but it is a necessary thing for a reliable
    // code in production. It also tries not to miss simple optimizations without
    // going too far into the woods.
    //
    // Note that some of the magic to run this workload fast in evaluation
    // conditions is done separately in the invocation script. Most of that
    // is only needed for the short-running scenarios. In real life, this code
    // would likely run well without any of that.
    //

    // ========================= Tunables =========================

    // Workload data file.
    private static final String FILE = "./measurements.txt";

    // Max distance to search for line separator when scanning for line
    // boundaries. 100 bytes name should fit into this power-of-two buffer.
    // Should probably never change.
    private static final int MAX_LINE_LENGTH = 128;

    // Fixed size of the measurements map. Must be the power of two. Should
    // be large enough to accomodate all the station names. Rules say there are
    // 10K station names max, so anything more than 16K works well.
    private static final int MAP_SIZE = 1 << 15;

    // The largest mmap-ed chunk. This can be be Integer.MAX_VALUE, but
    // it is normally tuned down to seed the workers with smaller mmap regions
    // more efficiently. This also allows to incrementally unmap chunks as we
    // complete working on them.
    private static final int MMAP_CHUNK_SIZE = Integer.MAX_VALUE / 32;

    // The largest slice as unit of work, processed serially by a worker.
    // Set it too low and there would be more tasks and less batching, but
    // more parallelism. Set it too high, and the reverse would be true.
    // Something around a large page would likely hit the right balance.
    private static final int UNIT_SLICE_SIZE = 4 * 1024 * 1024;

    // Employ direct unmapping techniques to alleviate the cost of system
    // unmmapping on process termination. This matters for very short runs
    // on highly parallel machines. This unfortunately calls into private
    // methods of buffers themselves. If not available on target JVM, the
    // feature would automatically turn off.
    private static final boolean DIRECT_UNMMAPS = true;

    // ========================= Storage =========================

    // Thread-local measurement maps, each thread gets one.
    // This allows workers to work nearly unimpeded without synchronization.
    // Even though crude, avoid lambdas here to alleviate startup costs.
    private static final ThreadLocal<MeasurementsMap> MAPS = ThreadLocal.withInitial(new Supplier<>() {
        @Override
        public MeasurementsMap get() {
            MeasurementsMap m = new MeasurementsMap();
            ALL_MAPS.add(m);
            return m;
        }
    });

    // After worker threads finish, the data is available here. The reporting
    // code would pull the maps from here, once all workers finish.
    private static final ConcurrentLinkedQueue<MeasurementsMap> ALL_MAPS = new ConcurrentLinkedQueue<>();

    // Releasable mmaped buffers that workers are done with. These can be un-mapped
    // in background. Main thread would wait on this queue, until it gets the poison
    // pill from the root task.
    private static final LinkedBlockingQueue<ByteBuffer> RELEASABLE_BUFFERS = new LinkedBlockingQueue<>();
    private static final ByteBuffer RELEASABLE_BUFFER_POISON_PILL = ByteBuffer.allocate(1);

    // ========================= MEATY GRITTY PARTS: PARSE AND AGGREGATE =========================

    public static final class Bucket {
        // Raw station name, encoded as two prefixes and the name tail,
        // its total length, and hash.
        public final byte[] nameTail;
        public final int len;
        public final int hash;
        public final int prefix1, prefix2;

        // Temperature values, in 10x scale.
        public long sum;
        public int count;
        public int min;
        public int max;

        public Bucket(ByteBuffer slice, int begin, int end, int hash, int temp) {
            len = end - begin;

            // Decode the station name. It is handy to have a few prefixes
            // available to simplify matches later.
            int tailStart = 0;
            if (len >= 8) {
                prefix1 = slice.getInt(begin + 0);
                prefix2 = slice.getInt(begin + 4);
                tailStart += 8;
            }
            else if (len >= 4) {
                prefix1 = slice.getInt(begin + 0);
                prefix2 = 0;
                tailStart += 4;
            }
            else {
                prefix1 = 0;
                prefix2 = 0;
            }

            // The rest goes to tail byte array. We are checking reading it on hot-path.
            // Therefore, it is convenient to keep allocation for names near the buckets.
            // One can avoid this by carefully recording the tail in a separate field,
            // like the prefixes above, but this is simple enough to gain enough perf.
            int tailLen = len - tailStart;
            nameTail = new byte[tailLen];
            slice.get(begin + tailStart, nameTail, 0, tailLen);

            // Seed the bucket with initial value.
            this.hash = hash;
            this.sum = temp;
            this.count = 1;
            this.min = temp;
            this.max = temp;
        }

        // Little helper method to compare the array with given ByteBuffer range.
        public boolean matches(ByteBuffer cand, int begin, int end) {
            int origLen = len;
            int candLen = end - begin;
            if (origLen != candLen) {
                return false;
            }

            // Check the prefixes first, if we can.
            int tailStart = 0;
            if (origLen >= 8) {
                if (prefix1 != cand.getInt(begin)) {
                    return false;
                }
                if (prefix2 != cand.getInt(begin + 4)) {
                    return false;
                }
                tailStart += 8;
            }
            else if (origLen >= 4) {
                if (prefix1 != cand.getInt(begin)) {
                    return false;
                }
                tailStart += 4;
            }

            // Check the rest.
            for (int i = 0; i < origLen - tailStart; i++) {
                if (nameTail[i] != cand.get(begin + tailStart + i)) {
                    return false;
                }
            }
            return true;
        }

        // Check if current Bucket matches another.
        public boolean matches(Bucket other) {
            return len == other.len &&
                    prefix1 == other.prefix1 &&
                    prefix2 == other.prefix2 &&
                    Arrays.equals(nameTail, other.nameTail);
        }

        // Merge the temp value. Hot-path, should be fairly efficient.
        public void merge(int value) {
            sum += value;
            count++;

            // We rarely do the updates, so these branches are almost
            // never taken. Writing them as explicit branches instead of
            // Math.{min,max} improves performance a bit.
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }

        // Merge the buckets. Called during reporting, not a hot path.
        public void merge(Bucket s) {
            sum += s.sum;
            count += s.count;
            min = Math.min(min, s.min);
            max = Math.max(max, s.max);
        }

        public Row toRow() {
            // Reconstruct the name first. The prefixes and the tail were copied
            // from the little-endian slice, so we need to match the endianness here.
            ByteBuffer bb = ByteBuffer.allocate(len);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            if (len >= 4) {
                bb.putInt(prefix1);
            }
            if (len >= 8) {
                bb.putInt(prefix2);
            }
            bb.put(nameTail);

            return new Row(
                    new String(Arrays.copyOf(bb.array(), len)),
                    Math.round((double) min) / 10.0,
                    Math.round((double) sum / count) / 10.0,
                    Math.round((double) max) / 10.0);
        }
    }

    // Quick and dirty linear-probing hash map. YOLO.
    public static final class MeasurementsMap {
        // Individual map buckets. Inlining these straight into map complicates
        // the implementation without much of the performance improvement.
        // The map is likely sparse, so whatever footprint loss we have due to
        // Bucket headers we gain by allocating the buckets lazily. The memory
        // dereference costs are still high in both cases. The additional benefit
        // for explicit fields in Bucket is that we only need to pay for a single
        // null-check on bucket instead of multiple range-checks on inlined array.
        private final Bucket[] buckets = new Bucket[MAP_SIZE];

        // Fast path is inlined in seqCompute. This is a slow-path that is taken
        // rarely, usually when there is a hash collision. We normally do not enter here.
        private void updateSlow(ByteBuffer name, int begin, int end, int hash, int temp) {
            int idx = hash & (MAP_SIZE - 1);

            while (true) {
                Bucket cur = buckets[idx];
                if (cur == null) {
                    // No bucket yet, lucky us. Create the bucket and be done.
                    buckets[idx] = new Bucket(name, begin, end, hash, temp);
                    return;
                }
                else if ((cur.hash == hash) && cur.matches(name, begin, end)) {
                    // Same as bucket fastpath. Check for collision by checking the full hash
                    // first (since the index is truncated by map size), and then the exact name.
                    cur.merge(temp);
                    return;
                }
                else {
                    // No dice. Keep searching.
                    idx = (idx + 1) & (MAP_SIZE - 1);
                }
            }
        }

        // Same as update(), really, but for merging maps. See the comments there.
        public void merge(MeasurementsMap otherMap) {
            for (Bucket other : otherMap.buckets) {
                if (other == null)
                    continue;
                int idx = other.hash & (MAP_SIZE - 1);
                while (true) {
                    Bucket cur = buckets[idx];
                    if (cur == null) {
                        buckets[idx] = other;
                        break;
                    }
                    else if ((cur.hash == other.hash) && cur.matches(other)) {
                        cur.merge(other);
                        break;
                    }
                    else {
                        idx = (idx + 1) & (MAP_SIZE - 1);
                    }
                }
            }
        }

        // Convert from internal representation to the rows. This does several
        // major things: filters away null-s, instantates full Strings, and
        // computes the final rows.
        public int fill(Row[] rows) {
            int idx = 0;
            for (Bucket bucket : buckets) {
                if (bucket == null)
                    continue;
                rows[idx++] = bucket.toRow();
            }
            return idx;
        }
    }

    // The heavy-weight, where most of the magic happens. This is not a usual
    // RecursiveAction, but rather a CountedCompleter in order to be more robust
    // in presence of I/O stalls and other scheduling irregularities.
    public static final class ParsingTask extends CountedCompleter<Void> {
        private final MappedByteBuffer mappedBuf;
        private final ByteBuffer buf;

        // Entered from the root task, records the original mmap-ed slice
        // for later cleanup.
        public ParsingTask(CountedCompleter<Void> p, MappedByteBuffer mappedBuf) {
            super(p);
            this.mappedBuf = mappedBuf;
            this.buf = mappedBuf;
        }

        // Entered from the other parsing tasks.
        public ParsingTask(CountedCompleter<Void> p, ByteBuffer buf) {
            super(p);
            this.mappedBuf = null;
            this.buf = buf;
        }

        @Override
        public void compute() {
            try {
                internalCompute();
            }
            catch (Exception e) {
                // Meh, YOLO.
                e.printStackTrace();
                throw new IllegalStateException("Internal error", e);
            }
        }

        @Override
        public void onCompletion(CountedCompleter<?> caller) {
            // FJP API: Would be called when this task completes. At that point,
            // we know the mmap-ed slice is not needed anymore, and can give it
            // out for unmmaps. We do not do unmmap here, let the main thread
            // handle it for us, as we go on doing other hot work.
            if (DIRECT_UNMMAPS && (mappedBuf != null)) {
                RELEASABLE_BUFFERS.offer(mappedBuf);
            }
        }

        private void internalCompute() throws Exception {
            int len = buf.limit();
            if (len > UNIT_SLICE_SIZE) {
                // Still a large chunk, let's split it in half.
                int mid = len / 2;

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
                // Small enough chunk, time to process it.
                // The call to seqCompute would normally be non-inlined.
                // Do setup stuff here to save inlining budget.
                MeasurementsMap map = MAPS.get();

                // Force the order we need for bit extraction to work. This fits
                // most of the hardware very well without introducing platform
                // dependencies. Note that it would be wrong to use nativeOrder()
                // here, because we _need_ a particular byte ordering for our
                // computations to work. It just so happens that most hardware
                // we have is LE.
                buf.order(ByteOrder.LITTLE_ENDIAN);

                // Go!
                seqCompute(map, buf, len);

                // FJP API: Notify that this task have completed.
                tryComplete();
            }
        }

        private void seqCompute(MeasurementsMap map, ByteBuffer origSlice, int length) throws IOException {
            Bucket[] buckets = map.buckets;

            // Slice up our slice! Pecular note here: this instantiates a full new buffer
            // object, which allows compiler to trust its fields more thoroughly.
            ByteBuffer slice = origSlice.slice();

            // New slice lost the endianness setting, set it up as the original slice.
            slice.order(ByteOrder.LITTLE_ENDIAN);

            // Touch the buffer once to let the compiler eject the common checks
            // for this slice from the loop here. This is an odd, flaky, and sometimes
            // desperate, but a safe thing to do.
            slice.get(0);

            int idx = 0;
            while (idx < length) {
                // Parse out the name, computing the hash on the fly.
                // Reading with ints allows us to guarantee that read would always
                // be in bounds, since the temperature+EOL is at least 4 bytes
                // long themselves. This implementation prefers simplicity over
                // advanced tricks like SWAR.
                int nameBegin = idx;
                int nameHash = 0;

                outer: while (true) {
                    int intName = slice.getInt(idx);
                    for (int c = 0; c < 4; c++) {
                        int b = (intName >> (c << 3)) & 0xFF;
                        if (b == ';') {
                            idx += c + 1;
                            break outer;
                        }
                        nameHash ^= b * 82805;
                    }
                    idx += 4;
                }
                int nameEnd = idx - 1;

                // Parse out the temperature. The rules specify temperatures
                // are within -99.9..99.9. This means even in the shortest case of
                // "0.0<EOL>", we are not out of bounds for the int-sized read.
                int intTemp = slice.getInt(idx);

                int neg = 1;
                if ((intTemp & 0xFF) == '-') {
                    // Unlucky, there is a sign. Record it, shift one byte and read
                    // the remaining digit again. Surprisingly, doing a second read
                    // is not significantly worse than reading into long and trying
                    // to do bit shifts on it. But it is significantly simpler.
                    neg = -1;
                    intTemp >>>= 8;
                    intTemp |= slice.get(idx + 4) << 24;
                    idx++;
                }

                // Since the sign is consumed, we are only left with two cases,
                // which means we can trivially extract the number from int.
                int temp = 0;
                if ((intTemp >>> 24) == '\n') {
                    // Case 1: EOL-digitL-point-digitH
                    temp = (((intTemp & 0xFF)) - '0') * 10 +
                            ((intTemp >> 16) & 0xFF) - '0';
                    idx += 4;
                }
                else {
                    // Case 2: digitL-point-digitH-digitHH
                    temp = (((intTemp & 0xFF)) - '0') * 100 +
                            (((intTemp >> 8) & 0xFF) - '0') * 10 +
                            (((intTemp >>> 24)) - '0');
                    idx += 5;
                }

                // All done, just flip the sign, if needed.
                temp *= neg;

                // Time to update!
                Bucket bucket = buckets[nameHash & (MAP_SIZE - 1)];
                if ((bucket != null) && (nameHash == bucket.hash) && bucket.matches(slice, nameBegin, nameEnd)) {
                    // Lucky fast path: matching bucket hit. Most of the time we complete here.
                    bucket.merge(temp);
                }
                else {
                    // Unlucky, slow path. The method would not be inlined, it is useful
                    // to give it the original slice, so that we keep current hot slice
                    // metadata provably unmodified.
                    map.updateSlow(origSlice, nameBegin, nameEnd, nameHash, temp);
                }
            }
        }
    }

    // Fork out the initial tasks. We would normally just fork out one large
    // task and let it split, but unfortunately buffer API does not allow us
    // "long" start-s and length-s. So we have to chunk at least by mmap-ed
    // size first. It is a CountedCompleter for the same reason ParsingTask is.
    // This also gives us a very nice opportunity to process mmap-ed chunks
    // one by one, thus allowing incremental unmmaps.
    public static final class RootTask extends CountedCompleter<Void> {
        public RootTask() {
            super(null);
        }

        @Override
        public void compute() {
            try {
                internalCompute();
            }
            catch (Exception e) {
                // Meh, YOLO.
                e.printStackTrace();
                throw new IllegalStateException("Internal error", e);
            }
        }

        private void internalCompute() throws Exception {
            ByteBuffer buf = ByteBuffer.allocateDirect(MAX_LINE_LENGTH);
            FileChannel fc = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);

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

                // Fork out the large slice.
                long len = end - start;
                MappedByteBuffer slice = fc.map(FileChannel.MapMode.READ_ONLY, start, len);
                start += len;

                // FJP API: Announce we have a pending task before forking.
                addToPendingCount(1);

                // ...and fork it!
                new ParsingTask(this, slice).fork();
            }

            // All mappings are up, can close the channel now.
            fc.close();

            // FJP API: We have finished, try to complete the whole task tree.
            propagateCompletion();
        }

        @Override
        public void onCompletion(CountedCompleter<?> caller) {
            // FJP API: This would be called when root task completes along with
            // all subtasks. This means the processing is done, we can go and
            // tell main thread about that.
            try {
                RELEASABLE_BUFFERS.put(RELEASABLE_BUFFER_POISON_PILL);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // ========================= Invocation =========================

    public static void main(String[] args) throws Exception {
        // Instantiate a separate FJP to match the parallelism accurately, without
        // relying on common pool defaults.
        ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

        // This little line carries the whole world
        pool.submit(new RootTask());

        // While the root task is working, prepare what we need for the
        // end of the run. Go and try to report something to prepare the
        // reporting code for execution. This prepares classes, storage,
        // and some profiles for eventual execution.
        MeasurementsMap map = new MeasurementsMap();
        Row[] rows = new Row[MAP_SIZE];
        StringBuilder sb = new StringBuilder(16384);

        report(map, rows, sb);
        sb.setLength(0);

        // Nothing else is left to do preparation-wise. Now see if we can clean up
        // buffers that tasks do not need anymore. The root task would communicate
        // that it is done by giving us a poison pill.
        ByteBuffer buf;
        while ((buf = RELEASABLE_BUFFERS.take()) != RELEASABLE_BUFFER_POISON_PILL) {
            DirectUnmaps.invokeCleaner(buf);
        }

        // All done. Merge results from thread-local maps...
        for (MeasurementsMap m : ALL_MAPS) {
            map.merge(m);
        }

        // ...and truly report them
        System.out.println(report(map, rows, sb));
    }

    private static String report(MeasurementsMap map, Row[] rows, StringBuilder sb) {
        int rowCount = map.fill(rows);
        Arrays.sort(rows, 0, rowCount);

        sb.append("{");
        boolean first = true;
        for (int c = 0; c < rowCount; c++) {
            if (c != 0) {
                sb.append(", ");
            }
            rows[c].printTo(sb);
        }
        sb.append("}");
        return sb.toString();
    }

    // ========================= Reporting =========================

    private static final class Row implements Comparable<Row> {
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

    // ========================= Utils =========================

    // Tries to figure out if calling Cleaner directly on the DirectByteBuffer
    // is possible. If this fails, we still go on.
    public static class DirectUnmaps {
        private static final Method METHOD_GET_CLEANER;
        private static final Method METHOD_CLEANER_CLEAN;

        static Method getCleaner() {
            try {
                ByteBuffer dbb = ByteBuffer.allocateDirect(1);
                Method m = dbb.getClass().getMethod("cleaner");
                m.setAccessible(true);
                return m;
            }
            catch (NoSuchMethodException | InaccessibleObjectException e) {
                return null;
            }
        }

        static Method getCleanerClean(Method methodGetCleaner) {
            try {
                ByteBuffer dbb = ByteBuffer.allocateDirect(1);
                Object cleaner = methodGetCleaner.invoke(dbb);
                Method m = cleaner.getClass().getMethod("clean");
                m.setAccessible(true);
                m.invoke(cleaner);
                return m;
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InaccessibleObjectException e) {
                return null;
            }
        }

        static {
            METHOD_GET_CLEANER = getCleaner();
            METHOD_CLEANER_CLEAN = (METHOD_GET_CLEANER != null) ? getCleanerClean(METHOD_GET_CLEANER) : null;
        }

        public static void invokeCleaner(ByteBuffer bb) {
            if (METHOD_GET_CLEANER == null || METHOD_CLEANER_CLEAN == null) {
                return;
            }
            try {
                METHOD_CLEANER_CLEAN.invoke(METHOD_GET_CLEANER.invoke(bb));
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException("Cannot happen at this point", e);
            }
        }
    }

}
