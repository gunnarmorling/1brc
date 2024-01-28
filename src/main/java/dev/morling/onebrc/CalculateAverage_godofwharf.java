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

import jdk.incubator.vector.*;
import jdk.incubator.vector.Vector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_godofwharf {
    private static final String FILE = "./measurements.txt";
    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));

    private static final VectorSpecies<Byte> PREFERRED_SPECIES = VectorSpecies.ofPreferred(byte.class);
    private static final VectorSpecies<Byte> SMALL_SPECIES = VectorSpecies.of(byte.class, VectorShape.S_64_BIT);
    private static final VectorSpecies<Byte> MEDIUM_SPECIES = VectorSpecies.of(byte.class, VectorShape.S_128_BIT);

    private static final Vector<Byte> NEW_LINE_VEC = PREFERRED_SPECIES.broadcast('\n');
    // This array is used for quick conversion of fractional part
    private static final double[] DOUBLES = new double[]{ 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };
    // This array is used for quick conversion from ASCII to digit
    private static final int[] DIGIT_LOOKUP = new int[]{
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, 0, 1,
            2, 3, 4, 5, 6, 7, 8, 9, -1, -1 };
    private static final int MAX_STR_LEN = 100;
    private static final int DEFAULT_HASH_TBL_SIZE = 10007;
    private static final int DEFAULT_PAGE_SIZE = 8_388_608; // 8 MB
    private static final int PAGE_SIZE = Integer.parseInt(System.getProperty("pageSize", STR."\{DEFAULT_PAGE_SIZE}"));

    public static void main(String[] args) throws Exception {
        long startTimeMs = System.currentTimeMillis();
        Map<String, MeasurementAggregator> measurements = compute();
        long time1 = System.nanoTime();
        System.out.println(measurements);
        printDebugMessage("Print took %d ns%n", (System.nanoTime() - time1));
        printDebugMessage("Took %d ms%n", System.currentTimeMillis() - startTimeMs);
        printDebugMessage("Time spent on GC=%d ms%n", ManagementFactory.getGarbageCollectorMXBeans().get(0).getCollectionTime());
        System.exit(0);
    }

    private static Map<String, MeasurementAggregator> compute() throws Exception {
        int nThreads = Integer.parseInt(
                System.getProperty("threads", "" + Runtime.getRuntime().availableProcessors()));
        printDebugMessage("Running program with %d threads %n", nThreads);
        Job job = new Job(nThreads - 1);
        job.compute(FILE);
        job.merge();
        return job.sort();
    }

    public static class Job {
        private final int nThreads;
        private final State[] threadLocalStates;
        private final Map<String, MeasurementAggregator> globalMap = new ConcurrentHashMap<>(DEFAULT_HASH_TBL_SIZE);
        private final ExecutorService executorService;

        public Job(final int nThreads) {
            this.threadLocalStates = new State[(nThreads << 4)];
            IntStream.range(0, nThreads << 4)
                    .forEach(i -> threadLocalStates[i] = new State());
            this.nThreads = nThreads;
            this.executorService = Executors.newFixedThreadPool(nThreads);
        }

        public void compute(final String path) throws Exception {
            // Create a random access file so that we can map the contents of the file into native memory for faster access
            try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
                // Create a memory segment for the entire file
                MemorySegment globalSegment = file.getChannel().map(
                        FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
                long fileLength = file.length();
                // Ensure that the split length never exceeds Integer.MAX_VALUE. This is because ByteBuffers cannot
                // be larger than 2 GiB.
                int splitLength = (int) Math.min(Integer.MAX_VALUE, Math.rint(fileLength * 1.0 / nThreads));
                printDebugMessage("fileLength = %d, splitLength = %d%n", file.length(), splitLength);
                long time1 = System.nanoTime();
                // Break the file into multiple splits. One thread would process one split.
                // This routine makes sure that the splits are uniformly sized to the best extent possible.
                // Each split would either end with a '\n' character or EOF
                List<Split> splits = breakFileIntoSplits(file, splitLength, PAGE_SIZE, globalSegment, false);
                printDebugMessage("Number of splits = %d, splits = [%s]%n", splits.size(), splits);
                printDebugMessage("Splits calculation took %d ns%n", System.nanoTime() - time1);
                // consume splits in parallel using the common fork join pool
                long time = System.nanoTime();
                List<Future<?>> futures = new ArrayList<>(splits.size() * 2);
                splits
                        .forEach(split -> {
                            // process splits concurrently using a thread pool
                            futures.add(executorService.submit(() -> {
                                int tid = (int) Thread.currentThread().threadId();
                                byte[] currentPage = new byte[PAGE_SIZE + MAX_STR_LEN];
                                long[] hashCodes = new long[2];
                                // iterate over each page in split
                                for (Page page : split.pages) {
                                    // this byte buffer should end with '\n' or EOF
                                    MemorySegment segment = globalSegment.asSlice(page.offset, page.length);
                                    segment.load();
                                    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0L, currentPage, 0, (int) page.length);
                                    SearchResult searchResult = findNewLinesVectorized(currentPage, (int) page.length);
                                    int prevOffset = 0;
                                    int j = 0;
                                    // iterate over search results
                                    while (j < searchResult.len) {
                                        int curOffset = searchResult.offsets[j];
                                        byte ch1 = currentPage[curOffset - 4];
                                        byte ch2 = currentPage[curOffset - 5];
                                        int temperatureLen = 5;
                                        if (ch1 == ';') {
                                            temperatureLen = 3;
                                        }
                                        else if (ch2 == ';') {
                                            temperatureLen = 4;
                                        }
                                        int lineLength = curOffset - prevOffset;
                                        int stationLen = lineLength - temperatureLen - 1;
                                        byte[] temperature = new byte[temperatureLen];
                                        byte[] station = new byte[stationLen];
                                        System.arraycopy(currentPage, prevOffset, station, 0, stationLen);
                                        System.arraycopy(currentPage, prevOffset + stationLen + 1, temperature, 0, temperatureLen);
                                        hashCodes[0] = computeHashCode1(station);
                                        hashCodes[1] = computeHashCode2(station);
                                        Measurement m = new Measurement(
                                                station, stationLen, temperature, temperatureLen, false, hashCodes);
                                        threadLocalStates[tid].update(m);
                                        prevOffset = curOffset + 1;
                                        j++;
                                    }
                                    // Explicitly commented out because unload seems to take a lot of time
                                    segment.unload();
                                }
                            }));
                        });
                for (Future<?> future : futures) {
                    future.get();
                }
                printDebugMessage("Aggregate took %d ns%n", (System.nanoTime() - time));
            }
        }

        public void merge() {
            long time = System.nanoTime();
            Arrays.stream(threadLocalStates)
                    .parallel()
                    .filter(Objects::nonNull)
                    .forEach(threadLocalState -> threadLocalState.state
                            .forEach((k, v) -> {
                                globalMap.compute(k.toString(), (ignored, agg) -> {
                                    if (agg == null) {
                                        agg = v;
                                    }
                                    else {
                                        agg.merge(v);
                                    }
                                    return agg;
                                });
                            }));
            printDebugMessage("Merge took %d ns%n", (System.nanoTime() - time));
        }

        public Map<String, MeasurementAggregator> sort() {
            long time = System.nanoTime();
            Map<String, MeasurementAggregator> sortedMap = new TreeMap<>(globalMap);
            printDebugMessage("Tree map construction took %d ns%n", (System.nanoTime() - time));
            return sortedMap;
        }

        private static LineMetadata findNextOccurrenceOfNewLine(final ByteBuffer buffer,
                                                                final int capacity,
                                                                final int offset) {
            int maxLen = capacity - offset;
            byte[] src = new byte[Math.min(MAX_STR_LEN, maxLen)];
            byte[] station = new byte[src.length];
            byte[] temperature = new byte[5];
            buffer.position(offset);
            buffer.get(src);
            int i = 0;
            int j = 0;
            int k = 0;
            boolean isAscii = true;
            boolean afterDelim = false;
            int hashCode = 0;
            for (; i < src.length; i++) {
                byte b = src[i];
                if (b < 0) {
                    isAscii = false;
                }
                if (!afterDelim && b != '\n') {
                    if (b == ';') {
                        afterDelim = true;
                    }
                    else {
                        hashCode = hashCode * 31 + b;
                        station[j++] = b;
                    }
                }
                else if (b != '\n') {
                    temperature[k++] = b;
                }
                else {
                    return new LineMetadata(
                            station, temperature, j, k, offset + i + 1, hashCode, isAscii);
                }
            }
            if (i == 0 & j == 0 && k == 0) {
                hashCode = -1;
            }
            return new LineMetadata(
                    station, temperature, j, k, offset + i, hashCode, isAscii);
        }

        private static SearchResult findNewLinesVectorized(final byte[] page,
                                                           final int pageLen) {
            SearchResult ret = new SearchResult(new int[pageLen / 10], 0);
            int loopLength = PREFERRED_SPECIES.length();
            int loopBound = PREFERRED_SPECIES.loopBound(pageLen);
            int i = 0;
            int j = 0;
            int[] positions = new int[64];
            while (j < loopBound) {
                Vector<Byte> vec = ByteVector.fromArray(PREFERRED_SPECIES, page, j);
                long res = vec.eq(NEW_LINE_VEC).toLong();
                int k = 0;
                int bitCount = Long.bitCount(res);
                while (res > 0) {
                    int idx = Long.numberOfTrailingZeros(res);
                    positions[k++] = j + idx;
                    res &= (res - 1);
                    idx = Long.numberOfTrailingZeros(res);
                    positions[k++] = j + idx;
                    res &= (res - 1);
                    idx = Long.numberOfTrailingZeros(res);
                    positions[k++] = j + idx;
                    res &= (res - 1);
                    idx = Long.numberOfTrailingZeros(res);
                    positions[k++] = j + idx;
                    res &= (res - 1);
                }
                System.arraycopy(positions, 0, ret.offsets, i, bitCount);
                j += loopLength;
                i += bitCount;
            }

            // tail loop
            while (j < pageLen) {
                byte b = page[j];
                if (b == '\n') {
                    ret.offsets[i++] = j;
                }
                j++;
            }
            ret.len = i;
            return ret;
        }

        private static List<Split> breakFileIntoSplits(final RandomAccessFile file,
                                                       final int splitLength,
                                                       final int pageLength,
                                                       final MemorySegment memorySegment,
                                                       final boolean enableChecks)
                throws IOException {
            final List<Split> splits = new ArrayList<>();
            // Try to break the file into multiple splits while ensuring that each split has at least splitLength bytes
            // and ends with '\n' or EOF
            for (long i = 0; i < file.length();) {
                long splitStartOffset = i;
                long splitEndOffset = Math.min(file.length(), splitStartOffset + splitLength); // not inclusive
                if (splitEndOffset == file.length()) { // reached EOF
                    List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment, enableChecks);
                    splits.add(new Split(splitStartOffset, splitEndOffset - splitStartOffset, pages));
                    break;
                }
                // Look past the end offset to find next '\n' or EOF
                long segmentLength = Math.min(MAX_STR_LEN, file.length() - i);
                // Create a new memory segment for reading contents beyond splitEndOffset
                MemorySegment lookahead = memorySegment.asSlice(splitEndOffset, segmentLength);
                ByteBuffer bb = lookahead.asByteBuffer();
                // Find the next offset which has either '\n' or EOF
                LineMetadata lineMetadata = findNextOccurrenceOfNewLine(bb, (int) segmentLength, 0);
                splitEndOffset += lineMetadata.offset;
                if (enableChecks &&
                        memorySegment.asSlice(splitEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                    throw new IllegalStateException("Page doesn't end with NL char");
                }
                // Break the split further into multiple pages based on pageLength
                List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment, enableChecks);
                splits.add(new Split(splitStartOffset, splitEndOffset - splitStartOffset, pages));
                i = splitEndOffset;
                lookahead.unload();
            }
            return splits;
        }

        private static List<Page> breakSplitIntoPages(final long splitStartOffset,
                                                      final long splitEndOffset,
                                                      final int pageLength,
                                                      final MemorySegment memorySegment,
                                                      final boolean enableChecks) {
            List<Page> pages = new ArrayList<>();
            for (long i = splitStartOffset; i < splitEndOffset;) {
                long pageStartOffset = i;
                long pageEndOffset = Math.min(splitEndOffset, pageStartOffset + pageLength); // not inclusive
                if (pageEndOffset == splitEndOffset) {
                    pages.add(new Page(pageStartOffset, pageEndOffset - pageStartOffset));
                    break;
                }
                // Look past the end offset to find next '\n' till we reach the end of split
                long lookaheadLength = Math.min(MAX_STR_LEN, splitEndOffset - i);
                MemorySegment lookahead = memorySegment.asSlice(pageEndOffset, lookaheadLength);
                ByteBuffer bb = lookahead.asByteBuffer();
                // Find next offset which has either '\n' or the end of split
                LineMetadata lineMetadata = findNextOccurrenceOfNewLine(bb, (int) lookaheadLength, 0);
                pageEndOffset += lineMetadata.offset;
                if (enableChecks &&
                        memorySegment.asSlice(pageEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                    throw new IllegalStateException("Page doesn't end with NL char");
                }
                pages.add(new Page(pageStartOffset, pageEndOffset - pageStartOffset));
                i = pageEndOffset;
                lookahead.unload();
            }
            return pages;
        }

        private static long computeHashCode1(final byte[] b) {
            long result = -2;
            for (byte value : b) {
                result = 31 * result + value;
            }
            return result;
        }

        private static long computeHashCode2(final byte[] b) {
            long result = -2;
            for (byte value : b) {
                result = 127 * result + value;
            }
            return result;
        }
    }

    public static class State {
        // private final Map<AggregationKey, MeasurementAggregator> state;
        // private final FastHashMap state;
        private final FastHashMap2 state;

        public State() {
            // this.state = new HashMap<>(DEFAULT_HASH_TBL_SIZE);
            // this.state = new FastHashMap(1 << 14);
            this.state = new FastHashMap2(DEFAULT_HASH_TBL_SIZE);
        }

        // Implementing the logic in update method instead of calling HashMap.compute() has reduced the runtime significantly
        // primarily because it causes update method to be inlined by the compiler into the calling loop
        // public void update(final Measurement m) {
        // MeasurementAggregator agg = state.get(m.aggregationKey);
        // if (agg == null) {
        // state.put(m.aggregationKey, new MeasurementAggregator(m.value, m.value, m.value, 1L));
        // return;
        // }
        // agg.count++;
        // agg.min = m.value <= agg.min ? m.value : agg.min;
        // agg.max = m.value >= agg.max ? m.value : agg.max;
        // agg.sum += m.value;
        // }

        public void update(final Measurement m) {
            state.compute(m.aggregationKey, (ignored, agg) -> {
                if (agg == null) {
                    return new MeasurementAggregator(m.value, m.value, m.value, 1L);
                }
                agg.count++;
                agg.min = m.value <= agg.min ? m.value : agg.min;
                agg.max = m.value >= agg.max ? m.value : agg.max;
                agg.sum += m.value;
                return agg;
            });
        }

        public static class AggregationKey {
            private final byte[] station;
            private final int stationLen;
            private final boolean isAscii;
            private final long[] hashCodes;

            public AggregationKey(final byte[] station,
                                  final int stationLen,
                                  final boolean isAscii,
                                  final long[] hashCodes) {
                this.station = station;
                this.stationLen = stationLen;
                this.isAscii = isAscii;
                this.hashCodes = hashCodes;
            }

            @Override
            public String toString() {
                return new String(station, 0, stationLen, isAscii ? US_ASCII : UTF_8);
            }

            @Override
            public int hashCode() {
                return (int) (hashCodes[0] & 0xFFFF);
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof AggregationKey)) {
                    return false;
                }
                AggregationKey sk = (AggregationKey) other;
                // return stationLen == sk.stationLen
                // && Arrays.equals(station, 0, stationLen, sk.station, 0, stationLen);
                return stationLen == sk.stationLen
                        && checkArrayEquals(station, sk.station, stationLen);
            }

            private boolean checkArrayEquals(final byte[] a1,
                                             final byte[] a2,
                                             final int len) {
                if (a1[0] != a2[0]) {
                    return false;
                }
                if (len < 8) {
                    for (int i = 1; i < len; i++) {
                        if (a1[i] != a2[i]) {
                            return false;
                        }
                    }
                    return true;
                }
                // use vectorized code for fast equals comparison
                return !vectorizedMismatch(MEDIUM_SPECIES, len, a1, a2);
            }

            private static boolean vectorizedMismatch(final VectorSpecies<Byte> species,
                                                      final int len,
                                                      final byte[] a1,
                                                      final byte[] a2) {
                int i = 0;
                int loopLength = species.length();
                int loopBound = species.loopBound(len);
                for (; i < loopBound; i += loopLength) {
                    Vector<Byte> b1 = ByteVector.fromArray(species, a1, i);
                    Vector<Byte> b2 = ByteVector.fromArray(species, a2, i);
                    VectorMask<Byte> result = b1.eq(b2);
                    if (!result.allTrue()) {
                        return true;
                    }
                }
                // tail loop
                for (; i < len; i++) {
                    if (a1[i] != a2[i]) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    public static class MeasurementAggregator {
        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator(final double min,
                                     final double max,
                                     final double sum,
                                     final long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public String toString() {
            double min1 = round(min);
            double max1 = round(max);
            double mean = round(sum / count);
            return min1 + "/" + mean + "/" + max1;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        private void merge(final MeasurementAggregator m2) {
            count += m2.count;
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
        }
    }

    public static class NumberUtils {
        public static int toDigit(final char c) {
            return DIGIT_LOOKUP[c];
        }

        public static int fastMul10(final int i) {
            return (i << 1) + (i << 3);
        }

        public static double parseDouble2(final byte[] b, final int len) {
            try {
                char ch0 = (char) b[0];
                char ch1 = (char) b[1];
                char ch2 = (char) b[2];
                char ch3 = len > 3 ? (char) b[3] : ' ';
                char ch4 = len > 4 ? (char) b[4] : ' ';
                if (len == 3) {
                    int decimal = toDigit(ch0);
                    double fractional = DOUBLES[toDigit(ch2)];
                    return decimal + fractional;
                }
                else if (len == 4) {
                    // -1.2 or 11.2
                    int decimal = (ch0 == '-' ? toDigit(ch1) : (fastMul10(toDigit(ch0)) + toDigit(ch1)));
                    double fractional = DOUBLES[toDigit(ch3)];
                    if (ch0 == '-') {
                        return Math.negateExact(decimal) - fractional;
                    }
                    else {
                        return decimal + fractional;
                    }
                }
                else {
                    int decimal = fastMul10(toDigit(ch1)) + toDigit(ch2);
                    double fractional = DOUBLES[toDigit(ch4)];
                    return Math.negateExact(decimal) - fractional;
                }
            }
            catch (ArrayIndexOutOfBoundsException e) {
                printDebugMessage("Array index out of bounds for string: %s%n", new String(b, 0, len));
                throw new RuntimeException(e);
            }
            catch (StringIndexOutOfBoundsException e) {
                printDebugMessage("String index out of bounds for string: %s%n", new String(b, 0, len));
                throw new RuntimeException(e);
            }
        }
    }

    // record classes
    record Measurement(byte[] station,
                       int stationLen,
                       double value,
                       boolean isAscii,
                       long[] hashCodes,
                       State.AggregationKey aggregationKey) {

    public Measurement(byte[] station,
                       int stationLen,
                       byte[] temperature,
                       int temperatureLen,
                       boolean isAscii,
                       long[] hashCodes) {
            this(station,
                    stationLen,
                    NumberUtils.parseDouble2(temperature, temperatureLen),
                    isAscii,
                    hashCodes,
                    new State.AggregationKey(station, stationLen, isAscii, hashCodes));
        }

    }

    record LineMetadata(byte[] station,
                        byte[] temperature,
                        int stationLen,
                        int temperatureLen,
                        int offset,
                        int precomputedHashCode, boolean isAscii) {
    }

    record Split(long offset, long length, List<Page> pages) {
    }

    record Page(long offset, long length) {
    }

    public static class SearchResult {
        private int[] offsets;
        private int len;

        public SearchResult(final int[] offsets,
                            final int len) {
            this.offsets = offsets;
            this.len = len;
        }
    }

    public static class FastHashMap {
        private TableEntry[] tableEntries;
        private int size;

        public FastHashMap(final int capacity) {
            this.size = tableSizeFor(capacity);
            this.tableEntries = new TableEntry[size];
        }

        private int tableSizeFor(final int capacity) {
            int n = -1 >>> Integer.numberOfLeadingZeros(capacity - 1);
            return (n < 0) ? 1 : (n >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : n + 1;
        }

        public void put(final State.AggregationKey key,
                        final MeasurementAggregator aggregator) {
            tableEntries[(int) ((size - 1) & key.hashCodes[0])] = new TableEntry(key, aggregator);
        }

        public MeasurementAggregator get(final State.AggregationKey key) {
            TableEntry entry = tableEntries[(int) ((size - 1) & key.hashCodes[0])];
            if (entry != null) {
                return entry.aggregator;
            }
            return null;
        }

        public void forEach(final BiConsumer<State.AggregationKey, MeasurementAggregator> action) {
            for (int i = 0; i < size; i++) {
                TableEntry entry = tableEntries[i];
                if (entry != null) {
                    action.accept(entry.key, entry.aggregator);
                }
            }
        }

        record TableEntry(State.AggregationKey key, MeasurementAggregator aggregator) {
        }
    }

    // A simple implementation of HashMap which only supports compute and forEach methods
    // This implementation is faster than Java's HashMap implementation because it uses open addressing (double hashing to be specific)
    // to resolve collisions.
    public static class FastHashMap2 {
        private TableEntry[] tableEntries;
        private int size;

        public FastHashMap2(final int size) {
            this.size = size;
            this.tableEntries = new TableEntry[size];
        }

        public void compute(final State.AggregationKey key,
                            final BiFunction<State.AggregationKey, MeasurementAggregator, MeasurementAggregator> function) {
            int idx = mod(key.hashCodes[0], size);
            // either find the corresponding entry if it exists (update) or find an empty slot for creating a new entry (insert)
            idx = probe(idx, key);
            TableEntry entry = tableEntries[idx];
            if (entry != null) {
                // update
                entry.aggregator = function.apply(key, entry.aggregator);
            }
            else {
                // insert
                tableEntries[idx] = new TableEntry(key, function.apply(key, null));
            }
        }

        public void forEach(final BiConsumer<State.AggregationKey, MeasurementAggregator> action) {
            for (int i = 0; i < size; i++) {
                TableEntry entry = tableEntries[i];
                if (entry != null) {
                    action.accept(entry.key, entry.aggregator);
                }
            }
        }

        private int mod(final long a,
                        final long b) {
            return (int) ((a % b + b) % b);
        }

        // This method tries to find the next possible idx in hash table which either contains no entry or contains
        // the key we are looking for
        // h1 - primary hashcode of key
        // h2 - secondary hashcode of key
        private int probe(final int idx,
                          final State.AggregationKey key) {
            int curIdx = idx;
            // if we find an empty slot, return immediately
            if (tableEntries[curIdx] == null) {
                return curIdx;
            }
            // we found a non-empty slot
            // check if we can use it
            // to check if a key exists in map, we compare both the hash codes and then check for key equality
            // boolean exists = tableEntries[curIdx].key.hashCodes[0] == key.hashCodes[0] &&
            // tableEntries[curIdx].key.hashCodes[1] == key.hashCodes[1] &&
            // tableEntries[curIdx].key.equals(key);
            boolean exists = tableEntries[curIdx].key.hashCodes[0] == key.hashCodes[0] &&
                    tableEntries[curIdx].key.hashCodes[1] == key.hashCodes[1];
            if (exists) {
                return curIdx;
            }

            // we need to search for other slots (empty/non-empty)
            // update curIdx to the next slot
            curIdx = mod(curIdx + key.hashCodes[1], size);

            // iterate until we find a slot which meets any of the following criteria
            // - slot is empty
            // - slot is non-empty but
            // - h1 doesn't match with key (or)
            // - h1 matches but h2 doesn't match with key (or)
            // - h1 and h2 match but station name doesn't match
            while (curIdx != idx &&
                    tableEntries[curIdx] != null &&
                    (tableEntries[curIdx].key.hashCodes[0] != key.hashCodes[0] ||
                            tableEntries[curIdx].key.hashCodes[1] != key.hashCodes[1] ||
                            !tableEntries[curIdx].key.equals(key))) {
                curIdx = mod(curIdx + key.hashCodes[1], size);
            }
            // if curIdx matches the idx we started with, then a cycle has occurred
            if (curIdx == idx) {
                throw new IllegalStateException("Probe failed because we can't find slot for key");
            }
            return curIdx;
        }

        public static class TableEntry {
            State.AggregationKey key;
            MeasurementAggregator aggregator;

            public TableEntry(final State.AggregationKey key,
                              final MeasurementAggregator aggregator) {
                this.key = key;
                this.aggregator = aggregator;
            }
        }

    }

    private static void printDebugMessage(final String message,
                                          final Object... args) {
        if (DEBUG) {
            System.err.printf(message, args);
        }
    }
}