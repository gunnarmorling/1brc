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
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_godofwharf {
    private static final String FILE = "./measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = VectorSpecies.ofPreferred(byte.class);
    private static final Vector<Byte> NEW_LINE_VEC = SPECIES.broadcast('\n');
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
    private static final int DEFAULT_HASH_TBL_SIZE = 10010;

    public static void main(String[] args) throws Exception {
        long startTimeMs = System.currentTimeMillis();
        Map<String, MeasurementAggregator> measurements = compute();
        long time1 = System.nanoTime();
        measurements.forEach((k, v) -> System.out.println("%s=%s".formatted(k, v)));
        // System.out.println(measurements);
        System.err.println("Print took " + (System.nanoTime() - time1) + " ns");
        System.err.printf("Took %d ms%n", System.currentTimeMillis() - startTimeMs);
        System.err.printf("Time spent on GC=%d ms%n", ManagementFactory.getGarbageCollectorMXBeans().get(0).getCollectionTime());
        System.exit(0);
    }

    private static Map<String, MeasurementAggregator> compute() throws Exception {
        int nThreads = Integer.parseInt(System.getProperty("threads",
                "" + Runtime.getRuntime().availableProcessors()));
        System.err.printf("Running program with %d threads %n", nThreads);
        Job job = new Job(nThreads - 1);
        job.compute(FILE);
        job.merge();
        return job.sort();
    }

    public static class Job {
        private final int nThreads;
        private final State[] threadLocalStates;
        private final Map<State.AggregationKey, MeasurementAggregator> ret = new ConcurrentHashMap<>(DEFAULT_HASH_TBL_SIZE);
        private final ExecutorService executorService;
        private final int pageSize = 8_388_608; // 8 MB

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
                MemorySegment memorySegment = file.getChannel().map(
                        FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
                long fileLength = file.length();
                // Ensure that the split length never exceeds Integer.MAX_VALUE. This is because ByteBuffers cannot
                // be larger than 2 GiB.
                int splitLength = (int) Math.min(Integer.MAX_VALUE, Math.rint(fileLength * 1.0 / nThreads));
                System.err.printf("fileLength = %d, splitLength = %d%n", file.length(), splitLength);
                long time1 = System.nanoTime();
                // Break the file into multiple splits. One thread would process one split.
                // This routine makes sure that the splits are uniformly sized to the best extent possible.
                // Each split would either end with a '\n' character or EOF
                List<Split> splits = breakFileIntoSplits(file, splitLength, pageSize, memorySegment);
                System.err.printf("Number of splits = %d, splits = [%s]%n", splits.size(), splits);
                System.err.printf("Splits calculation took %d ns%n", System.nanoTime() - time1);
                // consume splits in parallel using the common fork join pool
                long time = System.nanoTime();
                List<Future<?>> futures = new ArrayList<>(splits.size() * 2);
                splits
                        .forEach(split -> {
                            // process splits concurrently using a thread pool
                            futures.add(executorService.submit(() -> {
                                MemorySegment subSegment = memorySegment.asSlice(split.offset, split.length);
                                // this byte buffer should end with '\n' or EOF
                                subSegment.load();
                                ByteBuffer bb = subSegment.asByteBuffer();
                                int tid = (int) Thread.currentThread().threadId();
                                byte[] currentPage = new byte[pageSize + MAX_STR_LEN];
                                int bytesRead = 0;
                                // iterate over each page in split
                                for (Page page : split.pages) {
                                    bb.get(bytesRead, currentPage, 0, (int) page.length);
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
                                        int hashCode = Arrays.hashCode(station);
                                        Measurement m = new Measurement(
                                                station, stationLen, temperature, temperatureLen, false, hashCode);
                                        threadLocalStates[tid].update(m);
                                        prevOffset = curOffset + 1;
                                        j++;
                                    }
                                    bytesRead += (int) page.length;
                                }
                                // Explicitly commented out because unload seems to take a lot of time
                                subSegment.unload();
                            }));
                        });
                for (Future<?> future : futures) {
                    future.get();
                }
                System.err.println("Aggregate took " + (System.nanoTime() - time) + " ns");
            }
        }

        public void merge() {
            long time = System.nanoTime();
            Arrays.stream(threadLocalStates)
                    .parallel()
                    .filter(Objects::nonNull)
                    .forEach(threadLocalState -> threadLocalState.state
                            .forEach((k, v) -> {
                                ret.compute(k, (ignored, agg) -> {
                                    if (agg == null) {
                                        agg = v;
                                    }
                                    else {
                                        agg.merge(v);
                                    }
                                    return agg;
                                });
                            }));
            System.err.println("Merge took " + (System.nanoTime() - time) + " ns");
        }

        public Map<String, MeasurementAggregator> sort() {
            long time = System.nanoTime();
            Map<String, MeasurementAggregator> ret2 = new TreeMap<>();
            ret.forEach((k, v) -> ret2.put(k.toString(), v));
            System.err.println("Tree map construction took " + (System.nanoTime() - time) + " ns");
            return ret2;
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
            int loopLength = SPECIES.length();
            int loopBound = SPECIES.loopBound(pageLen);
            int i = 0;
            int j = 0;
            int[] positions = new int[64];
            while (j < loopBound) {
                Vector<Byte> vec = ByteVector.fromArray(SPECIES, page, j);
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
                                                       final MemorySegment memorySegment)
                throws IOException {
            final List<Split> splits = new ArrayList<>();
            // Try to break the file into multiple splits while ensuring that each split has at least splitLength bytes
            // and ends with '\n' or EOF
            for (long i = 0; i < file.length();) {
                long splitStartOffset = i;
                long splitEndOffset = Math.min(file.length(), splitStartOffset + splitLength); // not inclusive
                if (splitEndOffset == file.length()) { // reached EOF
                    List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment);
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
                if (memorySegment.asSlice(splitEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                    throw new IllegalStateException("Page doesn't end with NL char");
                }
                // Break the split further into multiple pages based on pageLength
                List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment);
                splits.add(new Split(splitStartOffset, splitEndOffset - splitStartOffset, pages));
                i = splitEndOffset;
                lookahead.unload();
            }
            return splits;
        }

        private static List<Page> breakSplitIntoPages(final long splitStartOffset,
                                                      final long splitEndOffset,
                                                      final int pageLength,
                                                      final MemorySegment memorySegment) {
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
                if (memorySegment.asSlice(pageEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                    throw new IllegalStateException("Page doesn't end with NL char");
                }
                pages.add(new Page(pageStartOffset, pageEndOffset - pageStartOffset));
                i = pageEndOffset;
                lookahead.unload();
            }
            return pages;
        }
    }

    public static class State {
        private final Map<AggregationKey, MeasurementAggregator> state;
        // private final FastHashMap state;

        public State() {
            this.state = new HashMap<>(DEFAULT_HASH_TBL_SIZE);
            // this.state = new FastHashMap(DEFAULT_HASH_TBL_SIZE);
        }

        // Implementing the logic in update method instead of calling HashMap.compute() has reduced the runtime significantly
        // primarily because it causes update method to be inlined by the compiler into the calling loop
        public void update(final Measurement m) {
            MeasurementAggregator agg = state.get(m.aggregationKey);
            if (agg == null) {
                state.put(m.aggregationKey, new MeasurementAggregator(m.value, m.value, m.value, 1L));
                return;
            }
            agg.count++;
            agg.min = m.value <= agg.min ? m.value : agg.min;
            agg.max = m.value >= agg.max ? m.value : agg.max;
            agg.sum += m.value;
        }

        public static class AggregationKey {
            private final byte[] station;
            private final int stationLen;
            private final boolean isAscii;
            private final int hashCode;

            public AggregationKey(final byte[] station,
                                  final int stationLen,
                                  final boolean isAscii,
                                  final int hashCode) {
                this.station = station;
                this.stationLen = stationLen;
                this.isAscii = isAscii;
                this.hashCode = hashCode;
            }

            @Override
            public String toString() {
                return new String(station, 0, stationLen, isAscii ? US_ASCII : UTF_8);
            }

            @Override
            public int hashCode() {
                return hashCode;
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof AggregationKey)) {
                    return false;
                }
                AggregationKey sk = (AggregationKey) other;
                return stationLen == sk.stationLen
                        && Arrays.equals(station, 0, stationLen, sk.station, 0, stationLen);
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
                System.err.printf("Array index out of bounds for string: %s%n", new String(b, 0, len));
                throw new RuntimeException(e);
            }
            catch (StringIndexOutOfBoundsException e) {
                System.err.printf("String index out of bounds for string: %s%n", new String(b, 0, len));
                throw new RuntimeException(e);
            }
        }
    }

    // record classes
    record Measurement(byte[] station,
                       int stationLen,
                       double value,
                       boolean isAscii,
                       int precomputedHashCode,
                       State.AggregationKey aggregationKey) {
        public Measurement(byte[] station,
                           int stationLen,
                           byte[] temperature,
                           int temperatureLen,
                           boolean isAscii,
                           int hashCode) {
            this(station,
                    stationLen,
                    NumberUtils.parseDouble2(temperature, temperatureLen),
                    isAscii,
                    hashCode,
                    new State.AggregationKey(station, stationLen, isAscii, hashCode));
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
            tableEntries[(size - 1) & key.hashCode] = new TableEntry(key, aggregator);
        }

        public MeasurementAggregator get(final State.AggregationKey key) {
            TableEntry entry = tableEntries[(size - 1) & key.hashCode];
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
}