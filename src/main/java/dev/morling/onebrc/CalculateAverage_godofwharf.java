package dev.morling.onebrc;

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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;

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
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_godofwharf {
    private static final String FILE = "./measurements.txt";
    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));
    private static final int NCPU = Runtime.getRuntime().availableProcessors();

    private static final VectorSpecies<Byte> PREFERRED_SPECIES = VectorSpecies.ofPreferred(byte.class);

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
    private static final int MAX_STR_LEN = 108;
    private static final int DEFAULT_HASH_TBL_SIZE = 4096;
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
                System.getProperty("threads", STR."\{NCPU}"));
        printDebugMessage("Running program with %d threads %n", nThreads);
        Job job = new Job(nThreads - 1);
        job.compute(FILE);
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
                int splitLength = (int) Math.min(Integer.MAX_VALUE, Math.max(PAGE_SIZE, Math.rint(fileLength * 1.0 / nThreads)));
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
                                MemorySegment splitSegment = globalSegment.asSlice(split.offset, split.length);
                                splitSegment.load();
                                int tid = (int) Thread.currentThread().threadId();
                                byte[] currentPage = new byte[PAGE_SIZE + MAX_STR_LEN];
                                // iterate over each page in split
                                for (Page page : split.pages) {
                                    // this byte buffer should end with '\n' or EOF
                                    MemorySegment segment = globalSegment.asSlice(page.offset, page.length);
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
                                        byte[] station = new byte[stationLen];
                                        System.arraycopy(currentPage, prevOffset, station, 0, stationLen);
                                        int hashcode = Arrays.hashCode(station);
                                        double temperature = NumberUtils.parseDouble2(currentPage, prevOffset + stationLen + 1, temperatureLen);
                                        Measurement m = new Measurement(station, temperature, hashcode);
                                        threadLocalStates[tid].update(m);
                                        prevOffset = curOffset + 1;
                                        j++;
                                    }
                                    // Explicitly commented out because unload seems to take a lot of time
                                    // segment.unload();
                                }
                                mergeInternal(threadLocalStates[tid]);
                            }));
                        });
                for (Future<?> future : futures) {
                    future.get();
                }
                printDebugMessage("Aggregate took %d ns%n", (System.nanoTime() - time));
            }
        }

        private void mergeInternal(final State state) {
            state.state.forEach((k, v) -> {
                globalMap.compute(k.toString(), (ignored, agg) -> {
                    if (agg == null) {
                        agg = v;
                    }
                    else {
                        agg.merge(v);
                    }
                    return agg;
                });
            });
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
            SearchResult ret = new SearchResult(new int[pageLen / 5], 0);
            VectorSpecies<Byte> species = PREFERRED_SPECIES;
            int loopBound = pageLen - species.length() * 4;
            int i = 0;
            int j = 0;
            while (j < loopBound) {
                Vector<Byte> v1 = ByteVector.fromArray(species, page, j);
                Vector<Byte> v2 = ByteVector.fromArray(species, page, j + species.length());
                Vector<Byte> v3 = ByteVector.fromArray(species, page, j + species.length() * 2);
                Vector<Byte> v4 = ByteVector.fromArray(species, page, j + species.length() * 3);
                long l1 = NEW_LINE_VEC.eq(v1).toLong();
                long l2 = NEW_LINE_VEC.eq(v2).toLong();
                long l3 = NEW_LINE_VEC.eq(v3).toLong();
                long l4 = NEW_LINE_VEC.eq(v4).toLong();
                long r1 = l1 & 0xFFFFFFFFL | (l2 << species.length());
                long r2 = l3 & 0xFFFFFFFFL | (l4 << (species.length()));
                int b1 = Long.bitCount(r1);
                int b2 = Long.bitCount(r2);
                int k = i;
                int it = b1;
                while (it > 0) {
                    int idx = Long.numberOfTrailingZeros(r1);
                    ret.offsets[k++] = j + idx;
                    r1 &= (r1 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r1);
                    ret.offsets[k++] = j + idx;
                    r1 &= (r1 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r1);
                    ret.offsets[k++] = j + idx;
                    r1 &= (r1 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r1);
                    ret.offsets[k++] = j + idx;
                    r1 &= (r1 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r1);
                    ret.offsets[k++] = j + idx;
                    r1 &= (r1 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r1);
                    ret.offsets[k++] = j + idx;
                    r1 &= (r1 - 1);
                    it--;
                }
                i += b1;
                j += species.length() * 2;
                k = i;
                it = b2;
                while (it > 0) {
                    int idx = Long.numberOfTrailingZeros(r2);
                    ret.offsets[k++] = j + idx;
                    r2 &= (r2 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r2);
                    ret.offsets[k++] = j + idx;
                    r2 &= (r2 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r2);
                    ret.offsets[k++] = j + idx;
                    r2 &= (r2 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r2);
                    ret.offsets[k++] = j + idx;
                    r2 &= (r2 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r2);
                    ret.offsets[k++] = j + idx;
                    r2 &= (r2 - 1);
                    it--;
                    idx = Long.numberOfTrailingZeros(r2);
                    ret.offsets[k++] = j + idx;
                    r2 &= (r2 - 1);
                    it--;
                }
                i += b2;
                j += species.length() * 2;
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
    }

    public static class State {
        private final Map<AggregationKey, MeasurementAggregator> state;

        public State() {
            this.state = new HashMap<>(DEFAULT_HASH_TBL_SIZE);
            // insert a DUMMY key to prime the hashmap for usage
            AggregationKey dummy = new AggregationKey("DUMMY".getBytes(UTF_8), -1);
            this.state.put(dummy, null);
            this.state.remove(dummy);
        }

        public void update(final Measurement m) {
            MeasurementAggregator agg = state.get(m.aggregationKey);
            if (agg == null) {
                state.put(m.aggregationKey, new MeasurementAggregator(m.temperature, m.temperature, m.temperature, 1L));
                return;
            }
            agg.count++;
            agg.min = m.temperature <= agg.min ? m.temperature : agg.min;
            agg.max = m.temperature >= agg.max ? m.temperature : agg.max;
            agg.sum += m.temperature;
        }

        public static class AggregationKey {
            private final byte[] station;
            private final int hashCode;

            public AggregationKey(final byte[] station,
                                  final int hashCode) {
                this.station = station;
                this.hashCode = hashCode;
            }

            @Override
            public String toString() {
                return new String(station, UTF_8);
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
                return station.length == sk.station.length && Arrays.mismatch(station, sk.station) < 0;
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
            double mean = round(round(sum) / count);
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

        public static double parseDouble2(final byte[] b,
                                          final int offset,
                                          final int len) {
            try {
                char ch0 = (char) b[offset];
                char ch1 = (char) b[offset + 1];
                char ch2 = (char) b[offset + 2];
                char ch3 = len > 3 ? (char) b[offset + 3] : ' ';
                char ch4 = len > 4 ? (char) b[offset + 4] : ' ';
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
                       double temperature,
                       int hash,
                       State.AggregationKey aggregationKey) {

    public Measurement(byte[] station,
                       double temperature,
                       int hashCode) {
            this(station,
                    temperature,
                    hashCode,
                    new State.AggregationKey(station, hashCode));
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

    private static void printDebugMessage(final String message,
                                          final Object... args) {
        if (DEBUG) {
            System.err.printf(message, args);
        }
    }
}
