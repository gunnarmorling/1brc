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
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_godofwharf_all {
    private static final VectorSpecies<Byte> SPECIES = VectorSpecies.ofPreferred(byte.class);
    private static final Vector<Byte> NEW_LINE_VEC = SPECIES.broadcast('\n');

    private static final String FILE = "./measurements.txt";
    private static final double[] DOUBLES = new double[]{ 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };
    private static final int[] DIGIT_LOOKUP = new int[]{
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, 0, 1,
            2, 3, 4, 5, 6, 7, 8, 9, -1, -1 };
    private static final int MAX_STR_LEN = 100;

    // public record Measurement(String station, double value, long tid) {
    // public Measurement(String[] parts) {
    // this(parts[0], Double.parseDouble(parts[1]), -1);
    // }
    //
    // public Measurement(String[] parts,
    // boolean fast) {
    // this(parts[0], parseDouble2(parts[1]), -1);
    // }
    //
    // public Measurement(String[] parts,
    // long tid) {
    // this(parts[0], parseDouble2(parts[1]), tid);
    // }
    //
    // public Measurement(String station,
    // String tempValue) {
    // this(station, parseDouble2(tempValue), -1);
    // }
    // }

    public record Measurement(byte[] station,
                              int stationLen,
                              double value,
                              boolean isAscii,
                              int precomputedHashCode,
                              long tid,
                              State.StateKey stateKey) {
        public Measurement(byte[] station,
                           int stationLen,
                           byte[] temperature,
                           int temperatureLen,
                           boolean isAscii,
                           int hashCode,
                           long tid) {
            this(station,
                    stationLen,
                    parseDouble2(temperature, temperatureLen),
                    isAscii,
                    hashCode,
                    tid,
                    new State.StateKey(station, stationLen, isAscii, hashCode));
        }
    }

    // public static class Measurement2 {
    // String station;
    // double value;
    // long tid;
    // int hashCode;
    //
    // public Measurement2(final String station,
    // final double value,
    // final long tid,
    // final int hashCode) {
    // this.station = station;
    // this.value = value;
    // this.tid = tid;
    // this.hashCode = hashCode;
    // }
    //
    // public Measurement2(String[] parts,
    // long tid,
    // int hashCode) {
    // this(parts[0], parseDouble2(parts[1]), tid, hashCode);
    // }
    //
    // @Override
    // public String toString() {
    // return station;
    // }
    //
    // @Override
    // public int hashCode() {
    // return hashCode;
    // }
    //
    // @Override
    // public boolean equals(final Object other) {
    // if (!(other instanceof Measurement2)) {
    // return false;
    // }
    // return station.equals(((Measurement2) other).station);
    // }
    // }

    // private record ResultRow(double min, double mean, double max) {
    // public String toString() {
    // return round(min) + "/" + round(mean) + "/" + round(max);
    // }
    //
    // private double round(double value) {
    // return Math.round(value * 10.0) / 10.0;
    // }
    // };

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

    public static void main(String[] args) throws Exception {
        long startTimeMs = System.currentTimeMillis();
        // Map<String, ResultRow> measurements = compute_Baseline(collector);
        // Map<String, ResultRow> measurements = compute_Listing_3(collector);
        // Map<String, ResultRow> measurements = compute_Listing_4(collector);
        // Map<String, ResultRow> measurements = compute_Listing_5(collector);
        // Map<String, ResultRow> measurements = compute_Listing_6(collector);
        // Map<String, ResultRow> measurements = compute_Listing_7(collector);
        // Map<String, MeasurementAggregator2> measurements = compute_Listing_8();
        // Map<String, MeasurementAggregator2> measurements = compute_Listing_9();
        // Map<String, MeasurementAggregator2> measurements = compute_Listing_11();
        // Map<String, MeasurementAggregator2> measurements = compute_Listing_12();
        Map<String, MeasurementAggregator> measurements = compute_Listing_13();
        // Map<String, MeasurementAggregator2> measurements = compute_Listing_14();
        // Map<String, MeasurementAggregator> measurements = compute_Listing_15();
        long time1 = System.nanoTime();
        System.out.println(measurements);
        System.err.println("Print took " + (System.nanoTime() - time1) + " ns");
        System.err.printf("Took %d ms%n", System.currentTimeMillis() - startTimeMs);
        System.err.printf("Time spent on GC=%d ms%n", ManagementFactory.getGarbageCollectorMXBeans().get(0).getCollectionTime());
        System.exit(0);
    }

    // private static Map<String, ResultRow> compute_Baseline(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>(Files.lines(Paths.get(FILE))
    // .map(l -> new Measurement(l.split(";")))
    // .collect(groupingBy(m -> m.station(), collector)));
    // }

    // // REJECTED - Slower than baseline
    // // String split method handles split calls with regex containing a single non-regex character in a different way
    // // In such cases, a simple O(n) loop with character match and substring works out to be faster than using compiled regex pattern
    // private static Map<String, ResultRow> compute_Listing_1(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // Pattern pattern = Pattern.compile(";");
    // return new TreeMap<>(Files.lines(Paths.get(FILE))
    // .map(l -> new Measurement(pattern.split(l)))
    // .collect(groupingBy(m -> m.station(), collector)));
    // }
    //
    // // Faster than baseline
    // // Pre-allocation of memory by setting capacity correctly
    // private static Map<String, ResultRow> compute_Listing_2(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>(Files.lines(Paths.get(FILE))
    // .map(l -> new Measurement(splitByChar(l, ';')))
    // .collect(groupingBy(m -> m.station(), collector)));
    // }
    //
    // // Use common Fork-Join pool to read the lines
    // private static Map<String, ResultRow> compute_Listing_3(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>((Map<String, ResultRow>) new OptimizedFileReader().readLines(FILE)
    // .map(l -> new Measurement(splitByChar(l, ';')))
    // .collect(groupingBy(m -> m.station(), ConcurrentHashMap::new, collector)));
    // }
    //
    // private static Map<String, ResultRow> compute_Listing_4(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>(new OptimizedFileReader().readLines(FILE)
    // .map(l -> new Measurement(l.split(";")))
    // .collect(groupingBy(m -> m.station(), collector)));
    // }
    //
    // private static Map<String, ResultRow> compute_Listing_5(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>((Map<String, ResultRow>) new OptimizedFileReader().readLines(FILE)
    // .map(l -> new Measurement(splitByChar2(l, ';')))
    // .collect(groupingBy(m -> m.station(), ConcurrentHashMap::new, collector)));
    // }
    //
    // private static Map<String, ResultRow> compute_Listing_6(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>((Map<String, ResultRow>) new OptimizedFileReader().readLines(FILE)
    // .map(l -> new Measurement(splitByChar2(l, ';'), true))
    // .collect(groupingBy(m -> m.station(), ConcurrentHashMap::new, collector)));
    // }
    //
    // private static Map<String, ResultRow> compute_Listing_7(Collector<Measurement, MeasurementAggregator, ResultRow> collector) throws IOException {
    // return new TreeMap<>((Map<String, ResultRow>) new OptimizedFileReader().readLines(FILE)
    // .map(l -> new Measurement(splitByChar2(l, ';'), true))
    // .collect(groupingBy(m -> m.station(), () -> new ConcurrentHashMap<>(10001), collector)));
    // }
    //
    // private static Map<String, MeasurementAggregator2> compute_Listing_8() throws IOException {
    // Map<String, MeasurementAggregator2> ret = new ConcurrentHashMap<>(10001);
    // new OptimizedFileReader().readLines(FILE)
    // .forEach(line -> {
    // Measurement m = new Measurement(splitByChar2(line, ';'), true);
    // ret.compute(m.station(), (k, v) -> {
    // if (v == null) {
    // v = new MeasurementAggregator2();
    // }
    // v.count++;
    // v.min = Math.min(v.min, m.value);
    // v.max = Math.max(v.max, m.value);
    // v.sum += m.value;
    // return v;
    // });
    // });
    // // long startTimeMs = System.currentTimeMillis();
    // Map<String, MeasurementAggregator2> ret2 = new TreeMap<>(ret);
    // // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
    // return ret2;
    // }
    //
    // private static Map<String, MeasurementAggregator2> compute_Listing_9() throws IOException {
    // Map<String, MeasurementAggregator2> ret = new ConcurrentHashMap<>(10001);
    // new OptimizedFileReader2().readLines(FILE)
    // .forEach(line -> {
    // Measurement m = new Measurement(splitByChar2(line, ';'), true);
    // ret.compute(m.station(), (k, v) -> {
    // if (v == null) {
    // v = new MeasurementAggregator2();
    // }
    // v.count++;
    // v.min = Math.min(v.min, m.value);
    // v.max = Math.max(v.max, m.value);
    // v.sum += m.value;
    // return v;
    // });
    // });
    // // long startTimeMs = System.currentTimeMillis();
    // Map<String, MeasurementAggregator2> ret2 = new TreeMap<>(ret);
    // // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
    // return ret2;
    // }
    //
    // private static Map<String, MeasurementAggregator2> compute_Listing_10() throws IOException {
    // Map<String, MeasurementAggregator2> ret = new ConcurrentHashMap<>(10001);
    // new ReadFromDisk().compute(FILE, m -> {
    // ret.compute(m.station(), (k, v) -> {
    // if (v == null) {
    // v = new MeasurementAggregator2();
    // }
    // v.count++;
    // v.min = Math.min(v.min, m.value);
    // v.max = Math.max(v.max, m.value);
    // v.sum += m.value;
    // return v;
    // });
    // });
    // // long startTimeMs = System.currentTimeMillis();
    // Map<String, MeasurementAggregator2> ret2 = new TreeMap<>(ret);
    // // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
    // return ret2;
    // }
    //
    // private static Map<String, MeasurementAggregator2> compute_Listing_11() throws IOException {
    // Map<String, MeasurementAggregator2> ret = new ConcurrentHashMap<>(10001);
    // new ReadFromDisk().compute(FILE, m -> {
    // ret.compute(m.station(), (k, v) -> {
    // if (v == null) {
    // v = new MeasurementAggregator2();
    // }
    // v.count++;
    // v.min = Math.min(v.min, m.value);
    // v.max = Math.max(v.max, m.value);
    // v.sum += m.value;
    // return v;
    // });
    // });
    // // long startTimeMs = System.currentTimeMillis();
    // Map<String, MeasurementAggregator2> ret2 = new TreeMap<>(ret);
    // // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
    // return ret2;
    // }
    //
    // private static Map<String, MeasurementAggregator2> compute_Listing_12() throws IOException {
    // Map<String, MeasurementAggregator2> ret = new ConcurrentHashMap<>(10001);
    // new ReadAfterMemoryMapping().compute(FILE, m -> {
    // ret.compute(m.station(), (k, v) -> {
    // if (v == null) {
    // v = new MeasurementAggregator2();
    // }
    // v.count++;
    // v.min = Math.min(v.min, m.value);
    // v.max = Math.max(v.max, m.value);
    // v.sum += m.value;
    // return v;
    // });
    // });
    // // long startTimeMs = System.currentTimeMillis();
    // Map<String, MeasurementAggregator2> ret2 = new TreeMap<>(ret);
    // // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
    // return ret2;
    // }

    @SuppressWarnings("unchecked")
    private static Map<String, MeasurementAggregator> compute_Listing_13() throws Exception {
        // estimateBytes();
        int nThreads = Integer.parseInt(System.getProperty("threads",
                "" + Runtime.getRuntime().availableProcessors()));
        System.err.printf("Running program with %d threads %n", nThreads);
        AggregateMemoryMappedFile job = new AggregateMemoryMappedFile(nThreads - 1);
        job.compute(FILE);
        job.merge();
        return job.sort();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, MeasurementAggregator> compute_Listing_15() throws Exception {
        // estimateBytes();
        int nThreads = Integer.parseInt(System.getProperty("threads",
                "" + Runtime.getRuntime().availableProcessors()));
        System.err.printf("Running program with %d threads %n", nThreads);
        AggregateMemoryMappedFile2 job = new AggregateMemoryMappedFile2(nThreads - 1);
        job.compute(FILE);
        job.merge();
        return job.sort();
    }

    @SuppressWarnings("unchecked")
    // private static Map<String, MeasurementAggregator2> compute_Listing_14() throws IOException {
    // int nThreads = Runtime.getRuntime().availableProcessors();
    // long time1 = System.nanoTime();
    // State[] threadLocalStates = new State[(nThreads << 4)];
    // IntStream.range(0, nThreads * 8)
    // .forEach(i -> threadLocalStates[i] = new State());
    // System.out.println("Init took " + (System.nanoTime() - time1) + " ns");
    // // estimateBytes();
    // long time2 = System.nanoTime();
    // int segmentSize = Integer.parseInt(System.getProperty("segment.size", "83,88,608"));
    // AggregateMemoryMappedFile2 job = new AggregateMemoryMappedFile2(segmentSize);
    // job
    // .compute(
    // FILE,
    // m -> (threadLocalStates[(int) m.tid]).update(m));
    // System.out.println("Aggregate took " + (System.nanoTime() - time2) + " ns");
    // Map<String, MeasurementAggregator2> ret = new ConcurrentHashMap<>(10010);
    // long time3 = System.nanoTime();
    // Arrays.stream(threadLocalStates)
    // .parallel()
    // .filter(Objects::nonNull)
    // .forEach(threadLocalState -> threadLocalState.state
    // .forEach((k, v) -> {
    // ret.compute(k, (k1, v1) -> {
    // if (v1 == null) {
    // v1 = new MeasurementAggregator2();
    // }
    // v1.merge(v);
    // return v1;
    // });
    // }));
    // // long startTimeMs = System.currentTimeMillis();
    // // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
    // Map<String, MeasurementAggregator2> ret2 = new TreeMap<>(ret);
    // System.out.println("Merge took " + (System.nanoTime() - time3) + " ns");
    // return ret2;
    // }

    // private static void estimateBytes() throws IOException {
    // long time = System.nanoTime();
    // long bytesCount = new OptimizedFileReader2().readLines(FILE)
    // .parallel()
    // .mapToLong(String::length)
    // .sum();
    // System.out.println("Estimate bytes result = " + bytesCount + " took " + (System.nanoTime() - time) + " ns");
    // }

    public static class State {
        // private final Map<StateKey, MeasurementAggregator> state;
        private final FastHashMap state;

        public State() {
            // this.state = new HashMap<>(10010);
            this.state = new FastHashMap(10010);
        }

        // implementing the logic in update method instead of calling HashMap.compute() has reduced the runtime significantly
        // primarily because it causes update method to be inlined by the compiler into the preceding loop
        public void update(final Measurement m) {
            MeasurementAggregator agg = state.get(m.stateKey);
            if (agg == null) {
                state.put(m.stateKey, new MeasurementAggregator(m.value, m.value, m.value, 1L));
                return;
            }
            agg.count++;
            agg.min = m.value <= agg.min ? m.value : agg.min;
            agg.max = m.value >= agg.max ? m.value : agg.max;
            agg.sum += m.value;
        }

        public static class StateKey {
            private final byte[] station;
            private final int stationLen;
            private final boolean isAscii;
            private final int hashCode;

            public StateKey(final byte[] station,
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
                if (!(other instanceof StateKey)) {
                    return false;
                }
                StateKey sk = (StateKey) other;
                // return stationLen == sk.stationLen
                // && checkArrayEquals(station, sk.station, stationLen);
                return stationLen == sk.stationLen
                        && Arrays.equals(station, 0, stationLen, sk.station, 0, stationLen);
            }

            public boolean checkArrayEquals(final byte[] a1,
                                            final byte[] a2,
                                            final int len) {
                if (a1[0] != a2[0]) {
                    return false;
                }
                if (len < 8) {
                    int i = 1;
                    for (; i < len; i++) {
                        if (a1[i] != a2[i]) {
                            return false;
                        }
                    }
                }
                else {
                    int i = 1;
                    int loopLength = SPECIES.length();
                    int loopBound = SPECIES.loopBound(len);
                    for (; i < loopBound; i += loopLength) {
                        Vector<Byte> b1 = ByteVector.fromArray(SPECIES, a1, i);
                        Vector<Byte> b2 = ByteVector.fromArray(SPECIES, a2, i);
                        VectorMask<Byte> result = b1.eq(b2);
                        if (!result.allTrue()) {
                            return false;
                        }
                    }
                    // for (; i < len - 8; i += 8) {
                    // long a = bytesToLong(a1, i);
                    // long b = bytesToLong(a2, i);
                    // if ((a ^ b) > 0) {
                    // return false;
                    // }
                    // }
                    for (; i < len; i++) {
                        if (a1[i] != a2[i]) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }
    }

    // public static class State2 {
    // private Map<Measurement2, MeasurementAggregator2> state;
    //
    // public State2() {
    // this.state = new HashMap<>(10010);
    // }
    //
    // public void update(final Measurement2 m) {
    // this.state.compute(m, (k, v) -> {
    // if (v == null) {
    // v = new MeasurementAggregator2();
    // }
    // v.count++;
    // v.min = Math.min(v.min, m.value);
    // v.max = Math.max(v.max, m.value);
    // v.sum += m.value;
    // return v;
    // });
    // }
    // }

    // private static String[] splitByChar(final String s,
    // final char c) {
    // String[] ret = new String[2];
    // int j = 0;
    // int k = 0;
    // char[] ch1 = new char[128];
    // char[] ch2 = new char[8];
    // for (char d : s.toCharArray()) {
    // if (d != c) {
    // ch1[k++] = d;
    // }
    // else {
    // ret[j++] = new String(ch1, 0, k);
    // ch1 = ch2;
    // k = 0;
    // }
    // }
    // ret[j] = new String(ch1, 0, k);
    // return ret;
    // }

    // private static String[] splitByChar2(final String s,
    // final char c) {
    // try {
    // String[] ret = new String[2];
    // int idx = indexOf(s, c);
    // ret[0] = s.substring(0, idx);
    // ret[1] = s.substring(idx + 1);
    // return ret;
    // }
    // catch (StringIndexOutOfBoundsException e) {
    // throw new RuntimeException(e);
    // }
    // }

    // private static String[] splitByChar3(final String s,
    // final int splitIdx) {
    // if (splitIdx < 0) {
    // return splitByChar2(s, ';');
    // }
    // try {
    // String[] ret = new String[2];
    // ret[0] = s.substring(0, splitIdx);
    // ret[1] = s.substring(splitIdx + 1);
    // return ret;
    // }
    // catch (StringIndexOutOfBoundsException e) {
    // throw new RuntimeException(e);
    // }
    // }
    //
    // private static int indexOf(final String s,
    // final char c) {
    // int len = s.length();
    // if (s.charAt(len - 4) == c) {
    // return len - 4;
    // }
    // if (s.charAt(len - 5) == c) {
    // return len - 5;
    // }
    // return len - 6;
    // }

    private static int toDigit(final char c) {
        return DIGIT_LOOKUP[(int) c];
    }

    private static int fastMul10(final int i) {
        return (i << 1) + (i << 3);
    }

    // private static double parseDouble2(final String s) {
    // try {
    // int len = s.length();
    // char ch0 = s.charAt(0);
    // char ch1 = s.charAt(1);
    // char ch2 = s.charAt(2);
    // char ch3 = len > 3 ? s.charAt(3) : ' ';
    // char ch4 = len > 4 ? s.charAt(4) : ' ';
    // if (len == 3) {
    // int decimal = toDigit(ch0);
    // double fractional = lookup[toDigit(ch2)];
    // return decimal + fractional;
    // }
    // else if (len == 4) {
    // // -1.2 or 11.2
    // int decimal = (ch0 == '-' ? toDigit(ch1) : (fastMul10(toDigit(ch0)) + toDigit(ch1)));
    // double fractional = lookup[toDigit(ch3)];
    // if (ch0 == '-') {
    // return Math.negateExact(decimal) - fractional;
    // }
    // else {
    // return decimal + fractional;
    // }
    // }
    // else {
    // int decimal = fastMul10(toDigit(ch1)) + toDigit(ch2);
    // double fractional = lookup[toDigit(ch4)];
    // return Math.negateExact(decimal) - fractional;
    // }
    // }
    // catch (ArrayIndexOutOfBoundsException e) {
    // System.out.printf("Array index out of bounds for string: %s%n", s);
    // throw new RuntimeException(e);
    // }
    // catch (StringIndexOutOfBoundsException e) {
    // System.out.printf("String index out of bounds for string: %s%n", s);
    // throw new RuntimeException(e);
    // }
    // }

    private static double parseDouble2(final byte[] b, final int len) {
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
            System.out.printf("Array index out of bounds for string: %s%n", new String(b, 0, len));
            throw new RuntimeException(e);
        }
        catch (StringIndexOutOfBoundsException e) {
            System.out.printf("String index out of bounds for string: %s%n", new String(b, 0, len));
            throw new RuntimeException(e);
        }
    }

    // @FunctionalInterface
    // public interface FileReader {
    // Stream<String> readLines(String path);
    // }
    //
    // public static class BaselineFileReader {
    // public Stream<String> readLines(final String path) throws IOException {
    // return Files.lines(Paths.get(path));
    // }
    // }
    //
    // public static class OptimizedFileReader {
    // public Stream<String> readLines(final String path) throws IOException {
    // return Files.lines(Paths.get(path)).parallel();
    // }
    // }
    //
    // public static class OptimizedFileReader2 {
    // public Stream<String> readLines(final String path) throws IOException {
    // return newBufferedReader(Paths.get(path), UTF_8, 32768).lines().parallel();
    // }
    //
    // private static BufferedReader newBufferedReader(final Path path,
    // final Charset cs,
    // final int bufferSize)
    // throws IOException {
    // CharsetDecoder decoder = cs.newDecoder();
    // Reader reader = new InputStreamReader(Files.newInputStream(path), decoder);
    // return new BufferedReader(reader, bufferSize);
    // }
    // }

    public static class AggregateMemoryMappedFile {
        private final int nThreads;
        private final State[] threadLocalStates;
        private final Map<State.StateKey, MeasurementAggregator> ret = new ConcurrentHashMap<>(10010);
        private final ExecutorService executorService;
        private final int pageSize = 4096;

        public AggregateMemoryMappedFile(final int nThreads) {
            this.threadLocalStates = new State[(nThreads << 4)];
            IntStream.range(0, nThreads << 4)
                    .forEach(i -> threadLocalStates[i] = new State());
            this.nThreads = nThreads;
            this.executorService = Executors.newFixedThreadPool(nThreads);
        }

        public void compute(final String path) throws Exception {
            try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
                MemorySegment memorySegment = file.getChannel().map(
                        FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
                long fileLength = file.length();
                int splitLength = (int) Math.min(Integer.MAX_VALUE, Math.rint(fileLength * 1.0 / nThreads));
                int capacity = (int) (splitLength > file.length() ? 1 : (fileLength / splitLength + 1));
                System.err.printf("fileLength = %d, splitLength = %d%n", file.length(), splitLength);
                List<Split> splits = new ArrayList<>(capacity);
                long time1 = System.nanoTime();
                // iterate over the memory mapped file
                for (long i = 0; i < file.length();) {
                    long startOffset = i;
                    long endOffset = Math.min(file.length(), startOffset + splitLength); // not inclusive
                    if (endOffset == file.length()) {
                        splits.add(new Split(startOffset, endOffset - startOffset));
                        break;
                    }
                    // look past the end offset
                    long segmentLength = Math.min(MAX_STR_LEN, file.length() - i);
                    MemorySegment lookahead = memorySegment.asSlice(
                            endOffset,
                            segmentLength);
                    ByteBuffer bb = lookahead.asByteBuffer();
                    // find next new line char and next offset
                    LineMetadata lineMetadata = findNewLine(bb, (int) segmentLength, 0);
                    endOffset += lineMetadata.offset;
                    splits.add(new Split(startOffset, endOffset - startOffset));
                    i = endOffset;
                    lookahead.unload();
                }
                System.err.printf("Number of splits = %d, splits = [%s]%n", splits.size(), splits);
                verifySplits(file.getChannel(), splits);
                System.err.printf("Splits calculation took %d ns%n", System.nanoTime() - time1);
                // consume splits in parallel using the common fork join pool

                long time = System.nanoTime();
                List<Future<?>> futures = new ArrayList<>(splits.size() * 2);
                splits
                        .forEach(split -> {
                            futures.add(executorService.submit(() -> {
                                MemorySegment subSegment = memorySegment.asSlice(split.offset, split.length);
                                // this byte buffer should end with '\n' or EOF
                                subSegment.load();
                                ByteBuffer bb = subSegment.asByteBuffer();
                                int bytesRead = 0;
                                int bytesToRead = (int) split.length;
                                int tid = (int) Thread.currentThread().threadId();
                                byte[] currentPage = new byte[pageSize];
                                bb.get(0, currentPage, 0, Math.min(pageSize, (int) split.length));
                                SearchResult searchResult = new SearchResult();
                                while (bytesRead < bytesToRead) {
                                    int len = searchForNewLine(currentPage, searchResult);
                                    bytesRead += len;
                                    if (!searchResult.found) {
                                        // need to read next page
                                        bb.position(bytesRead);
                                        bb.get(bytesRead, currentPage, 0,
                                                Math.min(pageSize, (int) (split.length - bytesRead)));
                                        searchResult.position = 0;
                                        continue;
                                    }
                                    Measurement m = new Measurement(
                                            searchResult.station,
                                            searchResult.stationLen,
                                            searchResult.temperature,
                                            searchResult.temperatureLen,
                                            searchResult.isAscii,
                                            searchResult.hashCode,
                                            tid);
                                    threadLocalStates[tid].update(m);
                                    int prevPos = searchResult.position;
                                    searchResult = new SearchResult();
                                    searchResult.position = prevPos;
                                }
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
            // long startTimeMs = System.currentTimeMillis();
            // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
            System.err.println("Merge took " + (System.nanoTime() - time) + " ns");
        }

        public Map<String, MeasurementAggregator> sort() {
            long time = System.nanoTime();
            Map<String, MeasurementAggregator> ret2 = new TreeMap<>();
            ret.forEach((k, v) -> ret2.put(k.toString(), v));
            System.err.println("Tree map construction took " + (System.nanoTime() - time) + " ns");
            return ret2;
        }
    }

    public static class AggregateMemoryMappedFile2 {
        private final int nThreads;
        private final State[] threadLocalStates;
        private final Map<State.StateKey, MeasurementAggregator> ret = new ConcurrentHashMap<>(10010);
        private final ExecutorService executorService;
        private final int pageSize = 8_388_608; // 8 MB

        public AggregateMemoryMappedFile2(final int nThreads) {
            this.threadLocalStates = new State[(nThreads << 4)];
            IntStream.range(0, nThreads << 4)
                    .forEach(i -> threadLocalStates[i] = new State());
            this.nThreads = nThreads;
            this.executorService = Executors.newFixedThreadPool(nThreads);
        }

        public void compute(final String path) throws Exception {
            try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
                MemorySegment memorySegment = file.getChannel().map(
                        FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
                long fileLength = file.length();
                int splitLength = (int) Math.min(Integer.MAX_VALUE, Math.rint(fileLength * 1.0 / nThreads));
                System.err.printf("FileLength = %d, SplitLength = %d%n", file.length(), splitLength);
                long time1 = System.nanoTime();
                // iterate over the memory mapped file
                List<Split2> splits = breakFileIntoSplits(file, splitLength, pageSize, memorySegment);
                System.err.printf("Number of splits = %d, splits = [%s]%n", splits.size(), splits);
                // verifySplits2(file.getChannel(), splits);
                System.err.printf("Splits calculation took %d ns%n", System.nanoTime() - time1);
                // consume splits in parallel using the common fork join pool

                long time = System.nanoTime();
                List<Future<?>> futures = new ArrayList<>(splits.size() * 2);
                splits
                        .forEach(split -> {
                            futures.add(executorService.submit(() -> {
                                MemorySegment subSegment = memorySegment.asSlice(split.offset, split.length);
                                // this byte buffer should end with '\n' or EOF
                                subSegment.load();
                                ByteBuffer bb = subSegment.asByteBuffer();
                                int tid = (int) Thread.currentThread().threadId();
                                byte[] currentPage = new byte[pageSize + MAX_STR_LEN];
                                int bytesRead = 0;
                                for (Page page : split.pages) {
                                    bb.position(bytesRead);
                                    bb.get(bytesRead, currentPage, 0, (int) page.length);
                                    SearchResult2 searchResult = findNewLinesVectorized(currentPage, (int) page.length);
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
                                        System.arraycopy(currentPage, curOffset - temperatureLen, temperature, 0, temperatureLen);
                                        System.arraycopy(currentPage, prevOffset, station, 0, stationLen);
                                        int hashCode = Arrays.hashCode(station);
                                        Measurement m = new Measurement(
                                                station, stationLen, temperature, temperatureLen, false, hashCode, tid);
                                        threadLocalStates[tid].update(m);
                                        prevOffset = curOffset + 1;
                                        j++;
                                    }
                                    bytesRead += (int) page.length;
                                }
                                // subSegment.unload();
                            }));
                        });
                for (Future<?> future : futures) {
                    future.get();
                }
                System.err.println("Aggregate took " + (System.nanoTime() - time) + " ns");
            }
        }

        private static List<Split2> breakFileIntoSplits(final RandomAccessFile file,
                                                        final int splitLength,
                                                        final int pageLength,
                                                        final MemorySegment memorySegment)
                throws IOException {
            final List<Split2> splits = new ArrayList<>();
            for (long i = 0; i < file.length();) {
                long splitStartOffset = i;
                long splitEndOffset = Math.min(file.length(), splitStartOffset + splitLength); // not inclusive
                if (splitEndOffset == file.length()) { // reached EOF
                    List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment);
                    splits.add(new Split2(splitStartOffset, splitEndOffset - splitStartOffset, pages));
                    break;
                }
                // look past the end offset
                long segmentLength = Math.min(MAX_STR_LEN, file.length() - i);
                MemorySegment lookahead = memorySegment.asSlice(splitEndOffset, segmentLength);
                ByteBuffer bb = lookahead.asByteBuffer();
                // find next new line char and next offset
                LineMetadata lineMetadata = findNewLine(bb, (int) segmentLength, 0);
                splitEndOffset += lineMetadata.offset;
                // if (memorySegment.asSlice(splitEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                // throw new IllegalStateException("Page doesn't end with NL char");
                // }
                List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment);
                splits.add(new Split2(splitStartOffset, splitEndOffset - splitStartOffset, pages));
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
                // look past the end offset
                long lookaheadLength = Math.min(MAX_STR_LEN, splitEndOffset - i);
                MemorySegment lookahead = memorySegment.asSlice(pageEndOffset, lookaheadLength);
                ByteBuffer bb = lookahead.asByteBuffer();
                // find next new line char and next offset
                LineMetadata lineMetadata = findNewLine(bb, (int) lookaheadLength, 0);
                pageEndOffset += lineMetadata.offset;
                // if (memorySegment.asSlice(pageEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                // throw new IllegalStateException("Page doesn't end with NL char");
                // }
                pages.add(new Page(pageStartOffset, pageEndOffset - pageStartOffset));
                i = pageEndOffset;
                lookahead.unload();
            }
            return pages;
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
            // long startTimeMs = System.currentTimeMillis();
            // System.out.printf("Construction of tree map took = %d ms%n", System.currentTimeMillis() - startTimeMs);
            System.err.println("Merge took " + (System.nanoTime() - time) + " ns");
        }

        public Map<String, MeasurementAggregator> sort() {
            long time = System.nanoTime();
            Map<String, MeasurementAggregator> ret2 = new TreeMap<>();
            ret.forEach((k, v) -> ret2.put(k.toString(), v));
            System.err.println("Tree map construction took " + (System.nanoTime() - time) + " ns");
            return ret2;
        }
    }

    // public static class AggregateMemoryMappedFile2 {
    // private final int splitLength;
    //
    // public AggregateMemoryMappedFile2(final int splitLength) {
    // this.splitLength = splitLength;
    // }
    //
    // public void compute(final String path,
    // final Consumer<Measurement> consumer)
    // throws IOException {
    // try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
    // MemorySegment memorySegment = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
    // long fileLength = file.length();
    // int capacity = (int) (splitLength > file.length() ? 1 : (fileLength / splitLength + 1));
    // System.out.printf("File length = %d split length = %d%n", file.length(), splitLength);
    // List<Split> splits = new ArrayList<>(capacity);
    // long time1 = System.nanoTime();
    // // iterate over the memory mapped file
    // for (long i = 0; i < file.length();) {
    // long startOffset = i;
    // long endOffset = Math.min(file.length(), startOffset + splitLength); // not inclusive
    // if (endOffset == file.length()) {
    // splits.add(new Split(startOffset, endOffset - startOffset));
    // break;
    // }
    // // look past the end offset
    // long segmentLength = Math.min(512, file.length() - i);
    // MemorySegment lookahead = memorySegment.asSlice(
    // endOffset,
    // segmentLength);
    // ByteBuffer bb = lookahead.asByteBuffer();
    // // find next new line char and next offset
    // LineMetadata lineMetadata = findNewLine(bb, (int) segmentLength, 0);
    // endOffset += lineMetadata.bbOffset;
    // splits.add(new Split(startOffset, endOffset - startOffset));
    // i = endOffset;
    // lookahead.unload();
    // }
    // System.out.printf("Splits = [%s]%n", splits);
    // verifySplits(file.getChannel(), splits);
    // System.out.printf("Splits calculation took %d ns%n", System.nanoTime() - time1);
    // // consume splits in parallel using the common fork join pool
    // splits
    // .stream()
    // .parallel()
    // .forEach(split -> {
    // MemorySegment subSegment = memorySegment.asSlice(split.offset, split.length);
    // long tid = Thread.currentThread().threadId();
    // findNewLinesVectorized(subSegment, (int) split.length, 1024)
    // .forEachRemaining(line -> {
    // Measurement m = new Measurement(splitByChar2(line, ';'), tid);
    // consumer.accept(m);
    // });
    // subSegment.unload();
    // });
    // }
    // }
    // }

    private static LineMetadata findNewLine(final ByteBuffer buffer,
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

    public static class SearchResult {
        byte[] station;
        byte[] temperature;
        int stationLen = 0;
        int temperatureLen = 0;
        int hashCode = 0;
        boolean isAscii = true;
        boolean afterDelim = false;
        int position = 0;
        boolean found = false;

        public SearchResult() {
            this.station = new byte[MAX_STR_LEN];
            this.temperature = new byte[5];
        }

        public void reset() {
            this.stationLen = 0;
            this.temperatureLen = 0;
            this.hashCode = 0;
            this.found = false;
            this.afterDelim = false;
            this.isAscii = true;
        }
    }

    private static int searchForNewLine(final byte[] page,
                                        final SearchResult searchResult) {
        int bytesRead = 0;
        int negativeBytes = 0;
        if (!searchResult.afterDelim) {
            for (; searchResult.position < page.length; searchResult.position++) {
                byte b = page[searchResult.position];
                bytesRead++;
                negativeBytes += ((b >> 31) & 1);
                if (b == ';') {
                    searchResult.position++;
                    searchResult.afterDelim = true;
                    break;
                }
                searchResult.hashCode = searchResult.hashCode * 31 + b;
                searchResult.station[searchResult.stationLen++] = b;
            }
        }
        if (negativeBytes > 0) {
            searchResult.isAscii = false;
        }
        for (; searchResult.position < page.length; searchResult.position++) {
            byte b = page[searchResult.position];
            bytesRead++;
            if (b == '\n') {
                searchResult.position++;
                searchResult.found = true;
                return bytesRead;
            }
            searchResult.temperature[searchResult.temperatureLen++] = b;
        }
        return bytesRead;
    }

    // private static List<Integer> vectorizedSearchForNewLine(final byte[] page) {
    // int bytesRead = 0;
    // int negativeBytes = 0;
    // int loopLength = SPECIES.length();
    // int loopBound = SPECIES.loopBound(page.length);
    // int newLinePos = -1;
    // Vector<Byte> zeroVec = SPECIES.broadcast(0);
    // Vector<Byte> newLineVec = SPECIES.broadcast('\n');
    // for (; searchResult.position < loopBound; searchResult.position += loopLength) {
    // Vector<Byte> byteVec =
    // ByteVector.fromArray(SPECIES, page, searchResult.position);
    // negativeBytes += byteVec.lt(zeroVec).trueCount();
    // int semiColonPos = byteVec.eq(newLineVec).firstTrue();
    // if (semiColonPos < loopLength) {
    // searchResult.position += semiColonPos;
    // searchResult.found = true;
    // bytesRead += semiColonPos;
    // break;
    // }
    // bytesRead += loopLength;
    // }
    // for (; searchResult.position < page.length; searchResult.position++) {
    // if (page[searchResult.position] == '\n') {
    // newLinePos = searchResult.position;
    // }
    // }
    // if (negativeBytes > 0) {
    // searchResult.isAscii = false;
    // }
    //
    // for (; searchResult.position < page.length; searchResult.position++) {
    // byte b = page[searchResult.position];
    // bytesRead++;
    // if (b == '\n') {
    // searchResult.position++;
    // searchResult.found = true;
    // return bytesRead;
    // }
    // searchResult.temperature[searchResult.temperatureLen++] = b;
    // }
    // return bytesRead;
    // }

    // private static Iterator<String> findNewLinesVectorized(final MemorySegment segment,
    // final int segmentLength,
    // final int batchSize) {
    // return new VectorizedLineReader(segment, segmentLength, batchSize);
    // }

    // public static class VectorizedLineReader implements Iterator<String> {
    // private MemorySegment segment;
    // private int segmentLength;
    // private int loopBound;
    // private int batchSize;
    // private Vector<Byte> searchVector;
    // private VectorSpecies<Byte> species;
    //
    // private int offset = 0;
    // private int lastLane = 0;
    // private List<String> lines = Collections.emptyList();
    // private int idx = 0;
    //
    // public VectorizedLineReader(final MemorySegment segment,
    // final int segmentLength,
    // final int batchSize) {
    // this.segment = segment;
    // this.segmentLength = segmentLength;
    // this.batchSize = batchSize;
    // this.species = ByteVector.SPECIES_PREFERRED;
    // this.loopBound = segmentLength;
    // this.searchVector = ByteVector.broadcast(species, (byte) '\n');
    // }
    //
    // @Override
    // public boolean hasNext() {
    // if (lines != null
    // && !lines.isEmpty()
    // && idx < lines.size()) {
    // return true;
    // }
    // return refill();
    // }
    //
    // @Override
    // public String next() {
    // if (!hasNext()) {
    // throw new NoSuchElementException();
    // }
    // if (idx < lines.size()) {
    // return lines.get(idx++);
    // }
    // else {
    // throw new IllegalStateException();
    // }
    // }
    //
    // private boolean refill() {
    // List<String> nextLines = new ArrayList<>(batchSize);
    // idx = 0;
    // int loopLength = switch (species.length()) {
    // case 8 -> 7;
    // case 16 -> 3;
    // case 32 -> 1;
    // case 64 -> 1;
    // default -> 1;
    // };
    // while (nextLines.size() < batchSize &&
    // offset < (loopBound - (loopLength * species.length()))) {
    // int offsetCopy = offset;
    // long maskAsLong = 0;
    // for (int i = 0; i < loopLength; i++) {
    // ByteVector bv = ByteVector.fromMemorySegment(species, segment, offsetCopy, ByteOrder.nativeOrder());
    // VectorMask<Byte> mask1 = bv.eq(searchVector);
    // offsetCopy += species.length();
    // maskAsLong |= (mask1.toLong() << ((long) i * species.length()));
    //
    // }
    //
    // int curLane = offset;
    // while (maskAsLong > 0) {
    // int cnt = Long.numberOfTrailingZeros(maskAsLong);
    // curLane += cnt;
    // MemorySegment lineSegment = segment.asSlice(lastLane, curLane - lastLane);
    // nextLines.add(new String(lineSegment.toArray(ValueLayout.JAVA_BYTE), UTF_8));
    // maskAsLong = maskAsLong >> (cnt + 1);
    // curLane++;
    // lastLane = curLane;
    // // ineSegment.unload();
    // }
    // offset += (loopLength * species.length());
    // }
    // while (nextLines.size() < batchSize &&
    // offset < segmentLength) {
    // if (segment.getAtIndex(ValueLayout.OfByte.JAVA_BYTE, offset) == '\n') {
    // MemorySegment lineSegment = segment.asSlice(lastLane, offset - lastLane);
    // nextLines.add(new String(lineSegment.toArray(ValueLayout.JAVA_BYTE), UTF_8));
    // lastLane = offset + 1;
    // // lineSegment.unload();
    // }
    // offset++;
    // }
    // lines = nextLines;
    // return !lines.isEmpty();
    // }
    // }

    record LineMetadata(byte[] station, byte[] temperature, int stationLen, int temperatureLen, int offset, int precomputedHashCode, boolean isAscii) {
    }

    // record LineMetadata(String line, int splitPos, int bbOffset, int hash) {
    // }

    // public static class ReadAfterMemoryMapping {
    // private ExecutorService executorService;
    // private int threads;
    //
    // public ReadAfterMemoryMapping() {
    // this.threads = Runtime.getRuntime().availableProcessors() * 2;
    // this.executorService = Executors.newFixedThreadPool(threads);
    // }
    //
    // public void compute(final String path,
    // final Consumer<Measurement> consumer)
    // throws IOException {
    // List<Future<Void>> futures = new ArrayList<>();
    // try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
    // List<Split> splits = calculateSplits(file.getChannel(), threads);
    // int splitsPerThread = (int) Math.ceil(splits.size() * 1.0 / threads);
    // for (int i = 0; i < threads; i++) {
    // final int iFinal = i;
    // futures.add(executorService.submit(() -> {
    // for (int j = 0; j < splitsPerThread; j++) {
    // try {
    // int splitIdx = iFinal + j;
    // if (splitIdx < splits.size()) {
    // Split split = splits.get(splitIdx);
    // MemorySegment buf = file.getChannel().map(FileChannel.MapMode.READ_ONLY, split.offset, split.length, Arena.global());
    // int k = 0;
    // for (int l = 0; l < split.length; l++) {
    // if (buf.getAtIndex(ValueLayout.JAVA_BYTE, l) == '\n') {
    // String s = UTF_8.decode(buf.asSlice(k, l - k).asByteBuffer()).toString();
    // consumer.accept(
    // new Measurement(splitByChar2(
    // s,
    // ';')));
    // k = l + 1;
    // }
    // }
    // buf.unload();
    // System.out.printf("Finished processing split: %d. %n", splitIdx);
    // }
    // }
    // catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    // return null;
    // }));
    // }
    // for (Future<Void> future : futures) {
    // try {
    // future.get();
    // }
    // catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    // }
    // }
    // }

    // NOTE: Doesn't work as expected, contains some bugs which cause ArrayIndexOutOfBoundsException to be thrown
    // Also, it is slower than other listings
    // public static class ReadFromDisk {
    // private ExecutorService executorService;
    // private int threads;
    //
    // public ReadFromDisk() {
    // this.threads = Runtime.getRuntime().availableProcessors() * 2;
    // // this.threads = 1;
    // this.executorService = Executors.newFixedThreadPool(threads);
    // }
    //
    // public void compute(final String path,
    // final Consumer<Measurement> consumer)
    // throws IOException {
    // FileChannel fc = FileChannel.open(Paths.get(path));
    // List<Split> splitPos = calculateSplits(fc, threads);
    // System.out.printf("Number of splits = %d%n", splitPos.size());
    // List<Future<Void>> futures = new ArrayList<>();
    // int splitsPerThread = (int) Math.ceil(splitPos.size() * 1.0 / threads);
    // System.out.printf("Splits per thread = %d%n", splitsPerThread);
    // for (int i = 0; i < splitPos.size(); i += splitsPerThread) {
    // final int iFinal = i;
    // Future<Void> future = executorService.submit(() -> {
    // for (int j = 0; j < splitsPerThread; j++) {
    // try {
    // if (iFinal + j < splitPos.size()) {
    // readSplit(FileChannel.open(Paths.get(path)), splitPos.get(iFinal + j)).forEachRemaining(consumer);
    // System.out.printf("Finished processing split: %d. [i = %d, j = %d]%n", iFinal + j, iFinal, j);
    // }
    // }
    // catch (IOException e) {
    // throw new RuntimeException(e);
    // }
    // }
    // return null;
    // });
    // futures.add(future);
    // }
    //
    // for (Future<Void> future : futures) {
    // try {
    // future.get();
    // }
    // catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    // }
    //
    // private static Iterator<Measurement> readSplit(final FileChannel fc,
    // final Split split)
    // throws IOException {
    // fc.position(split.offset);
    // return new Iterator<>() {
    // private BufferedReader br = null;
    // private String nextLine = null;
    // private boolean closed = false;
    //
    // @Override
    // public boolean hasNext() {
    // try {
    // if (nextLine != null) {
    // return true;
    // }
    // if (closed) {
    // return false;
    // }
    // if (br == null) {
    // return fillBuffer();
    // }
    // nextLine = br.readLine();
    // if (nextLine == null) {
    // closed = true;
    // return false;
    // }
    // return true;
    // }
    // catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    //
    // @Override
    // public Measurement next() {
    // if (!hasNext()) {
    // throw new NoSuchElementException();
    // }
    // Measurement m = new Measurement(splitByChar2(nextLine, ';'));
    // nextLine = null;
    // // System.out.println(m);
    // return m;
    // }
    //
    // private boolean fillBuffer() throws IOException {
    // br = new BufferedReader(
    // Channels.newReader(
    // getReadableByteChannel(fc, split), UTF_8.newDecoder(), 134_217_728),
    // 32768);
    // // System.out.printf("Char buffer length = %d%n", charBuffer.length());
    // return true;
    // }
    //
    // private ReadableByteChannel getReadableByteChannel(final FileChannel fc,
    // final Split split) {
    // return new CustomReadableByteChannel(fc, split.offset, (split.offset + split.length));
    // }
    // };
    // }
    //
    // public static class CustomReadableByteChannel implements ReadableByteChannel {
    // private FileChannel fc;
    // private long index;
    // private long fence;
    //
    // public CustomReadableByteChannel(final FileChannel fc,
    // final long index,
    // final long fence) {
    // this.fc = fc;
    // this.index = index;
    // this.fence = fence;
    // }
    //
    // @Override
    // public int read(ByteBuffer dst) throws IOException {
    // long bytesToRead = fence - index;
    // if (bytesToRead == 0)
    // return -1;
    //
    // int bytesRead;
    // if (bytesToRead < dst.remaining()) {
    // // The number of bytes to read is less than remaining
    // // bytes in the buffer
    // // Snapshot the limit, reduce it, read, then restore
    // int oldLimit = dst.limit();
    // dst.limit((int) (dst.position() + bytesToRead));
    // bytesRead = fc.read(dst, index);
    // dst.limit(oldLimit);
    // }
    // else {
    // bytesRead = fc.read(dst, index);
    // }
    // if (bytesRead == -1) {
    // index = fence;
    // return bytesRead;
    // }
    //
    // index += bytesRead;
    // return bytesRead;
    // }
    //
    // @Override
    // public boolean isOpen() {
    // return fc.isOpen();
    // }
    //
    // @Override
    // public void close() throws IOException {
    // fc.close();
    // }
    // }
    //
    // }

    // private static List<Split> calculateSplits(final FileChannel fc,
    // final int threads)
    // throws IOException {
    // List<Split> splitPos = new ArrayList<>();
    // long totalBytesInFile = fc.size();
    // long bytesPerSplit = Math.min(134_217_728, totalBytesInFile / threads); // 128 MiB
    // long curOffset = 0;
    // long nextOffset = curOffset + bytesPerSplit;
    // System.out.printf("Total bytes in file: %d, bytes per split: %d%n", totalBytesInFile, bytesPerSplit);
    // while (nextOffset < totalBytesInFile) {
    // // Seek to nextOffset
    // fc.position(nextOffset);
    // // Read next 128 bytes to find the next occurrence of \n
    // ByteBuffer bb = ByteBuffer.allocate(128);
    // // Fill the byte buffer
    // int bytesRead = fc.read(bb);
    // byte[] arr = bb.array();
    // // Construct a string out of the contents read in byte buffer
    // int idx = 0;
    // while (idx < arr.length && arr[idx] != '\n') {
    // idx++;
    // }
    // if (idx < arr.length) {
    // // Adjust nextOffset
    // nextOffset = nextOffset + idx + 1;
    // // Add new split [curOffset, curOffset + length), where length = nextOffset - curOffset
    // splitPos.add(new Split(curOffset, nextOffset - curOffset));
    // curOffset = nextOffset;
    // nextOffset = curOffset + bytesPerSplit;
    // }
    // else {
    // throw new IllegalStateException();
    // }
    // }
    // // Add final split if not done
    // if (curOffset < totalBytesInFile) {
    // splitPos.add(new Split(curOffset, totalBytesInFile - curOffset));
    // }
    // System.out.printf("Splits = [%s]%n", splitPos);
    // verifySplits(fc, splitPos);
    // return splitPos;
    // }

    private static void verifySplits(final FileChannel fc,
                                     final List<Split> splits)
            throws IOException {
        long curOffset = 0;
        for (Split split : splits) {
            if (curOffset + split.length >= fc.size()) {
                break;
            }
            fc.position(curOffset + split.length - 1);
            ByteBuffer bb = ByteBuffer.allocate(128);
            fc.read(bb);
            byte[] arr = bb.array();
            if (arr[0] != '\n') {
                System.out.printf("Found non-newline character: '%s'. String: %s%n", (char) arr[0], new String(arr, StandardCharsets.UTF_8));
                System.out.printf("Error in split generation: [currentSplit = %s, curOffset = %s]%n", split, curOffset);
                throw new IllegalStateException();
            }
            curOffset += split.length;
        }
        System.out.println("Splits verified successfully");
    }

    private static void verifySplits2(final FileChannel fc,
                                      final List<Split2> splits)
            throws IOException {
        for (Split2 split : splits) {
            if (split.offset + split.length >= fc.size()) {
                break;
            }
            checkCharAtOffset(fc, split, split.offset + split.length - 1);
            for (Page page : split.pages) {
                checkCharAtOffset(fc, split, page.offset + page.length - 1);
            }
        }
        System.out.println("Splits verified successfully");
    }

    private static void checkCharAtOffset(final FileChannel fc,
                                          final Split2 split,
                                          final long offset)
            throws IOException {
        fc.position(offset);
        ByteBuffer bb = ByteBuffer.allocate(MAX_STR_LEN);
        fc.read(bb);
        byte[] arr = bb.array();
        if (arr[0] != '\n') {
            System.err.printf("Found non-newline character: '%s'. String: %s%n", (char) arr[0], new String(arr, StandardCharsets.UTF_8));
            System.err.printf("Error in split generation: [split = %s, offset = %s]%n", split, offset);
            throw new IllegalStateException();
        }
    }

    record Split(long offset, long length) {
    }

    record Split2(long offset, long length, List<Page> pages) {
    }

    record Page(long offset, long length) {
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

        public void put(final State.StateKey key,
                        final MeasurementAggregator aggregator) {
            tableEntries[(size - 1) & key.hashCode] = new TableEntry(key, aggregator);
        }

        public MeasurementAggregator get(final State.StateKey key) {
            TableEntry entry = tableEntries[(size - 1) & key.hashCode];
            if (entry != null) {
                return entry.aggregator;
            }
            return null;
        }

        public void forEach(final BiConsumer<State.StateKey, MeasurementAggregator> action) {
            for (int i = 0; i < size; i++) {
                TableEntry entry = tableEntries[i];
                if (entry != null) {
                    action.accept(entry.key, entry.aggregator);
                }
            }
        }

        record TableEntry(State.StateKey key, MeasurementAggregator aggregator) {
        }
    }

    private static SearchResult2 findNewLinesVectorized(final byte[] page,
                                                        final int pageLen) {
        SearchResult2 ret = new SearchResult2(new int[pageLen / 10], 0);
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

    public static class SearchResult2 {
        private int[] offsets;
        private int len;

        public SearchResult2(final int[] offsets,
                             final int len) {
            this.offsets = offsets;
            this.len = len;
        }
    }
}
