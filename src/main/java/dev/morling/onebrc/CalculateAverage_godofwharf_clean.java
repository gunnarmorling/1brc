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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class CalculateAverage_godofwharf_clean {
    private static final String FILE = "./measurements.txt";

    private static final double[] lookup = new double[]{ 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };

    public record Measurement(byte[] station, int stationLen, double value, int precomputedHashCode, long tid) {
        public Measurement(byte[] station,
                           int stationLen,
                           byte[] temperature,
                           int tempLen,
                           int hashCode,
                           long tid) {
            this(station, stationLen, parseDouble2(temperature, tempLen), hashCode, tid);
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0.0;
        private long count = 0;

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
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

    public static void main(String[] args) throws IOException {
        long startTimeMs = System.currentTimeMillis();
        Map<String, MeasurementAggregator> measurements = compute1();
        long time1 = System.nanoTime();
        System.out.println(measurements);
        System.err.println("Print took " + (System.nanoTime() - time1) + " ns");
        System.err.printf("Took %d ms%n", System.currentTimeMillis() - startTimeMs);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, MeasurementAggregator> compute1() throws IOException {
        int nThreads = Runtime.getRuntime().availableProcessors();
        long time1 = System.nanoTime();
        State[] threadLocalStates = new State[(nThreads << 4)];
        IntStream.range(0, nThreads << 4)
                .forEach(i -> threadLocalStates[i] = new State());
        System.err.println("Init took " + (System.nanoTime() - time1) + " ns");
        // estimateBytes();
        long time2 = System.nanoTime();
        int segmentSize = Integer.parseInt(System.getProperty("segment.size", "83,88,608"));
        AggregateMemoryMappedFile job = new AggregateMemoryMappedFile(segmentSize);
        job
                .compute(
                        FILE,
                        m -> (threadLocalStates[(int) m.tid]).update(m));
        System.err.println("Aggregate took " + (System.nanoTime() - time2) + " ns");
        Map<State.StateKey, MeasurementAggregator> ret = new ConcurrentHashMap<>(10010);
        long time3 = System.nanoTime();
        Arrays.stream(threadLocalStates)
                .parallel()
                .filter(Objects::nonNull)
                .forEach(threadLocalState -> threadLocalState.state
                        .forEach((k, v) -> {
                            ret.compute(k, (k1, v1) -> {
                                if (v1 == null) {
                                    v1 = new MeasurementAggregator();
                                }
                                v1.merge(v);
                                return v1;
                            });
                        }));
        System.err.println("Merge took " + (System.nanoTime() - time3) + " ns");
        long time4 = System.nanoTime();
        Map<String, MeasurementAggregator> ret2 = new TreeMap<>();
        ret.forEach((k, v) -> ret2.put(k.toString(), v));
        System.err.println("Tree map construction took " + (System.nanoTime() - time4) + " ns");
        return ret2;
    }

    public static class State {
        private Map<StateKey, MeasurementAggregator> state;

        public State() {
            this.state = new HashMap<>(10010);
        }

        public void update(final Measurement m) {
            this.state.compute(new StateKey(m.station, m.stationLen, m.precomputedHashCode), (k, v) -> {
                if (v == null) {
                    v = new MeasurementAggregator();
                }
                v.count++;
                v.min = Math.min(v.min, m.value);
                v.max = Math.max(v.max, m.value);
                v.sum += m.value;
                return v;
            });
        }

        public static class StateKey {
            byte[] station;
            int stationLen;
            int hashCode;

            public StateKey(final byte[] station,
                            final int stationLen,
                            final int hashCode) {
                this.station = station;
                this.stationLen = stationLen;
                this.hashCode = hashCode;
            }

            @Override
            public String toString() {
                return new String(station, 0, stationLen);
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
                return stationLen == sk.stationLen &&
                        Arrays.equals(station, sk.station);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            state.forEach((k, v) -> sb.append(k + "=" + v));
            return sb.toString();
        }
    }

    private static int toDigit(final char c) {
        return c - 48;
    }

    private static int fastMul10(final int i) {
        return (i << 1) + (i << 3);
    }

    private static double parseDouble2(final byte[] b,
                                       final int len) {
        try {
            char ch0 = (char) b[0];
            char ch1 = (char) b[1];
            char ch2 = (char) b[2];
            char ch3 = len > 3 ? (char) b[3] : ' ';
            char ch4 = len > 4 ? (char) b[4] : ' ';
            if (len == 3) {
                int decimal = toDigit(ch0);
                double fractional = lookup[toDigit(ch2)];
                return decimal + fractional;
            }
            else if (len == 4) {
                // -1.2 or 11.2
                int decimal = (ch0 == '-' ? toDigit(ch1) : (fastMul10(toDigit(ch0)) + toDigit(ch1)));
                double fractional = lookup[toDigit(ch3)];
                if (ch0 == '-') {
                    return Math.negateExact(decimal) - fractional;
                }
                else {
                    return decimal + fractional;
                }
            }
            else {
                int decimal = fastMul10(toDigit(ch1)) + toDigit(ch2);
                double fractional = lookup[toDigit(ch4)];
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

    public static class AggregateMemoryMappedFile {
        private final int splitLength;

        public AggregateMemoryMappedFile(final int splitLength) {
            this.splitLength = splitLength;
        }

        public void compute(final String path,
                            final Consumer<Measurement> consumer)
                throws IOException {
            try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
                MemorySegment memorySegment = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
                long fileLength = file.length();
                int capacity = (int) (splitLength > file.length() ? 1 : (fileLength / splitLength + 1));
                System.err.printf("File length = %d split length = %d%n", file.length(), splitLength);
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
                    long segmentLength = Math.min(512, file.length() - i);
                    MemorySegment lookahead = memorySegment.asSlice(
                            endOffset,
                            segmentLength);
                    ByteBuffer bb = lookahead.asByteBuffer();
                    // find next new line char and next offset
                    LineMetadata lineMetadata = findNewLine(bb, (int) segmentLength);
                    endOffset += lineMetadata.offset;
                    splits.add(new Split(startOffset, endOffset - startOffset));
                    i = endOffset;
                    lookahead.unload();
                }
                System.err.printf("Splits = [%s]%n", splits);
                verifySplits(file.getChannel(), splits);
                System.err.printf("Splits calculation took %d ns%n", System.nanoTime() - time1);
                // consume splits in parallel using the common fork join pool
                splits
                        .stream()
                        .parallel()
                        .forEach(split -> {
                            MemorySegment subSegment = memorySegment.asSlice(split.offset, split.length);
                            subSegment.load();
                            ByteBuffer bb = subSegment.asByteBuffer();
                            long tid = Thread.currentThread().threadId();
                            new LineIterator(bb, 0, (int) split.length, 1024)
                                    .forEachRemaining(lineMetadata -> {
                                        Measurement m = new Measurement(
                                                lineMetadata.station,
                                                lineMetadata.stationLen,
                                                lineMetadata.temperature,
                                                lineMetadata.temperatureLen,
                                                lineMetadata.precomputedHashCode,
                                                tid);
                                        consumer.accept(m);
                                    });
                            subSegment.unload();
                        });
            }
        }

        private static LineMetadata findNewLine(final ByteBuffer buffer,
                                                final int capacity) {
            byte[] src = new byte[Math.min(512, capacity)];
            byte[] station = new byte[src.length];
            byte[] temperature = new byte[8];
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
                hashCode = ((hashCode << 5) - hashCode) + b;
                if (!afterDelim && b != '\n') {
                    if (b == ';') {
                        afterDelim = true;
                    }
                    else {
                        station[j++] = b;
                    }
                }
                else if (b != '\n') {
                    temperature[k++] = b;
                }
                else {
                    return new LineMetadata(station, temperature, j, k, i + 1, hashCode, isAscii);
                }
            }
            if (j == 0 || k == 0) {
                hashCode = -1;
            }
            return new LineMetadata(station, temperature, j, k, i, hashCode, isAscii);
        }
    }

    record LineMetadata(byte[] station,
                        byte[] temperature,
                        int stationLen,
                        int temperatureLen,
                        int offset,
                        int precomputedHashCode,
                        boolean isAscii) {
    }

    private static class LineIterator implements Iterator<LineMetadata> {
        private ByteBuffer bb;
        private int segmentLength;
        private int offset;
        private int batchSize;

        private List<LineMetadata> outputBatch = new ArrayList<>();
        private int cursor = 0;

        public LineIterator(final ByteBuffer bb,
                            final int offset,
                            final int segmentLength,
                            final int batchSize) {
            this.bb = bb;
            this.offset = offset;
            this.segmentLength = segmentLength;
            this.batchSize = batchSize;
        }

        @Override
        public boolean hasNext() {
            if (outputBatch != null
                    && !outputBatch.isEmpty()
                    && cursor < outputBatch.size()) {
                return true;
            }
            // output batch has been exhausted

            // check whether there is more data to read in this segment
            if (offset >= segmentLength) {
                return false;
            }

            // generate the next batch
            return generateNextBatch();
        }

        @Override
        public LineMetadata next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (cursor >= outputBatch.size()) {
                throw new IllegalStateException();
            }
            // get line at cursor
            return outputBatch.get(cursor++);
        }

        private boolean generateNextBatch() {
            // create an empty batch
            // attempt to fill the next batch completely
            List<LineMetadata> tempBatch = new ArrayList<>(batchSize);
            cursor = 0;
            for (int i = 0; i < batchSize; i++) {
                // find the next line
                LineMetadata lineMetadata = findNextLine();
                // add next line to batch
                tempBatch.add(lineMetadata);
                // if offset has gone past segmentLength, stop looping
                if (offset >= segmentLength) {
                    break;
                }
            }
            outputBatch = tempBatch;
            return !outputBatch.isEmpty();
        }

        private LineMetadata findNextLine() {
            // create station, temperature byte arrays to hold contents copied over from src
            byte[] station = new byte[256];
            byte[] temperature = new byte[8];
            // initialize src and dst idx vars
            int i = offset;
            int j = 0;
            int k = 0;
            // used for ASCII/UTF-8 encoding detection
            boolean isAscii = true;
            boolean afterDelim = false;
            while (true) {
                if (i >= segmentLength) {
                    break;
                }
                byte b = bb.get();
                if (b < 0) {
                    isAscii = false;
                }
                if (!afterDelim && b != '\n') {
                    if (b == ';') {
                        afterDelim = true;
                    }
                    else {
                        if (j >= station.length) {
                            System.out.println("Resizing station byte[]");
                            byte[] temp = new byte[(station.length << 1)];
                            System.arraycopy(station, 0, temp, 0, j);
                            station = temp;
                        }
                        station[j] = b;
                        j++;
                    }
                }
                else if (b != '\n') {
                    temperature[k++] = b;
                }
                i++;
                offset++;
                if (b == '\n') {
                    break;
                }
            }
            return new LineMetadata(station, temperature, j, k, offset, -1, isAscii);
        }
    }

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
                System.err.printf("Found non-newline character: '%s'. String: %s%n", (char) arr[0], new String(arr, StandardCharsets.UTF_8));
                System.err.printf("Error in split generation: [currentSplit = %s, curOffset = %s]%n", split, curOffset);
                throw new IllegalStateException();
            }
            curOffset += split.length;
        }
        System.err.println("Splits verified successfully");
    }

    record Split(long offset, long length) {
    }
}
