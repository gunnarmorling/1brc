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
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.LongStream;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_kgeri {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregate {
        private double min = Double.POSITIVE_INFINITY;
        private double sum = 0d;
        private double max = Double.NEGATIVE_INFINITY;
        private long count;

        public void append(double measurement) {
            min = Math.min(min, measurement);
            max = Math.max(max, measurement);
            sum += measurement;
            count++;
        }

        public void merge(MeasurementAggregate other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        @Override
		public String toString() {
			return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
		}

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    /**
     * This is to avoid instantiating `String`s during processing as much as possible.
     */
    private static class StringSlice {
        protected final byte[] buf;
        protected int len;
        protected int hash;

        public StringSlice(StringSlice other) {
            buf = Arrays.copyOfRange(other.buf, 0, other.len);
            len = other.len;
            hash = other.hash;
        }

        public StringSlice(byte[] buf, int len) {
            this.buf = buf;
            this.len = len;
            calculateHash();
        }

        public int length() {
            return len;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof StringSlice other && Arrays.equals(buf, 0, len, other.buf, 0, other.len);
        }

        @Override
        public String toString() {
            return new String(buf, 0, len, UTF_8);
        }

        protected void calculateHash() {
            hash = 1;
            for (int i = 0; i < len; i++) {
                hash = 31 * hash + buf[i];
            }
        }
    }

    /**
     * A flyweight of {@link StringSlice}, to avoid instantiating that as well.
     */
    private static class MutableStringSlice extends StringSlice {

        public MutableStringSlice(byte[] buf) {
            super(buf, 0);
        }

        public void updateLength(int length) {
            len = length;
            calculateHash();
        }

        public void clear() {
            len = 0;
            hash = 0;
        }
    }

    private static Map<StringSlice, MeasurementAggregate> readChunk(FileChannel channel, long from, long chunkSize) {
        MemorySegment data;
        long remaining;
        try {
            long offset = Math.max(0, from - 1);
            remaining = channel.size() - offset;
            data = channel.map(READ_ONLY, offset, Math.min(remaining, chunkSize + 256), Arena.ofConfined()); // +256 to allow for reading records that are split by the chunk
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        long start = 0;
        if (from > 0) {
            while (data.get(JAVA_BYTE, start++) != '\n') {
            }
        }

        Map<StringSlice, MeasurementAggregate> results = new HashMap<>(1000);
        byte[] buf = new byte[256];
        MutableStringSlice name = new MutableStringSlice(buf);

        long until = Math.min(remaining, chunkSize); // Records may end (slightly) after the chunk boundary, but must not start after it
        for (long pos = start; pos < until;) {
            name.clear();
            int i = 0;
            for (;; i++) {
                if (pos + i >= data.byteSize()) { // Guard against malformed data (typically a missing newline at the end of the input)
                    pos = chunkSize;
                    break;
                }
                byte b = data.get(JAVA_BYTE, pos + i);
                buf[i] = b;
                if (b == ';') {
                    name.updateLength(i);
                }
                else if (b == '\n') {
                    pos = pos + i + 1;
                    break;
                }
            }

            double measurement = parseDoubleFrom(buf, name.length() + 1, i - name.length() - 1);
            MeasurementAggregate aggr = results.get(name);
            if (aggr == null) {
                aggr = new MeasurementAggregate();
                results.put(new StringSlice(name), aggr);
            }
            aggr.append(measurement);
        }

        return results;
    }

    // Note: based on java.lang.Integer.parseInt, surely missing some edge cases but avoids allocation
    private static double parseDoubleFrom(byte[] buf, int offset, int length) {
        boolean negative = false;
        int integer = 0;
        for (int i = 0; i < length; i++) {
            byte c = buf[offset + i];
            if (c == '-') {
                negative = true;
            }
            else if (c != '.') {
                integer *= 10;
                integer -= c - '0';
            }
        }
        return (negative ? integer : -integer) / 10.0;
    }

    public static void main(String[] args) throws IOException {
        Map<String, MeasurementAggregate> measurements = new TreeMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            long size = raf.length();
            long threads = Math.min(ForkJoinPool.getCommonPoolParallelism(), Math.max(size / 1000000, 1));
            long chunkSize = Math.ceilDiv(size, threads);
            System.err.printf("Processing size=%d, threads=%d, chunkSize=%d%n", size, threads, chunkSize);

            List<Map<StringSlice, MeasurementAggregate>> chunks = LongStream.range(0, threads)
                    .parallel()
                    .mapToObj(i -> readChunk(raf.getChannel(), i * chunkSize, chunkSize))
                    .toList();

            for (Map<StringSlice, MeasurementAggregate> chunk : chunks) {
                chunk.forEach((n, m) -> measurements.computeIfAbsent(n.toString(), x -> new MeasurementAggregate()).merge(m));
            }
        }

        System.out.println(measurements);
    }
}
