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
import jdk.incubator.vector.VectorOperators;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CalculateAverage_plevart {
    private static final Path FILE = Path.of("measurements.txt");

    private static final int MAX_CITY_LEN = 100;
    // 100 (city name) + 1 (;) + 5 (-99.9) + 1 (NL)
    private static final int MAX_LINE_LEN = MAX_CITY_LEN + 7;

    private static final int INITIAL_TABLE_CAPACITY = 8192;

    public static void main(String[] args) throws IOException {
        var arena = Arena.global();
        try (
                var channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ)) {
            var segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(FILE), arena);
            int regions = Runtime.getRuntime().availableProcessors();
            IntStream
                    .range(0, regions)
                    .parallel()
                    .mapToObj(r -> calculateRegion(segment, regions, r))
                    .reduce(StatsTable::reduce)
                    .ifPresent(System.out::println);
            segment.unload();
        }
    }

    private static StatsTable calculateRegion(MemorySegment segment, int regions, int r) {
        long start = (segment.byteSize() * r) / regions;
        long end = (segment.byteSize() * (r + 1)) / regions;
        if (r > 0) {
            start = skipPastNl(segment, start);
        }
        if (r + 1 < regions) {
            end = skipPastNl(segment, end);
        }

        var stats = new StatsTable(segment, INITIAL_TABLE_CAPACITY);
        calculateAdjustedRegion(segment, start, end, stats);
        return stats;
    }

    private static long skipPastNl(MemorySegment segment, long i) {
        int skipped = 0;
        while (skipped++ < MAX_LINE_LEN && getByte(segment, i++) != '\n') {
        }
        if (skipped > MAX_LINE_LEN) {
            throw new IllegalArgumentException(
                    "Encountered line that exceeds " + MAX_LINE_LEN + " bytes at offset: " + i);
        }
        return i;
    }

    private static void calculateAdjustedRegion(MemorySegment segment, long start, long end, StatsTable stats) {
        var species = ByteVector.SPECIES_PREFERRED;
        long speciesByteSize = species.vectorByteSize();

        long cityStart = start, numberStart = 0;
        int cityLen = 0;

        for (long i = start, j = i; i < end; j = i) {
            long semiNlSet;
            if (end - i >= speciesByteSize) {
                var vec = ByteVector.fromMemorySegment(species, segment, i, ByteOrder.nativeOrder());
                semiNlSet = vec.compare(VectorOperators.EQ, (byte) ';')
                        .or(vec.compare(VectorOperators.EQ, (byte) '\n'))
                        .toLong();
                i += speciesByteSize;
            }
            else { // tail, smaller than speciesByteSize
                semiNlSet = 0;
                long mask = 1;
                while (i < end && mask != 0) {
                    int c = getByte(segment, i++);
                    if (c == '\n' || c == ';') {
                        semiNlSet |= mask;
                    }
                    mask <<= 1;
                }
            }

            for (int step = Long.numberOfTrailingZeros(semiNlSet); step < 64; semiNlSet >>>= (step + 1), step = Long.numberOfTrailingZeros(semiNlSet)) {
                j += step;
                if (numberStart == 0) { // semi
                    cityLen = (int) (j - cityStart);
                    numberStart = ++j;
                }
                else { // nl
                    int numberLen = (int) (j - numberStart);
                    calculateEntry(segment, cityStart, cityLen, numberStart, numberLen, stats);
                    cityStart = ++j;
                    numberStart = 0;
                }
            }
        }
    }

    private static void calculateEntry(MemorySegment segment, long cityStart, int cityLen, long numberStart, int numberLen, StatsTable stats) {
        int hash = StatsTable.hash(segment, cityStart, cityLen);
        int number = parseNumber(segment, numberStart, numberLen);
        stats.aggregate(cityStart, cityLen, hash, 1, number, number, number);
    }

    private static int parseNumber(MemorySegment segment, long off, int len) {
        int c0 = getByte(segment, off);
        int d0;
        int sign;
        if (c0 == '-') {
            off++;
            len--;
            d0 = getByte(segment, off) - '0';
            sign = -1;
        } else {
            d0 = c0 - '0';
            sign = 1;
        }
        return sign * switch (len) {
            case 1 -> d0 * 10;                  // 9
            case 2 -> {
                int d1 = getByte(segment, off + 1) - '0';
                yield d0 * 100 + d1 * 10;       // 99
            }
            case 3 -> {
                int d2 = getByte(segment, off + 2) - '0';
                yield d0 * 10 + d2;             // 9.9
            }
            case 4 -> {
                int d1 = getByte(segment, off + 1) - '0';
                int d3 = getByte(segment, off + 3) - '0';
                yield d0 * 100 + d1 * 10 + d3;  // 99.9
            }
            default -> {
                throw new IllegalArgumentException("Invalid number: " + getString(segment, off, len));
            }
        };
    }

    private static int getByte(MemorySegment segment, long off) {
        return segment.get(ValueLayout.JAVA_BYTE, off);
    }

    private static String getString(MemorySegment segment, long off, int len) {
        return new String(segment.asSlice(off, len).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    final static class StatsTable implements Cloneable {
        private static final int LOAD_FACTOR = 16;
        // offsets of fields
        private static final int _lenHash = 0,
                _off = 1,
                _count = 2,
                _sum = 3,
                _min = 4,
                _max = 5;
        private final MemorySegment segment;
        private int pow2cap, loadedSize;
        private long[] table;

        StatsTable(MemorySegment segment, int capacity) {
            this.segment = segment;
            int pow2cap = Integer.highestOneBit(capacity);
            if (pow2cap < capacity) {
                pow2cap <<= 1;
            }
            this.pow2cap = pow2cap;
            this.table = new long[idx(pow2cap)];
        }

        private static int idx(int i) {
            return i << 3;
        }

        private static long lenHash(int len, int hash) {
            return ((long) len << 32) | ((long) hash & 0x00000000FFFFFFFFL);
        }

        private static int len(long lenHash) {
            return (int) (lenHash >>> 32);
        }

        private static int hash(long lenHash) {
            return (int) (lenHash & 0x00000000FFFFFFFFL);
        }

        private static final long[] LEN_LONG_MASK;
        private static final int[] LEN_INT_MASK;

        static {
            LEN_LONG_MASK = new long[Long.BYTES + 1];
            for (int len = 0; len <= Long.BYTES; len++) {
                LEN_LONG_MASK[len] = len == 0
                        ? 0L
                        : ValueLayout.JAVA_LONG_UNALIGNED.order() == ByteOrder.LITTLE_ENDIAN
                                ? -1L >>> ((Long.BYTES - len) * Byte.SIZE)
                                : -1L << ((Long.BYTES - len) * Byte.SIZE);
            }
            LEN_INT_MASK = new int[Integer.BYTES + 1];
            for (int len = 0; len <= Integer.BYTES; len++) {
                LEN_INT_MASK[len] = len == 0
                        ? 0
                        : ValueLayout.JAVA_LONG_UNALIGNED.order() == ByteOrder.LITTLE_ENDIAN
                                ? -1 >>> ((Integer.BYTES - len) * Byte.SIZE)
                                : -1 << ((Integer.BYTES - len) * Byte.SIZE);
            }
        }

        static int hash(MemorySegment segment, long off, int len) {
            if (len > Integer.BYTES) {
                int head = segment.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                int tail = segment.get(ValueLayout.JAVA_INT_UNALIGNED, off + len - Integer.BYTES);
                return (head * 31) ^ tail;
            }
            else {
                // assert len >= 0 && len <= 4;
                // each city name starts at least 4 bytes before segment end
                // assert off + Integer.BYTES <= segment.byteSize();
                return segment.get(ValueLayout.JAVA_INT_UNALIGNED, off) & LEN_INT_MASK[len];
            }
        }

        static boolean equals(MemorySegment segment, long off1, long off2, int len) {
            while (len >= Long.BYTES) {
                if (segment.get(ValueLayout.JAVA_LONG_UNALIGNED, off1) != segment.get(ValueLayout.JAVA_LONG_UNALIGNED, off2)) {
                    return false;
                }
                off1 += Long.BYTES;
                off2 += Long.BYTES;
                len -= Long.BYTES;
            }
            // still enough memory to compare two longs, but masked?
            if (Math.max(off1, off2) + Long.BYTES <= segment.byteSize()) {
                long mask = LEN_LONG_MASK[len];
                return (segment.get(ValueLayout.JAVA_LONG_UNALIGNED, off1) & mask) == (segment.get(ValueLayout.JAVA_LONG_UNALIGNED, off2) & mask);
            }
            else {
                return equalsAtBorder(segment, off1, off2, len);
            }
        }

        private static boolean equalsAtBorder(MemorySegment segment, long off1, long off2, int len) {
            if (len > Integer.BYTES) {
                if (segment.get(ValueLayout.JAVA_INT_UNALIGNED, off1) != segment.get(ValueLayout.JAVA_INT_UNALIGNED, off2)) {
                    return false;
                }
                len -= Integer.BYTES;
                off1 += Integer.BYTES;
                off2 += Integer.BYTES;
            }
            // assert len >= 0 && len <= 4;
            // each city name starts at least 4 bytes before segment end
            // assert Math.max(off1, off2) + Integer.BYTES <= segment.byteSize();
            int mask = LEN_INT_MASK[len];
            return (segment.get(ValueLayout.JAVA_INT_UNALIGNED, off1) & mask) == (segment.get(ValueLayout.JAVA_INT_UNALIGNED, off2) & mask);
        }

        void aggregate(
                       // key
                       long off, int len, int hash,
                       // value
                       long count, long sum, long min, long max) {
            long lenHash = lenHash(len, hash);
            int mask = pow2cap - 1;
            for (int i = hash & mask, probe = 0; probe < pow2cap; i = (i + 1) & mask, probe++) {
                int idx = idx(i);
                long lenHash_i = table[idx + _lenHash];
                if (lenHash_i == 0) {
                    table[idx + _lenHash] = lenHash;
                    table[idx + _off] = off;
                    table[idx + _count] = count;
                    table[idx + _sum] = sum;
                    table[idx + _min] = min;
                    table[idx + _max] = max;
                    loadedSize += LOAD_FACTOR;
                    if (loadedSize >= pow2cap) {
                        grow();
                    }
                    return;
                }
                if (lenHash_i == lenHash && equals(segment, table[idx + _off], off, len)) {
                    table[idx + _count] += count;
                    table[idx + _sum] += sum;
                    table[idx + _min] = Math.min(min, table[idx + _min]);
                    table[idx + _max] = Math.max(max, table[idx + _max]);
                    return;
                }
            }
            throw new OutOfMemoryError("StatsTable capacity exceeded due to poor hash");
        }

        private void grow() {
            if (idx(pow2cap) >= 0x4000_0000) {
                throw new OutOfMemoryError("StatsTable capacity exceeded");
            }
            else {
                var oldStats = clone();
                pow2cap <<= 1;
                table = new long[idx(pow2cap)];
                loadedSize = 0;
                reduce(oldStats);
            }
        }

        @Override
        protected StatsTable clone() {
            try {
                return (StatsTable) super.clone();
            }
            catch (CloneNotSupportedException e) {
                throw new InternalError(e);
            }
        }

        StatsTable reduce(StatsTable other) {
            other
                    .idxStream()
                    .forEach(
                            idx -> aggregate(
                                    other.table[idx + _off],
                                    len(other.table[idx + _lenHash]),
                                    hash(other.table[idx + _lenHash]),
                                    other.table[idx + _count],
                                    other.table[idx + _sum],
                                    other.table[idx + _min],
                                    other.table[idx + _max]));
            return this;
        }

        IntStream idxStream() {
            return IntStream
                    .range(0, pow2cap)
                    .map(StatsTable::idx)
                    .filter(idx -> table[idx + _lenHash] != 0);
        }

        Stream<Entry> stream() {
            return idxStream()
                    .mapToObj(
                            idx -> new Entry(
                                    new String(
                                            segment
                                                    .asSlice(table[idx + _off], len(table[idx + _lenHash]))
                                                    .toArray(ValueLayout.JAVA_BYTE),
                                            StandardCharsets.UTF_8),
                                    table[idx + _count],
                                    table[idx + _sum],
                                    table[idx + _min],
                                    table[idx + _max]));
        }

        @Override
        public String toString() {
            return stream()
                    .sorted(Comparator.comparing(StatsTable.Entry::city))
                    .map(Entry::toString)
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        record Entry(String city, long count, long sum, long min, long max) {
            double average() {
                return count > 0L ? (double) sum / (double) count : 0d;
            }

            @Override
            public String toString() {
                return String.format(
                    "%s=%.1f/%.1f/%.1f",
                    city(), (double) min() / 10d, average() / 10d, (double) max() / 10d
                );
            }
        }
    }
}