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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

public class CalculateAverage_jparera {
    private static final String FILE = "./measurements.txt";

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

    private static final int BYTE_SPECIES_SIZE = BYTE_SPECIES.vectorByteSize();

    private static final int BYTE_SPECIES_LANES = BYTE_SPECIES.length();

    private static final ValueLayout.OfLong LONG_U_LE = ValueLayout.JAVA_LONG_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    public static void main(String[] args) throws IOException {
        try (var fc = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            try (var arena = Arena.ofShared()) {
                var fs = fc.map(MapMode.READ_ONLY, 0, fc.size(), arena);
                var map = chunks(fs)
                        .parallelStream()
                        .map(Chunk::parse)
                        .flatMap(List::stream)
                        .collect(Collectors.toMap(
                                Entry::key,
                                Function.identity(),
                                Entry::merge,
                                TreeMap::new));
                System.out.println(map);
            }
        }
    }

    private static Collection<Chunk> chunks(MemorySegment ms) {
        var cpus = Runtime.getRuntime().availableProcessors();
        long expectedChunkSize = Math.ceilDiv(ms.byteSize(), cpus);
        var chunks = new ArrayList<Chunk>();
        long fileSize = ms.byteSize();
        long offset = 0;
        while (offset < fileSize) {
            var end = Math.min(offset + expectedChunkSize, fileSize);
            while (end < fileSize && ms.get(ValueLayout.JAVA_BYTE, end++) != '\n') {
            }
            long len = end - offset;
            chunks.add(new Chunk(ms.asSlice(offset, len)));
            offset = end;
        }
        return chunks;
    }

    private static final class Chunk {
        private static final byte SEPARATOR = ';';

        private static final byte DECIMAL_SEPARATOR = '.';

        private static final byte LF = '\n';

        private static final byte MINUS = '-';

        private static final int KEY_LOG2_BYTES = 7;

        private static final int KEY_BYTES = 1 << KEY_LOG2_BYTES;

        private static final int MAP_CAPACITY = 1 << 16;

        private static final int BUCKET_MASK = MAP_CAPACITY - 1;

        private final MemorySegment segment;

        private final Entry[] entries = new Entry[MAP_CAPACITY];

        private long offset;

        private byte current;

        private boolean hasCurrent = true;

        Chunk(MemorySegment segment) {
            this.segment = segment;
        }

        public List<Entry> parse() {
            long size = this.segment.byteSize();
            long safe = size - KEY_BYTES;
            while (offset < safe) {
                var e = vectorizedEntry();
                int value = vectorizedValue();
                e.add(value);
            }
            next();
            while (hasCurrent()) {
                var e = entry();
                int value = value();
                e.add(value);
            }
            var output = new ArrayList<Entry>(entries.length);
            for (int i = 0; i < entries.length; i++) {
                var e = entries[i];
                if (e != null) {
                    output.add(e);
                }
            }
            return output;
        }

        private Entry vectorizedEntry() {
            var start = this.offset;
            var first = ByteVector.fromMemorySegment(BYTE_SPECIES, this.segment, start, ByteOrder.nativeOrder());
            int equals = first.eq(SEPARATOR).firstTrue();
            int len = equals;
            for (int i = BYTE_SPECIES_SIZE; equals == BYTE_SPECIES_LANES; i += BYTE_SPECIES_SIZE) {
                var next = ByteVector.fromMemorySegment(BYTE_SPECIES, this.segment, start + i, ByteOrder.nativeOrder());
                equals = next.eq(SEPARATOR).firstTrue();
                len += equals;
            }
            this.offset = start + len + 1;
            int index = hash(this.segment, start, len);
            int count = 0;
            while (count < BUCKET_MASK) {
                index = index & BUCKET_MASK;
                var e = this.entries[index];
                if (e == null) {
                    return this.entries[index] = new Entry(len, this.segment.asSlice(start, KEY_BYTES));
                }
                else if (e.keyLength() == len && vectorizedEquals(e, first, start, len)) {
                    return e;
                }
                index++;
                count++;
            }
            throw new IllegalStateException("Map is full!");
        }

        private Entry entry() {
            long start = this.offset - 1;
            int len = 0;
            while (hasCurrent() && current != SEPARATOR) {
                len++;
                next();
            }
            expect(SEPARATOR);
            int index = hash(segment, start, len);
            int count = 0;
            while (count < BUCKET_MASK) {
                index = index & BUCKET_MASK;
                var e = this.entries[index];
                if (e == null) {
                    return this.entries[index] = new Entry(len, this.segment.asSlice(start, len));
                }
                else if (e.keyLength() == len && equals(e, start, len)) {
                    return e;
                }
                index++;
                count++;
            }
            throw new IllegalStateException("Map is full!");
        }

        private static final long MULTIPLY_ADD_DIGITS = 100 * (1L << 24) + 10 * (1L << 16) + 1;

        private int vectorizedValue() {
            long dw = this.segment.get(LONG_U_LE, this.offset);
            boolean negative = ((dw & 0xFF) ^ MINUS) == 0;
            int zeros = Long.numberOfTrailingZeros(~dw & 0x10101000L);
            dw = ((negative ? (dw & ~0xFF) : dw) << (28 - zeros)) & 0x0F000F0F00L;
            int value = (int) (((dw * MULTIPLY_ADD_DIGITS) >>> 32) & 0x3FF);
            this.offset += (zeros >>> 3) + 3;
            return negative ? -value : value;
        }

        private int value() {
            int value = 0;
            var negative = false;
            if (consume(MINUS)) {
                negative = true;
            }
            while (hasCurrent()) {
                if ((current & 0xF0) == 0x30) {
                    value *= 10;
                    value += current - '0';
                }
                else if (current != DECIMAL_SEPARATOR) {
                    break;
                }
                next();
            }
            if (hasCurrent()) {
                expect(LF);
            }
            return negative ? -value : value;
        }

        private boolean vectorizedEquals(Entry entry, ByteVector okey, long offset, int len) {
            var ekey = ByteVector.fromMemorySegment(BYTE_SPECIES, entry.segment(), 0, ByteOrder.nativeOrder());
            int equals = ekey.eq(okey).not().firstTrue();
            if (equals != BYTE_SPECIES_LANES) {
                return equals >= len;
            }
            long eo = BYTE_SPECIES_SIZE;
            int total = BYTE_SPECIES_LANES;
            while (equals == BYTE_SPECIES_LANES & eo < KEY_BYTES) {
                offset += BYTE_SPECIES_SIZE;
                ekey = ByteVector.fromMemorySegment(BYTE_SPECIES, entry.segment(), eo, ByteOrder.nativeOrder());
                okey = ByteVector.fromMemorySegment(BYTE_SPECIES, segment, offset, ByteOrder.nativeOrder());
                equals = ekey.eq(okey).not().firstTrue();
                total += equals;
                eo += BYTE_SPECIES_SIZE;
            }
            return total >= len;
        }

        private boolean equals(Entry entry, long offset, int len) {
            return MemorySegment.mismatch(this.segment, offset, offset + len, entry.segment(), 0, len) == -1;
        }

        private static final int GOLDEN_RATIO = 0x9E3779B9;
        private static final int HASH_LROTATE = 5;

        private static int hash(MemorySegment ms, long start, int len) {
            int x, y;
            if (len >= Integer.BYTES) {
                x = ms.get(ValueLayout.JAVA_INT_UNALIGNED, start);
                y = ms.get(ValueLayout.JAVA_INT_UNALIGNED, start + len - Integer.BYTES);
            }
            else {
                x = ms.get(ValueLayout.JAVA_BYTE, start);
                y = ms.get(ValueLayout.JAVA_BYTE, start + len - Byte.BYTES);
            }
            return (Integer.rotateLeft(x * GOLDEN_RATIO, HASH_LROTATE) ^ y) * GOLDEN_RATIO;
        }

        private void expect(byte b) {
            if (!consume(b)) {
                throw new IllegalStateException("Unexpected token!");
            }
        }

        private boolean consume(byte b) {
            if (current == b) {
                next();
                return true;
            }
            return false;
        }

        private boolean hasCurrent() {
            return hasCurrent;
        }

        private void next() {
            if (offset < segment.byteSize()) {
                this.current = segment.get(ValueLayout.JAVA_BYTE, offset++);
            }
            else {
                this.hasCurrent = false;
            }
        }
    }

    private static final class Entry {
        private final int keyLength;

        private final MemorySegment segment;

        private int min = Integer.MAX_VALUE;

        private int max = Integer.MIN_VALUE;

        private long sum;

        private int count;

        Entry(int keyLength, MemorySegment segment) {
            this.keyLength = keyLength;
            this.segment = segment;
        }

        int keyLength() {
            return keyLength;
        }

        MemorySegment segment() {
            return segment;
        }

        public String key() {
            return new String(segment.asSlice(0, keyLength).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }

        public void add(int value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        public Entry merge(Entry o) {
            min = Math.min(min, o.min);
            max = Math.max(max, o.max);
            sum += o.sum;
            count += o.count;
            return this;
        }

        @Override
        public String toString() {
            var average = Math.round(((sum / 10.0) / count) * 10.0);
            return decimal(min) + "/" + decimal(average) + "/" + decimal(max);
        }

        private static String decimal(long value) {
            boolean negative = value < 0;
            value = Math.abs(value);
            return (negative ? "-" : "") + (value / 10) + "." + (value % 10);
        }
    }
}
