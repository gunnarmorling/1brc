//COMPILE_OPTIONS -source 21 --enable-preview --add-modules jdk.incubator.vector
//RUNTIME_OPTIONS --enable-preview --add-modules jdk.incubator.vector
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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorOperators;

public class CalculateAverage_jparera {
    private static final String FILE = "./measurements.txt";

    private static final VarHandle BYTE_HANDLE = MethodHandles
            .memorySegmentViewVarHandle(ValueLayout.JAVA_BYTE);

    private static final VarHandle INT_HANDLE = MethodHandles
            .memorySegmentViewVarHandle(ValueLayout.JAVA_INT_UNALIGNED);

    private static final VarHandle LONG_LE_HANDLE = MethodHandles
            .memorySegmentViewVarHandle(ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN));

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

    private static final int BYTE_SPECIES_LANES = BYTE_SPECIES.length();

    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private static final byte LF = '\n';

    private static final byte SEPARATOR = ';';

    private static final byte DECIMAL_SEPARATOR = '.';

    private static final byte NEG = '-';

    public static void main(String[] args) throws IOException, InterruptedException {
        try (var fc = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            try (var arena = Arena.ofShared()) {
                var fs = fc.map(MapMode.READ_ONLY, 0, fc.size(), arena);
                var cpus = Runtime.getRuntime().availableProcessors();
                var output = chunks(fs, cpus).stream()
                        .parallel()
                        .map(Chunk::parse)
                        .flatMap(List::stream)
                        .collect(Collectors.toMap(
                                Entry::key,
                                Function.identity(),
                                Entry::merge,
                                TreeMap::new));
                System.out.println(output);
            }
        }
    }

    private static List<Chunk> chunks(MemorySegment ms, int splits) {
        long fileSize = ms.byteSize();
        long expectedChunkSize = Math.ceilDiv(fileSize, splits);
        var chunks = new ArrayList<Chunk>();
        long offset = 0;
        while (offset < fileSize) {
            var end = Math.min(offset + expectedChunkSize, fileSize);
            while (end < fileSize && (byte) BYTE_HANDLE.get(ms, end++) != LF) {
            }
            long len = end - offset;
            chunks.add(new Chunk(ms.asSlice(offset, len)));
            offset = end;
        }
        return chunks;
    }

    private static final class Chunk {
        private static final int KEY_LOG2_BYTES = 7;

        private static final int KEY_BYTES = 1 << KEY_LOG2_BYTES;

        private static final int ENTRIES_LOG2_CAPACITY = 16;

        private static final int ENTRIES_CAPACITY = 1 << ENTRIES_LOG2_CAPACITY;

        private static final int ENTRIES_MASK = ENTRIES_CAPACITY - 1;

        private final MemorySegment segment;

        private final long size;

        private final Entry[] entries = new Entry[ENTRIES_CAPACITY];

        private final byte[] keys = new byte[ENTRIES_CAPACITY * KEY_BYTES];

        private final MemorySegment kms = MemorySegment.ofArray(this.keys);

        private static final int KEYS_MASK = (ENTRIES_CAPACITY * KEY_BYTES) - 1;

        private long offset;

        private byte current;

        private boolean hasCurrent = true;

        Chunk(MemorySegment segment) {
            this.segment = segment;
            this.size = segment.byteSize();
        }

        public List<Entry> parse() {
            long safe = size - KEY_BYTES;
            while (offset < safe) {
                vectorizedEntry().add(vectorizedValue());
            }
            next();
            while (hasCurrent()) {
                entry().add(value());
            }
            var output = new ArrayList<Entry>(entries.length);
            for (int i = 0, o = 0; i < entries.length; i++, o += KEY_BYTES) {
                var e = entries[i];
                if (e != null) {
                    e.setkey(keys, o);
                    output.add(e);
                }
            }
            return output;
        }

        private Entry vectorizedEntry() {
            var separators = ByteVector.broadcast(BYTE_SPECIES, SEPARATOR);
            int len = 0;
            for (int i = 0;; i += BYTE_SPECIES_LANES) {
                var block = ByteVector.fromMemorySegment(BYTE_SPECIES, this.segment, offset + i, NATIVE_ORDER);
                int equals = block.compare(VectorOperators.EQ, separators).firstTrue();
                len += equals;
                if (equals != BYTE_SPECIES_LANES) {
                    break;
                }
            }
            var start = this.offset;
            this.offset = start + len + 1;
            int hash = hash(segment, start, len);
            int index = (hash - (hash >>> -ENTRIES_LOG2_CAPACITY)) & ENTRIES_MASK;
            int keyOffset = index << KEY_LOG2_BYTES;
            int count = 0;
            while (count < ENTRIES_MASK) {
                index = index & ENTRIES_MASK;
                keyOffset = keyOffset & KEYS_MASK;
                var e = this.entries[index];
                if (e == null) {
                    MemorySegment.copy(this.segment, start, kms, keyOffset, len);
                    return this.entries[index] = new Entry(len, hash);
                }
                else if (e.hash == hash && e.keyLength == len) {
                    int total = 0;
                    for (int i = 0; i < KEY_BYTES; i += BYTE_SPECIES_LANES) {
                        var ekey = ByteVector.fromArray(BYTE_SPECIES, keys, keyOffset + i);
                        var okey = ByteVector.fromMemorySegment(BYTE_SPECIES, this.segment, start + i, NATIVE_ORDER);
                        int equals = ekey.compare(VectorOperators.NE, okey).firstTrue();
                        total += equals;
                        if (equals != BYTE_SPECIES_LANES) {
                            break;
                        }
                    }
                    if (total >= len) {
                        return e;
                    }
                }
                count++;
                index++;
                keyOffset += KEY_BYTES;
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
            int hash = hash(segment, start, len);
            int index = (hash - (hash >>> -ENTRIES_LOG2_CAPACITY)) & ENTRIES_MASK;
            int keyOffset = index << KEY_LOG2_BYTES;
            int count = 0;
            while (count < ENTRIES_MASK) {
                index = index & ENTRIES_MASK;
                keyOffset = keyOffset & KEYS_MASK;
                var e = this.entries[index];
                if (e == null) {
                    MemorySegment.copy(this.segment, start, kms, keyOffset, len);
                    return this.entries[index] = new Entry(len, hash);
                }
                else if (e.hash == hash && e.keyLength == len) {
                    int total = 0;
                    for (int i = 0; i < len; i++) {
                        if (((byte) BYTE_HANDLE.get(this.segment, start + i)) != this.keys[keyOffset + i]) {
                            break;
                        }
                        total++;
                    }
                    if (total >= len) {
                        return e;
                    }
                }
                count++;
                index++;
                keyOffset += KEY_BYTES;
            }
            throw new IllegalStateException("Map is full!");
        }

        private static final long MULTIPLY_ADD_DIGITS = 100 * (1L << 24) + 10 * (1L << 16) + 1;

        private int vectorizedValue() {
            long dw = (long) LONG_LE_HANDLE.get(this.segment, this.offset);
            int zeros = Long.numberOfTrailingZeros(~dw & 0x10101000L);
            boolean negative = ((dw & 0xFF) ^ NEG) == 0;
            dw = ((negative ? (dw & ~0xFF) : dw) << (28 - zeros)) & 0x0F000F0F00L;
            int value = (int) (((dw * MULTIPLY_ADD_DIGITS) >>> 32) & 0x3FF);
            this.offset += (zeros >>> 3) + 3;
            return negative ? -value : value;
        }

        private int value() {
            int value = 0;
            var negative = false;
            if (consume(NEG)) {
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

        private static final int GOLDEN_RATIO = 0x9E3779B9;
        private static final int HASH_LROTATE = 5;

        private static int hash(MemorySegment ms, long start, int len) {
            int x, y;
            if (len >= Integer.BYTES) {
                x = (int) INT_HANDLE.get(ms, start);
                y = (int) INT_HANDLE.get(ms, start + len - Integer.BYTES);
            }
            else {
                x = (byte) BYTE_HANDLE.get(ms, start) & 0xFF;
                y = (byte) BYTE_HANDLE.get(ms, start + len - Byte.BYTES) & 0xFF;
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
            if (offset < size) {
                this.current = (byte) BYTE_HANDLE.get(segment, offset++);
            }
            else {
                this.hasCurrent = false;
            }
        }
    }

    private static final class Entry {
        final int keyLength;

        final int hash;

        private int min = Integer.MAX_VALUE;

        private int max = Integer.MIN_VALUE;

        private long sum;

        private int count;

        private String key;

        Entry(int keyLength, int hash) {
            this.keyLength = keyLength;
            this.hash = hash;
        }

        public String key() {
            return key;
        }

        void setkey(byte[] keys, int offset) {
            this.key = new String(keys, offset, keyLength, StandardCharsets.UTF_8);
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
            return decimal(min) + '/' + decimal(average) + '/' + decimal(max);
        }

        private static String decimal(long value) {
            var builder = new StringBuilder();
            if (value < 0) {
                builder.append((char) NEG);
            }
            value = Math.abs(value);
            builder.append(value / 10);
            builder.append((char) DECIMAL_SEPARATOR);
            builder.append(value % 10);
            return builder.toString();
        }
    }
}
