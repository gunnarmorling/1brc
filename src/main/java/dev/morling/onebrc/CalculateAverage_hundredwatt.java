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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_hundredwatt {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_ROW_SIZE = 100 + 1 + 5 + 1; // 100 for city name, 1 for ;, 5 for temperature, 1 for \n
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final long BUFFER_SIZE = 128 * 1024 * 1024; // 128MB
    private static final long CHUNK_SIZE = BUFFER_SIZE / THREAD_COUNT;
    private static final long FILE_CHUNK_SIZE = CHUNK_SIZE - MAX_ROW_SIZE;
    public static final int TEMPERATURE_SLOTS = 5003; // prime number
    private static final short[] TEMPERATURES = new short[TEMPERATURE_SLOTS];
    private static final long PERFECT_HASH_SEED = -1982870890352534081L;

    // Construct a perfect hash function mapping temperatures encoded as longs (e.g., 0x2d342e3000000000 for -4.3) to
    // the corresponding short integer (e.g., -43).
    static {
        // Figure out encoding for all possible temperature values (1999 total)
        long start = System.currentTimeMillis();
        Map<Long, Short> decodeTemperatureMap = new HashMap<>();
        for (short i = -999; i <= 999; i++) {
            long word = 0;
            int shift = 0;
            if (i < 0) {
                word |= ((long) '-') << shift;
                shift += 8;
            }
            if (Math.abs(i) >= 100) {
                int hh = Math.abs(i) / 100;
                int tt = (Math.abs(i) - hh * 100) / 10;

                word |= ((long) (hh + '0')) << shift;
                shift += 8;
                word |= ((long) (tt + '0')) << shift;
            }
            else {
                int tt = Math.abs(i) / 10;
                // convert to ascii
                word |= ((long) (tt + '0')) << shift;
            }
            shift += 8;
            word |= ((long) '.') << shift;
            shift += 8;
            int uu = Math.abs(i) % 10;
            word |= ((long) (uu + '0')) << shift;

            // 31302e3000000000
            decodeTemperatureMap.put(word, i);
        }

        decodeTemperatureMap.entrySet().stream().forEach(e -> {
            var word = e.getKey();
            var h = (word * PERFECT_HASH_SEED) & ~(1L << 63);
            var pos = (int) (h % TEMPERATURE_SLOTS);
            if (TEMPERATURES[pos] != 0)
                throw new RuntimeException("collision at " + pos);
            TEMPERATURES[pos] = e.getValue();
        });
        // System.out.println("Building table took " + (System.currentTimeMillis() - start) + "ms");
    }

    static class Record {
        short min;
        short max;
        int sum;
        int count;

        public Record() {
            this.min = Short.MAX_VALUE;
            this.max = Short.MIN_VALUE;
            this.sum = 0;
            this.count = 0;
        }

        public void updateWith(short value) {
            min = (short) Math.min(min, value);
            max = (short) Math.max(max, value);
            sum += value;
            count++;
        }

        @Override
        public String toString() {
            return round(min / 10.0) + "/" + round(sum / 10.0 / count) + "/" + round(max / 10.0);
        }

        double round(double v) {
            return Math.round(v * 10.0) / 10.0;
        }
    }

    record Entry(long[] key, Record value) {
    }

    static class HashTable {
        private static final int INITIAL_SIZE = 16 * 1024;
        private static final float LOAD_FACTOR = 0.75f;
        private static final int GROW_FACTOR = 4;
        private final long[][] KEYS = new long[INITIAL_SIZE][];
        private final Record[] VALUES = new Record[INITIAL_SIZE];
        private final long[] HASHES = new long[INITIAL_SIZE];
        private int size = INITIAL_SIZE;

        public HashTable() {
            for (int i = 0; i < INITIAL_SIZE; i++) {
                VALUES[i] = new Record();
            }
        }

        public void putOrMerge(int hash, int length, long[] key, short value) {
            int idx = hash & (size - 1);

            // linear probing
            int i = 0;
            while (KEYS[idx] != null && (HASHES[idx] != hash) && (0 != Arrays.compareUnsigned(KEYS[idx], 0, KEYS[idx].length, key, 0, length))) {
                i++;
                idx = (idx + 1) & (size - 1);
            }

            if (KEYS[idx] == null) {
                KEYS[idx] = Arrays.copyOf(key, length);
                HASHES[idx] = hash;
            }

            VALUES[idx].updateWith(value);
        }

        public List<Entry> getAll() {
            List<Entry> result = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                if (KEYS[i] != null) {
                    result.add(new Entry(KEYS[i], VALUES[i]));
                }
            }
            return result;
        }
    }

    private static String keyToString(long[] key) {
        ByteBuffer kb = ByteBuffer.allocate(8 * key.length).order(ByteOrder.LITTLE_ENDIAN);
        Arrays.stream(key).forEach(kb::putLong);

        // remove trailing '\0' bytes from kb and
        // fix two ';' in word issue here (rather than in hot path)
        byte b;
        int limit = kb.position() - 8;
        kb.position(limit);
        while ((b = kb.get()) != 0 && b != ';' && limit < kb.capacity() - 1) {
            limit++;
        }

        kb.flip();
        byte[] bytes = new byte[limit];
        kb.get(bytes);

        return new String(bytes);
    }

    private static Record merge(Record v, Record value) {
        var record = new Record();
        record.min = (short) Math.min(v.min, value.min);
        record.max = (short) Math.max(v.max, value.max);
        record.sum = v.sum + value.sum;
        record.count = v.count + value.count;
        return record;
    }

    private static int processChunk(ByteBuffer bb, HashTable hashTable, long start, long size) {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        // Find first entry
        while (start != 0 && bb.get() != '\n') {
        }

        long word;
        long[] key = new long[13];
        int offset;
        long arg, hasvalue, op1, op2;
        int position = bb.position();
        long hash;
        long temperature_hash;
        int temperature_pos;
        short temperature_value;
        int hashInt;

        int rc = 0;
        int end = (int) (size - MAX_ROW_SIZE);
        while (position <= end) {
            // rc++;
            offset = -1;

            // Parse city name
            // First word
            hash = key[++offset] = bb.getLong(position + offset * 8);

            // From "Determine if a word has a byte equal to n"
            // https://graphics.stanford.edu/~seander/bithacks.html#ValueInWord
            arg = (key[offset]) ^ (0x0101010101010101L * (';'));
            op1 = (arg - 0x0101010101010101L);
            op2 = ~(arg);
            hasvalue = (op1 & op2 & 0x8080808080808080L);

            // Remaining words (if present)
            while (hasvalue == 0) {
                ++offset;
                key[offset] = bb.getLong(position + offset * 8);
                hash ^= key[offset];

                arg = (key[offset]) ^ (0x0101010101010101L * (';'));
                op1 = (arg - 0x0101010101010101L);
                op2 = ~(arg);
                hasvalue = (op1 & op2 & 0x8080808080808080L);
            }
            hash ^= key[offset]; // unset last word since it will be updated
            key[offset] = key[offset] & ~(-(hasvalue >> 7));
            hash ^= key[offset];

            position = position + offset * 8 + Long.numberOfTrailingZeros(hasvalue) / 8 + 1; // +1 for \n

            // Parse temperature
            word = bb.getLong(position);
            hasvalue = (word - 0x0B0B0B0B0B0B0B0BL) & 0x8080808080808080L;
            int newlinePos = Long.numberOfTrailingZeros(hasvalue) - 8;

            word = word & (~(-(1L << newlinePos)));

            // Perfect hash lookup for temperature
            temperature_hash = (word * PERFECT_HASH_SEED) & ~(1L << 63);
            temperature_pos = (int) (temperature_hash % TEMPERATURE_SLOTS);
            temperature_value = TEMPERATURES[temperature_pos];

            position = position + newlinePos / 8 + 2; // +1 for \n

            hashInt = (int) (hash ^ (hash >> 32) ^ (hash >> 17));

            hashTable.putOrMerge(hashInt, offset + 1, key, temperature_value);
        }
        return rc;
    }

    public static void main(String[] args) throws IOException {
        final long fileSize = Files.size(Path.of(FILE));
        // System.out.println("File size: " + fileSize);

        // AtomicLong rowCount = new AtomicLong();

        // Read file in chunks using striping
        try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
            var r = IntStream.range(0, THREAD_COUNT + 1).mapToObj(stripe -> {
                long start = stripe * FILE_CHUNK_SIZE;
                var hashTable = new HashTable();

                if (stripe == THREAD_COUNT) { // last thread
                    try {
                        // handle trailing bytes in file in jankiest way possible (for now hopefully :) )
                        byte[] trailing = new byte[MAX_ROW_SIZE * 2];
                        fileChannel.read(ByteBuffer.wrap(trailing), Math.max(0, fileSize - MAX_ROW_SIZE));
                        var rc = processChunk(ByteBuffer.wrap(trailing), hashTable, Math.max(0, fileSize - MAX_ROW_SIZE),
                                MAX_ROW_SIZE + Math.min(fileSize, MAX_ROW_SIZE) - 1);
                        // rowCount.addAndGet(rc);
                        return hashTable;

                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                // if file is smaller than max row size, we're done b/c the trailing bytes handler processed the whole file
                if (fileSize <= MAX_ROW_SIZE) {
                    return hashTable;
                }

                while (start < fileSize) {
                    long end = Math.min(start + CHUNK_SIZE, fileSize);
                    MappedByteBuffer bb = null;
                    try {
                        bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, Math.min(end - start + 8, fileSize - start));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    var rc = processChunk(bb, hashTable, start, end - start);

                    // rowCount.addAndGet(rc);
                    start += FILE_CHUNK_SIZE * THREAD_COUNT;
                }

                return hashTable;
            }).parallel().flatMap(partition -> partition.getAll().stream())
                    .collect(Collectors.toMap(e -> keyToString(e.key()), Entry::value, CalculateAverage_hundredwatt::merge, TreeMap::new));

            System.out.println(r);
            // System.out.println(rowCount.get());
        }
    }
}
