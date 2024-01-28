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
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.concurrent.Executors;

/**
 * The implementation:
 * - reads a file with buffered IO
 * - uses VarHandles to get longs/ints from a byte array
 * - delimiter search is vectorized
 * - there is a custom hash function, that provides a low collision rate and short probe distances in hash tables
 * - has 2 custom open addressing hash tables: one for strings <=8 bytes in length, and one more for strings of any length
 */
public class CalculateAverage_melgenek {

    private static final VarHandle LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();
    private static final VarHandle INT_VIEW = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder()).withInvokeExactBehavior();
    private static final int CORES_COUNT = Runtime.getRuntime().availableProcessors();

    private static final String FILE = "./measurements.txt";

    /**
     * This is a prime number that gives pretty
     * <a href="https://vanilla-java.github.io/2018/08/15/Looking-at-randomness-and-performance-for-hash-codes.html">good hash distributions</a>
     * on the data in this challenge.
     */
    private static final long RANDOM_PRIME = 0x7A646E4D;
    private static final int ZERO_CHAR_3_SUM = 100 * '0' + 10 * '0' + '0';
    private static final int ZERO_CHAR_2_SUM = 10 * '0' + '0';
    private static final byte NEWLINE = '\n';
    private static final byte SEMICOLON = ';';
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int BYTE_SPECIES_BYTE_SIZE = BYTE_SPECIES.vectorByteSize();
    private static final Vector<Byte> NEWLINE_VECTOR = BYTE_SPECIES.broadcast(NEWLINE);
    private static final Vector<Byte> SEMICOLON_VECTOR = BYTE_SPECIES.broadcast(SEMICOLON);
    private static final int MAX_LINE_LENGTH = 107; // 100 + len(";-11.1\n") = 100+7
    private static final TreeMap<String, ResultRow> RESULT = new TreeMap<>();

    public static void main(String[] args) throws Throwable {
        long totalSize = Files.size(Path.of(FILE));
        try (var executor = Executors.newFixedThreadPool(CORES_COUNT - 1)) {
            long chunkSize = Math.max(1, totalSize / CORES_COUNT);
            long offset = 0;
            int i = 0;
            for (; offset < totalSize && i < CORES_COUNT - 1; i++) {
                long currentOffset = offset;
                long maxOffset = Math.min((i + 1) * chunkSize, totalSize);
                executor.submit(() -> processRange(currentOffset, maxOffset));
                offset = (i + 1) * chunkSize - 1;
            }
            if (offset < totalSize) {
                processRange(offset, totalSize);
            }
        }
        System.out.println(RESULT);
    }

    private static void processRange(long startOffset, long maxOffset) {
        final var table = new CompositeTable();
        try (var file = new BufferedFile(startOffset, maxOffset)) {
            processChunk(file, table);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        synchronized (RESULT) {
            table.addRows(RESULT);
        }
    }

    private static void processChunk(BufferedFile file, CompositeTable table) {
        if (file.offset != 0) {
            file.refillBuffer();
            int newlinePosition = findDelimiter(file, 0, NEWLINE_VECTOR, NEWLINE);
            file.bufferPosition = newlinePosition + 1;
            file.offset += file.bufferPosition;
        }
        while (file.offset < file.maxOffset) {
            file.refillBuffer();
            int bytesProcessed = processOneRow(file, table);
            file.offset += bytesProcessed;
        }
    }

    private static int processOneRow(BufferedFile file, CompositeTable table) {
        int stringStart = file.bufferPosition;
        int stringEnd = findDelimiter(file, stringStart, SEMICOLON_VECTOR, SEMICOLON);

        file.bufferPosition = stringEnd + 1;
        short value = parseValue(file);

        table.add(file.buffer, stringStart, stringEnd, value);

        return file.bufferPosition - stringStart;
    }

    private static short parseValue(BufferedFile file) {
        byte firstDigit = file.buffer[file.bufferPosition];
        int sign = 1;
        if (firstDigit == '-') {
            sign = -1;
            file.bufferPosition++;
            firstDigit = file.buffer[file.bufferPosition];
        }

        byte secondDigit = file.buffer[file.bufferPosition + 1];
        int result;
        if (secondDigit == '.') {
            result = firstDigit * 10 + file.buffer[file.bufferPosition + 2] - ZERO_CHAR_2_SUM;
            file.bufferPosition += 4;
        }
        else {
            result = firstDigit * 100 + secondDigit * 10 + file.buffer[file.bufferPosition + 3] - ZERO_CHAR_3_SUM;
            file.bufferPosition += 5;
        }
        return (short) (result * sign);
    }

    /**
     * <a href="https://gms.tf/stdfind-and-memchr-optimizations.html#do-more-faster">Finds a delimiter in a byte array using vectorized comparisons.</a>
     */
    private static int findDelimiter(BufferedFile file, int startPosition, Vector<Byte> repeatedDelimiter, byte delimiter) {
        int position = startPosition;
        int vectorLoopBound = startPosition + BYTE_SPECIES.loopBound(file.bufferLimit - startPosition);
        for (; position < vectorLoopBound; position += BYTE_SPECIES_BYTE_SIZE) {
            var vector = ByteVector.fromArray(BYTE_SPECIES, file.buffer, position);
            var comparisonResult = vector.compare(VectorOperators.EQ, repeatedDelimiter);
            if (comparisonResult.anyTrue()) {
                return position + comparisonResult.firstTrue();
            }
        }

        while (file.buffer[position] != delimiter) {
            position++;
        }

        return position;
    }

    private static long keepLastBytes(long value, int numBytesToKeep) {
        // Number of bits to shift, so that the mask covers only `numBytesToKeep` least significant bits
        int bitShift = (Long.BYTES - numBytesToKeep) * Byte.SIZE;
        // Mask with the specified number of the least significant bits set to 1
        long mask = -1L >>> bitShift;
        return value & mask;
    }

    /**
     * The function transforms a string with the length <=8 bytes to a java String.
     * The function assumes that the string is 0 terminated.
     */
    private static String longToString(long value) {
        int strLength = Long.BYTES - Long.numberOfLeadingZeros(value) / Byte.SIZE;
        var bytes = new byte[strLength];
        for (int i = 0; i < strLength; i++) {
            bytes[i] = (byte) (value >> (i * Byte.SIZE));
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Store measurements based on string lengths.
     * Stores strings of length <= 8 and other strings separately.
     * This table is a simplified implementation of strings hash table in <a href="https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/StringHashMap.h">ClickHouse</a>.
     * The original parer that describes benefits of the approach is <a href="https://www.mdpi.com/2076-3417/10/6/1915">SAHA: A String Adaptive Hash Table for Analytical Databases</a>.
     */
    private static final class CompositeTable {
        private final LongTable longTable = new LongTable();
        private final RegularTable regularTable = new RegularTable();

        private void add(byte[] buffer, int stringStart, int stringEnd, short value) {
            int stringLength = stringEnd - stringStart;
            if (stringLength <= Long.BYTES) {
                long str = keepLastBytes((long) LONG_VIEW.get(buffer, stringStart), stringLength);
                this.longTable.add(str, value);
            }
            else {
                int hash = calculateHash(buffer, stringStart, stringEnd);
                this.regularTable.add(buffer, stringStart, stringLength, hash, value);
            }
        }

        public void addRows(TreeMap<String, ResultRow> result) {
            this.longTable.addRows(result);
            this.regularTable.addRows(result);
        }
    }

    /**
     * The hash calculation is inspired by
     * <a href="https://questdb.io/blog/building-faster-hash-table-high-performance-sql-joins/#fastmap-internals">QuestDB FastMap</a>
     */
    private static int calculateHash(byte[] buffer, int startPosition, int endPosition) {
        long hash = 0;

        int position = startPosition;
        for (; position + Long.BYTES <= endPosition; position += Long.BYTES) {
            long value = (long) LONG_VIEW.get(buffer, position);
            hash = hash * RANDOM_PRIME + value;
        }

        if (position + Integer.BYTES <= endPosition) {
            int value = (int) INT_VIEW.get(buffer, position);
            hash = hash * RANDOM_PRIME + value;
            position += Integer.BYTES;
        }

        for (; position <= endPosition; position++) {
            hash = hash * RANDOM_PRIME + buffer[position];
        }
        hash = hash * RANDOM_PRIME;
        return (int) hash ^ (int) (hash >>> 32);
    }

    private static int calculateLongHash(long str) {
        long hash = str * RANDOM_PRIME;
        return (int) hash ^ (int) (hash >>> 32);
    }

    /**
     * This tables stores strings of length <= 8 bytes.
     * Does not store hashes.
     */
    private static final class LongTable {
        private static final int TABLE_CAPACITY = 32768;
        private static final int TABLE_CAPACITY_MASK = TABLE_CAPACITY - 1;
        /**
         * The buckets use 3 longs to store strings and measurements:
         * long 1) station name
         * long 2) sum of measurements
         * long 3) count (int) | min (short) | max (short) <-- packed into one long
         */
        private final long[] buckets = new long[TABLE_CAPACITY * 3];

        int keysCount = 0;

        public void add(long str, short value) {
            int hash = calculateLongHash(str);
            int bucketIdx = hash & TABLE_CAPACITY_MASK;

            long bucketStr = buckets[bucketIdx * 3];
            if (bucketStr == str) {
                updateBucket(bucketIdx, value);
            }
            else if (bucketStr == 0L) {
                createBucket(bucketIdx, str, value);
                keysCount++;
            }
            else {
                addWithProbing(str, value, (bucketIdx + 1) & TABLE_CAPACITY_MASK);
            }
        }

        private void addWithProbing(long str, short value, int bucketIdx) {
            int distance = 1;
            while (true) {
                long bucketStr = buckets[bucketIdx * 3];
                if (bucketStr == str) {
                    updateBucket(bucketIdx, value);
                    break;
                }
                else if (bucketStr == 0L) {
                    createBucket(bucketIdx, str, value);
                    keysCount++;
                    break;
                }
                else {
                    distance++;
                    // A new bucket index is calculated based on quadratic probing https://thenumb.at/Hashtables/#quadratic-probing
                    // Quadratic probing decreases the number of collisions and max probing distance.
                    // Linear:
                    // - capacity 16k, 28.6M collisions, 14-17 max distance
                    // - capacity 32k, 9.5M collisions, 5-7 max distance
                    // Quadratic:
                    // - capacity 16k 25M collisions, 8-10 max distance
                    // - capacity 32k, 9.3M collisions, 4-7 max distance
                    bucketIdx = (bucketIdx + distance) & TABLE_CAPACITY_MASK;
                }
            }
        }

        public void addRows(TreeMap<String, ResultRow> result) {
            for (int bucketIdx = 0; bucketIdx < TABLE_CAPACITY; bucketIdx++) {
                int bucketOffset = bucketIdx * 3;
                long str = buckets[bucketOffset];
                if (str != 0L) {
                    long sum = buckets[bucketOffset + 1];
                    long countMinMax = buckets[bucketOffset + 2];
                    int count = (int) ((countMinMax >> 32));
                    short min = (short) ((countMinMax >> 16) & 0xFFFF);
                    short max = (short) (countMinMax & 0xFFFF);

                    result.compute(longToString(str), (k, resultRow) -> {
                        if (resultRow == null) {
                            return new ResultRow(sum, count, min, max);
                        }
                        else {
                            resultRow.add(sum, count, min, max);
                            return resultRow;
                        }
                    });
                }
            }
        }

        private void createBucket(int bucketIdx, long str, short value) {
            int offset = bucketIdx * 3;
            buckets[offset] = str;
            buckets[offset + 1] = value;
            buckets[offset + 2] = (1L << 32) | ((long) (value & 0xFFFF) << 16) | (long) (value & 0xFFFF);
        }

        private void updateBucket(int bucketIdx, short value) {
            int offset = bucketIdx * 3;
            long sum = buckets[offset + 1];
            buckets[offset + 1] = sum + value;

            long countMinMax = buckets[offset + 2];
            int count = (int) ((countMinMax >> 32));
            short min = (short) ((countMinMax >> 16) & 0xFFFF);
            short max = (short) (countMinMax & 0xFFFF);
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            buckets[offset + 2] = ((long) (count + 1) << 32) | ((long) (min & 0xFFFF) << 16) | (long) (max & 0xFFFF);
        }
    }

    /**
     * An open addressing hash table that stores strings as byte arrays.
     * Stores hashes.
     */
    private static final class RegularTable {
        private static final int TABLE_CAPACITY = 16384;
        private static final int TABLE_CAPACITY_MASK = TABLE_CAPACITY - 1;
        private final Bucket[] buckets = new Bucket[TABLE_CAPACITY];

        int keysCount = 0;

        public void add(byte[] data, int start, int stringLength, int hash, short value) {
            int bucketIdx = hash & TABLE_CAPACITY_MASK;

            var bucket = buckets[bucketIdx];
            if (bucket == null) {
                buckets[bucketIdx] = new Bucket(data, start, stringLength, hash, value);
                keysCount++;
            }
            else if (hash == bucket.hash && bucket.isEqual(data, start, stringLength)) {
                bucket.update(value);
            }
            else {
                addWithProbing(data, start, stringLength, hash, value, (bucketIdx + 1) & TABLE_CAPACITY_MASK);
            }
        }

        private void addWithProbing(byte[] data, int start, int stringLength, int hash, short value, int bucketIdx) {
            int distance = 1;
            while (true) {
                var bucket = buckets[bucketIdx];
                if (bucket == null) {
                    buckets[bucketIdx] = new Bucket(data, start, stringLength, hash, value);
                    keysCount++;
                    break;
                }
                else if (hash == bucket.hash && bucket.isEqual(data, start, stringLength)) {
                    bucket.update(value);
                    break;
                }
                else {
                    distance++;
                    bucketIdx = (bucketIdx + distance) & TABLE_CAPACITY_MASK;
                }
            }
        }

        public void addRows(TreeMap<String, ResultRow> result) {
            for (var bucket : buckets) {
                if (bucket != null) {
                    result.compute(new String(bucket.str, StandardCharsets.UTF_8), (k, resultRow) -> {
                        if (resultRow == null) {
                            return new ResultRow(bucket.sum, bucket.count, bucket.min, bucket.max);
                        }
                        else {
                            resultRow.add(bucket.sum, bucket.count, bucket.min, bucket.max);
                            return resultRow;
                        }
                    });
                }
            }
        }

        private static final class Bucket {
            int hash;
            byte[] str;
            long sum;
            int count;
            short max = Short.MIN_VALUE;
            short min = Short.MAX_VALUE;

            Bucket(byte[] data, int start, int stringLength, int hash, short value) {
                this.str = new byte[stringLength];
                System.arraycopy(data, start, this.str, 0, stringLength);
                this.hash = hash;
                update(value);
            }

            public void update(short value) {
                this.sum += value;
                this.count++;
                if (max < value)
                    max = value;
                if (min > value)
                    min = value;
            }

            public boolean isEqual(byte[] data, int start, int length) {
                if (str.length != length)
                    return false;
                int i = 0;
                for (; i + Long.BYTES < str.length; i += Long.BYTES) {
                    long value1 = (long) LONG_VIEW.get(str, i);
                    long value2 = (long) LONG_VIEW.get(data, start + i);
                    if (value1 != value2)
                        return false;
                }
                if (i + Integer.BYTES < str.length) {
                    int value1 = (int) INT_VIEW.get(str, i);
                    int value2 = (int) INT_VIEW.get(data, start + i);
                    if (value1 != value2)
                        return false;
                    i += Integer.BYTES;
                }
                for (; i < str.length; i++) {
                    if (data[start + i] != str[i])
                        return false;
                }
                return true;
            }
        }
    }

    private static class ResultRow {
        long sum;
        int count;
        short min;
        short max;

        public ResultRow(long sum, int count, short min, short max) {
            this.sum = sum;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public void add(long anotherSum, int anotherCount, short anotherMin, short anotherMax) {
            sum += anotherSum;
            count += anotherCount;
            if (max < anotherMax)
                max = anotherMax;
            if (min > anotherMin)
                min = anotherMin;
        }

        public String toString() {
            return Math.round((double) min) / 10.0 + "/"
                    + Math.round((double) sum / count) / 10.0 + "/"
                    + Math.round((double) max) / 10.0;
        }
    }

    /**
     * A utility class that uses the RandomAccessFile to read at offset.
     * Keeps the in-memory buffer, as well as current offsets in the buffer and the file.
     */
    private static final class BufferedFile implements AutoCloseable {
        private static final int BUFFER_SIZE = 512 * 1024;
        private final byte[] buffer = new byte[BUFFER_SIZE];
        private int bufferLimit = 0;
        private int bufferPosition = 0;
        private final long maxOffset;
        private final RandomAccessFile file;
        private long offset;

        private BufferedFile(long startOffset, long maxOffset) throws FileNotFoundException {
            this.offset = startOffset;
            this.maxOffset = maxOffset;
            this.file = new RandomAccessFile(FILE, "r");
        }

        private void refillBuffer() {
            int remainingBytes = bufferLimit - bufferPosition;
            if (remainingBytes < MAX_LINE_LENGTH) {
                bufferPosition = 0;
                int bytesRead;
                try {
                    file.seek(offset);
                    bytesRead = file.read(buffer, 0, BUFFER_SIZE);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (bytesRead > 0) {
                    bufferLimit = bytesRead;
                }
                else {
                    bufferLimit = 0;
                }
            }
        }

        @Override
        public void close() throws Exception {
            file.close();
        }
    }

}
