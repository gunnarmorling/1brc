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
import java.util.Map;
import java.util.TreeMap;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class CalculateAverage_merykitty {
    private static final String FILE = "./measurements.txt";
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 32
            ? ByteVector.SPECIES_256
            : ByteVector.SPECIES_128;
    private static final ValueLayout.OfLong JAVA_LONG_LT = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final long KEY_MAX_SIZE = 100;

    private static class Aggregator {
        private int keySize;
        private long min = Integer.MAX_VALUE;
        private long max = Integer.MIN_VALUE;
        private long sum;
        private long count;

        public String toString() {
            return round(min / 10.) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    // An open-address map that is specialized for this task
    private static class PoorManMap {

        // 100-byte key + 4-byte hash + 4-byte size +
        // 2-byte min + 2-byte max + 8-byte sum + 8-byte count
        private static final int KEY_SIZE = 128;

        // There is an assumption that map size <= 10000;
        private static final int CAPACITY = 1 << 17;
        private static final int BUCKET_MASK = CAPACITY - 1;

        byte[] keyData;
        Aggregator[] nodes;

        PoorManMap() {
            this.keyData = new byte[CAPACITY * KEY_SIZE];
            this.nodes = new Aggregator[CAPACITY];
        }

        void observe(Aggregator node, long value) {
            if (node.min > value) {
                node.min = value;
            }
            if (node.max < value) {
                node.max = value;
            }
            node.sum += value;
            node.count++;
        }

        Aggregator indexSimple(MemorySegment data, long offset, int size) {
            int x;
            int y;
            if (size >= Integer.BYTES) {
                x = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
                y = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset + size - Integer.BYTES);
            }
            else {
                x = data.get(ValueLayout.JAVA_BYTE, offset);
                y = data.get(ValueLayout.JAVA_BYTE, offset + size - Byte.BYTES);
            }
            int hash = hash(x, y);
            int bucket = hash & BUCKET_MASK;
            for (;; bucket = (bucket + 1) & BUCKET_MASK) {
                var node = this.nodes[bucket];
                if (node == null) {
                    return insertInto(bucket, data, offset, size);
                }
                else if (keyEqualScalar(bucket, data, offset, size)) {
                    return node;
                }
            }
        }

        Aggregator insertInto(int bucket, MemorySegment data, long offset, int size) {
            var node = new Aggregator();
            node.keySize = size;
            this.nodes[bucket] = node;
            MemorySegment.copy(data, offset, MemorySegment.ofArray(this.keyData), (long) bucket * KEY_SIZE, size + 1);
            return node;
        }

        void mergeInto(Map<String, Aggregator> target) {
            for (int i = 0; i < CAPACITY; i++) {
                var node = this.nodes[i];
                if (node == null) {
                    continue;
                }

                String key = new String(this.keyData, i * KEY_SIZE, node.keySize, StandardCharsets.UTF_8);
                target.compute(key, (k, v) -> {
                    if (v == null) {
                        v = new Aggregator();
                    }

                    v.min = Math.min(v.min, node.min);
                    v.max = Math.max(v.max, node.max);
                    v.sum += node.sum;
                    v.count += node.count;
                    return v;
                });
            }
        }

        static int hash(int x, int y) {
            int seed = 0x9E3779B9;
            int rotate = 5;
            return (Integer.rotateLeft(x * seed, rotate) ^ y) * seed; // FxHash
        }

        private boolean keyEqualScalar(int bucket, MemorySegment data, long offset, int size) {
            if (this.nodes[bucket].keySize != size) {
                return false;
            }

            // Be simple
            for (int i = 0; i < size; i++) {
                int c1 = this.keyData[bucket * KEY_SIZE + i];
                int c2 = data.get(ValueLayout.JAVA_BYTE, offset + i);
                if (c1 != c2) {
                    return false;
                }
            }
            return true;
        }
    }

    // Parse a number that may/may not contain a minus sign followed by a decimal with
    // 1 - 2 digits to the left and 1 digits to the right of the separator to a
    // fix-precision format. It returns the offset of the next line (presumably followed
    // the final digit and a '\n')
    private static long parseDataPoint(PoorManMap aggrMap, Aggregator node, MemorySegment data, long offset) {
        long word = data.get(JAVA_LONG_LT, offset);
        // The 4th binary digit of the ascii of a digit is 1 while
        // that of the '.' is 0. This finds the decimal separator
        // The value can be 12, 20, 28
        int decimalSepPos = Long.numberOfTrailingZeros(~word & 0x10101000);
        int shift = 28 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~word << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii code
        // to actual digit value in each byte
        long digits = ((word & designMask) << shift) & 0x0F000F0F00L;

        // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
        // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
        // 0x000000UU00TTHH00 +
        // 0x00UU00TTHH000000 * 10 +
        // 0xUU00TTHH00000000 * 100
        // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
        // This results in our value lies in the bit 32 to 41 of this product
        // That was close :)
        long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        long value = (absValue ^ signed) - signed;
        aggrMap.observe(node, value);
        return offset + (decimalSepPos >>> 3) + 3;
    }

    // Tail processing version of the above, do not over-fetch and be simple
    private static long parseDataPointSimple(PoorManMap aggrMap, Aggregator node, MemorySegment data, long offset) {
        int value = 0;
        boolean negative = false;
        if (data.get(ValueLayout.JAVA_BYTE, offset) == '-') {
            negative = true;
            offset++;
        }
        for (;; offset++) {
            int c = data.get(ValueLayout.JAVA_BYTE, offset);
            if (c == '.') {
                c = data.get(ValueLayout.JAVA_BYTE, offset + 1);
                value = value * 10 + (c - '0');
                offset += 3;
                break;
            }

            value = value * 10 + (c - '0');
        }
        value = negative ? -value : value;
        aggrMap.observe(node, value);
        return offset;
    }

    // An iteration of the main parse loop, parse a line starting from offset.
    // This requires offset to be the start of the line and there is spare space so
    // that we have relative freedom in processing
    // It returns the offset of the next line that it needs processing
    private static long iterate(PoorManMap aggrMap, MemorySegment data, long offset) {
        var line = ByteVector.fromMemorySegment(BYTE_SPECIES, data, offset, ByteOrder.nativeOrder());

        // Find the delimiter ';'
        long semicolons = line.compare(VectorOperators.EQ, ';').toLong();

        // If we cannot find the delimiter in the vector, that means the key is
        // longer than the vector, fall back to scalar processing
        if (semicolons == 0) {
            int keySize = BYTE_SPECIES.length();
            while (data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';') {
                keySize++;
            }
            var node = aggrMap.indexSimple(data, offset, keySize);
            return parseDataPoint(aggrMap, node, data, offset + 1 + keySize);
        }

        // We inline the searching of the value in the hash map
        int keySize = Long.numberOfTrailingZeros(semicolons);
        int x;
        int y;
        if (keySize >= Integer.BYTES) {
            x = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            y = data.get(ValueLayout.JAVA_INT_UNALIGNED, offset + keySize - Integer.BYTES);
        }
        else {
            x = data.get(ValueLayout.JAVA_BYTE, offset);
            y = data.get(ValueLayout.JAVA_BYTE, offset + keySize - Byte.BYTES);
        }
        int hash = PoorManMap.hash(x, y);
        int bucket = hash & PoorManMap.BUCKET_MASK;
        Aggregator node;
        for (;; bucket = (bucket + 1) & PoorManMap.BUCKET_MASK) {
            node = aggrMap.nodes[bucket];
            if (node == null) {
                node = aggrMap.insertInto(bucket, data, offset, keySize);
                break;
            }
            if (node.keySize != keySize) {
                continue;
            }

            var nodeKey = ByteVector.fromArray(BYTE_SPECIES, aggrMap.keyData, bucket * PoorManMap.KEY_SIZE);
            long eqMask = line.compare(VectorOperators.EQ, nodeKey).toLong();
            long validMask = semicolons ^ (semicolons - 1);
            if ((eqMask & validMask) == validMask) {
                break;
            }
        }

        return parseDataPoint(aggrMap, node, data, offset + keySize + 1);
    }

    private static long findOffset(MemorySegment data, long offset, long limit) {
        if (offset == 0) {
            return offset;
        }

        offset--;
        while (offset < limit) {
            if (data.get(ValueLayout.JAVA_BYTE, offset++) == '\n') {
                break;
            }
        }
        return offset;
    }

    // Process all lines that start in [offset, limit)
    private static PoorManMap processFile(MemorySegment data, long offset, long limit) {
        var aggrMap = new PoorManMap();
        if (offset == limit) {
            return aggrMap;
        }
        int batches = 2;
        long batchSize = Math.ceilDiv(limit - offset, batches);
        long offset0 = offset;
        long offset1 = offset + batchSize;
        long limit0 = Math.min(offset1, limit);
        long limit1 = limit;

        // Find the start of a new line
        offset0 = findOffset(data, offset0, limit0);
        offset1 = findOffset(data, offset1, limit1);

        long mainLoopMinWidth = Math.max(BYTE_SPECIES.vectorByteSize(), KEY_MAX_SIZE + 1 + Long.BYTES);
        if (limit1 - offset1 < mainLoopMinWidth) {
            offset = findOffset(data, offset, limit);
            while (offset < limit - mainLoopMinWidth) {
                offset = iterate(aggrMap, data, offset);
            }
        }
        else {
            while (true) {
                boolean finish = false;
                if (offset0 < limit0) {
                    offset0 = iterate(aggrMap, data, offset0);
                }
                else {
                    finish = true;
                }
                if (offset1 < limit1 - mainLoopMinWidth) {
                    offset1 = iterate(aggrMap, data, offset1);
                }
                else {
                    if (finish) {
                        break;
                    }
                }
            }
            offset = offset1;
        }

        // Now we are at the tail, just be simple
        while (offset < limit) {
            int keySize = 0;
            while (data.get(ValueLayout.JAVA_BYTE, offset + keySize) != ';') {
                keySize++;
            }
            var node = aggrMap.indexSimple(data, offset, keySize);
            offset = parseDataPointSimple(aggrMap, node, data, offset + 1 + keySize);
        }

        return aggrMap;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        int processorCnt = Runtime.getRuntime().availableProcessors();
        var res = new TreeMap<String, Aggregator>();
        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
                var arena = Arena.ofShared()) {
            var data = file.map(MapMode.READ_ONLY, 0, file.size(), arena);
            long chunkSize = Math.ceilDiv(data.byteSize(), processorCnt);
            var threadList = new Thread[processorCnt];
            var resultList = new PoorManMap[processorCnt];
            for (int i = 0; i < processorCnt; i++) {
                int index = i;
                long offset = i * chunkSize;
                long limit = Math.min((i + 1) * chunkSize, data.byteSize());
                var thread = new Thread(() -> resultList[index] = processFile(data, offset, limit));
                threadList[index] = thread;
                thread.start();
            }
            for (var thread : threadList) {
                thread.join();
            }

            // Collect the results
            for (var aggrMap : resultList) {
                aggrMap.mergeInto(res);
            }
        }

        System.out.println(res);
    }
}
