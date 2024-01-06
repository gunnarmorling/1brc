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

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * This implementation is derived from Thomas Wuerthinger's genius pointer implementation.
 * It uses the awesome SIMD hack from Roy van Rijn.
 * 
 * Changes:
 * - Port a int SIMD city name scan, add a fnv64 alike hash function.
 * - Use the full fnv64 hash for name comparison.
 * - Use prime number hash table size.
 * 
 * This saves us from full name comparisons, with an expected
 * 99.99999999999995% correctness ((1-1/(2^64))^10000).
 * 
 * Outcome: 
 * - measurements3: rtreffer=3.9s baseline=2m6s thomaswue=8.5s
 * - measurements:  rtreffer=2.6s baseline=1m47 thomaswue=4.1s royvanrijn=3.2s
 */
public class CalculateAverage_rtreffer {
    private static final String FILE = "./measurements.txt";

    private static final int SEPARATOR_PATTERN = compilePattern((byte) ';');

    private static final int compilePattern(final byte value) {
        return ((int) value << 24) | ((int) value << 16) | ((int) value << 8) | (int) value;
    }

    // Holding the current result for a single city.
    private static class Result {
        int min;
        int max;
        long sum;
        int count;
        final long hash;

        private Result(long hash, int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.hash = hash;
            this.count = 1;
        }

        public String toString() {
            return round(((double) min) / 10.0) + "/" + round((((double) sum) / 10.0) / count) + "/" + round(((double) max) / 10.0);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // Accumulate another result into this one.
        private void add(Result other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }
    }

    public static void main(String[] args) throws IOException {
        // Calculate input segments.
        int numberOfChunks = Runtime.getRuntime().availableProcessors();
        long[] chunks = getSegments(numberOfChunks);

        // Parallel processing of segments.
        List<HashMap<String, Result>> allResults = IntStream.range(0, chunks.length - 1).mapToObj(chunkIndex -> {
            HashMap<String, Result> cities = HashMap.newHashMap(1 << 10);

            // Prime hash table sizes usually perform better, especially if the hash function is not so perfect.
            // 16411 is the next prime number after 1 << 14.
            // 1 << 14 is enough to capture 10'000 stations with an ok-ish load factor.
            Result[] results = new Result[16411];

            parseLoop(chunks[chunkIndex], chunks[chunkIndex + 1], results, cities);
            return cities;
        }).parallel().toList();

        // Accumulate results sequentially.
        HashMap<String, Result> result = allResults.getFirst();
        for (int i = 1; i < allResults.size(); ++i) {
            for (Map.Entry<String, Result> entry : allResults.get(i).entrySet()) {
                Result current = result.get(entry.getKey());
                if (current != null) {
                    current.add(entry.getValue());
                }
                else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // Final output.
        System.out.println(new TreeMap<>(result));
    }

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static void parseLoop(long chunkStart, long chunkEnd, Result[] results, HashMap<String, Result> cities) {
        long scanPtr = chunkStart;
        byte b;
        int hqb, lqb; // (high/low) quad byte

        while (scanPtr < chunkEnd) {
            long nameAddress = scanPtr;

            // see https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function for details
            long fnv64 = 0xcbf29ce484222325L;
            long lo = 0;
            LOOP: while (true) {
                // NOTE: we can always read 4 bytes while scanning the city name.
                // That's because the minimum trailer is ";0.0\n" which is 6 characters long.

                // NOTE: due to endianess the result will be inverse.
                // "ich;" will be loaded as ';', 'h', 'c', 'i'
                // everything works with this inverse order, but the logic looks a bit weird.
                // It also means we are hashing on reversed bytes, but that is not a problem
                // as long as we are consistent.
                hqb = UNSAFE.getInt(scanPtr);
                int match = hqb ^ SEPARATOR_PATTERN;
                long mask = ((match - 0x01010101) & ~match) & 0x80808080;
                if (mask != 0) {
                    // deliminator found
                    final int index = Long.numberOfTrailingZeros(mask) >> 3;
                    scanPtr += index + 1;
                    if (index == 0) {
                        break LOOP;
                    }
                    lo = (long) (hqb << (32 - (index << 3)));
                    break LOOP;
                }
                scanPtr += 4;

                long l = ((long) hqb) << 32;
                lqb = UNSAFE.getInt(scanPtr);
                match = lqb ^ SEPARATOR_PATTERN;
                mask = ((match - 0x01010101) & ~match) & 0x80808080;
                if (mask != 0) {
                    // deliminator found
                    final int index = Long.numberOfTrailingZeros(mask) >> 3;
                    scanPtr += index + 1;
                    if (index == 0) {
                        lo = l;
                        break LOOP;
                    }
                    lo = l + (long) (lqb << (32 - (index << 3)));
                    break LOOP;
                }
                scanPtr += 4;
                fnv64 = (fnv64 ^ (l + lqb)) * 0x100000001b3L;
            }
            fnv64 = (fnv64 ^ lo) * 0x100000001b3L;
            int nameLength = (int) (scanPtr - 1 - nameAddress);

            int index = (int) ((fnv64 & 0x7fffffffffffffffL) % results.length);

            int number;
            byte sign = UNSAFE.getByte(scanPtr++);
            if (sign == '-') {
                number = UNSAFE.getByte(scanPtr++) - '0';
                if ((b = UNSAFE.getByte(scanPtr++)) != '.') {
                    number = number * 10 + (b - '0');
                    scanPtr++;
                }
                number = number * 10 + (UNSAFE.getByte(scanPtr++) - '0');
                number = -number;
            }
            else {
                number = sign - '0';
                if ((b = UNSAFE.getByte(scanPtr++)) != '.') {
                    number = number * 10 + (b - '0');
                    scanPtr++;
                }
                number = number * 10 + (UNSAFE.getByte(scanPtr++) - '0');
            }

            while (true) {
                Result existingResult = results[index];
                if (existingResult == null) {
                    Result r = new Result(fnv64, number);
                    results[index] = r;
                    byte[] bytes = new byte[nameLength];
                    UNSAFE.copyMemory(null, nameAddress, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);
                    cities.put(new String(bytes, StandardCharsets.UTF_8), r);
                    break;
                }
                else if (fnv64 == existingResult.hash) {
                    existingResult.min = Math.min(existingResult.min, number);
                    existingResult.max = Math.max(existingResult.max, number);
                    existingResult.sum += number;
                    existingResult.count++;
                    break;
                }
                else {
                    // Collision error, try next.
                    index = (index + 1) % (results.length);
                }
            }

            // Skip new line.
            scanPtr++;
        }
    }

    private static long[] getSegments(int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            long[] chunks = new long[numberOfChunks + 1];
            MemorySegment segment = fileChannel.map(MapMode.READ_ONLY, 0, fileSize, Arena.global());
            new Thread(() -> {
                segment.load();
            }).start();
            long mappedAddress = segment.address();
            chunks[0] = mappedAddress;
            long endAddress = mappedAddress + fileSize;
            for (int i = 1; i < numberOfChunks; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (chunkAddress < endAddress && UNSAFE.getByte(chunkAddress++) != '\n') {
                    // nop
                }
                chunks[i] = Math.min(chunkAddress, endAddress);
            }
            chunks[numberOfChunks] = endAddress;
            return chunks;
        }
    }
}
