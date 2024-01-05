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

public class CalculateAverage_thomaswue {
    private static final String FILE = "./measurements.txt";

    // Holding the current result for a single city.
    private static class Result {
        int min;
        int max;
        long sum;
        int count;
        final long nameAddress;
        final int nameLength;

        private Result(long nameAddress, int nameLength, int value) {
            this.nameAddress = nameAddress;
            this.nameLength = nameLength;
            this.min = value;
            this.max = value;
            this.sum = value;
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
            Result[] results = new Result[1 << 14];
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

    static boolean unsafeEquals(long aStart, long aLength, long bStart, long bLength) {
        if (aLength != bLength) {
            return false;
        }
        for (int i = 0; i < aLength; ++i) {
            if (UNSAFE.getByte(aStart + i) != UNSAFE.getByte(bStart + i)) {
                return false;
            }
        }
        return true;
    }

    private static void parseLoop(long chunkStart, long chunkEnd, Result[] results, HashMap<String, Result> cities) {
        long scanPtr = chunkStart;
        byte b;
        while (scanPtr < chunkEnd) {
            long nameAddress = scanPtr;

            int hash = UNSAFE.getByte(scanPtr++);
            while ((b = UNSAFE.getByte(scanPtr++)) != ';') {
                hash += b;
                hash += hash << 10;
                hash ^= hash >> 6;
            }

            int nameLength = (int) (scanPtr - 1 - nameAddress);
            hash = hash & (results.length - 1);

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
                Result existingResult = results[hash];
                if (existingResult == null) {
                    Result r = new Result(nameAddress, nameLength, number);
                    results[hash] = r;
                    byte[] bytes = new byte[nameLength];
                    UNSAFE.copyMemory(null, nameAddress, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);
                    cities.put(new String(bytes, StandardCharsets.UTF_8), r);
                    break;
                }
                else if (unsafeEquals(existingResult.nameAddress, existingResult.nameLength, nameAddress, nameLength)) {
                    existingResult.min = Math.min(existingResult.min, number);
                    existingResult.max = Math.max(existingResult.max, number);
                    existingResult.sum += number;
                    existingResult.count++;
                    break;
                }
                else {
                    // Collision error, try next.
                    hash = (hash + 1) & (results.length - 1);
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
            long mappedAddress = fileChannel.map(MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = mappedAddress;
            for (int i = 1; i < numberOfChunks; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (UNSAFE.getByte(chunkAddress++) != '\n') {
                    // nop
                }
                chunks[i] = chunkAddress;
            }
            chunks[numberOfChunks] = mappedAddress + fileSize;
            return chunks;
        }
    }
}
