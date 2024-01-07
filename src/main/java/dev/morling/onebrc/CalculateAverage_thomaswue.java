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
import java.util.*;
import java.util.stream.IntStream;

/**
 * Simple solution that memory maps the input file, then splits it into one segment per available core and uses
 * sun.misc.Unsafe to directly access the mapped memory.
 *
 * Runs in 0.92s on my Intel i9-13900K
 * Perf stats:
 *     65,004,666,383      cpu_core/cycles/
 *     71,141,249,972      cpu_atom/cycles/
 */
public class CalculateAverage_thomaswue {
    private static final String FILE = "./measurements.txt";

    // Holding the current result for a single city.
    private static class Result {
        short min;
        short max;
        long sum;
        int count;
        final long nameAddress;

        private Result(long nameAddress, int value) {
            this.nameAddress = nameAddress;
            this.min = (short) value;
            this.max = (short) value;
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
            min = (short) Math.min(min, other.min);
            max = (short) Math.max(max, other.max);
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
            Result[] results = new Result[1 << 18];
            parseLoop(chunks[chunkIndex], chunks[chunkIndex + 1], results, cities);
            return cities;
        }).parallel().toList();

        // Accumulate results sequentially.
        HashMap<String, Result> result = allResults.getFirst();
        for (int i = 1; i < allResults.size(); ++i) {
            for (Map.Entry<String, Result> entry : allResults.get(i).entrySet()) {
                Result current = result.putIfAbsent(entry.getKey(), entry.getValue());
                if (current != null) {
                    current.add(entry.getValue());
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
        while (scanPtr < chunkEnd) {
            long nameAddress = scanPtr;
            int hash = 0;

            // Skip first letter.
            scanPtr++;

            // Scan for ';' delimiter, always 4 bytes at a time.
            while (true) {
                int nextVal = UNSAFE.getInt(scanPtr);
                if ((nextVal & 0x3B) == 0x3B) {
                    scanPtr++;
                    break;
                }
                else if ((nextVal & 0x3B00) == 0x3B00) {
                    scanPtr += 2;
                    hash = hash ^ (nextVal & 0xFF);
                    break;
                }
                else if ((nextVal & 0x3B0000) == 0x3B0000) {
                    scanPtr += 3;
                    hash = hash ^ (nextVal & 0xFFFF);
                    break;
                }
                else if (((nextVal & 0x3B000000) == 0x3B000000)) {
                    scanPtr += 4;
                    hash = hash ^ (nextVal & 0xFFFFFF);
                    break;
                }
                scanPtr += 4;
                hash = hash ^ nextVal;
            }

            // Save length of name for later.
            int nameLength = (int) (scanPtr - nameAddress - 1);

            // Parse number.
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

            // Final calculation for index into hash table.
            int tableIndex = (((hash ^ (hash >>> 18)) & (results.length - 1)));
            while (true) {
                Result existingResult = results[tableIndex];
                if (existingResult == null) {
                    newEntry(results, cities, nameAddress, number, tableIndex, nameLength);
                    break;
                }
                else {
                    // Check for collision.
                    boolean result = true;
                    int i = 0;
                    if ((long) nameLength >= 8) {
                        if (UNSAFE.getLong(existingResult.nameAddress) != UNSAFE.getLong(nameAddress)) {
                            result = false;
                        }
                        else {
                            i += 8;
                        }
                    }
                    else if ((long) nameLength >= 4) {
                        if (UNSAFE.getInt(existingResult.nameAddress) != UNSAFE.getInt(nameAddress)) {
                            result = false;
                        }
                        else {
                            i += 4;
                        }
                    }
                    if (result) {
                        for (; i < (long) nameLength; ++i) {
                            if (UNSAFE.getByte(existingResult.nameAddress + i) != UNSAFE.getByte(nameAddress + i)) {
                                result = false;
                                break;
                            }
                        }
                    }
                    if (result) {
                        existingResult.min = (short) Math.min(existingResult.min, number);
                        existingResult.max = (short) Math.max(existingResult.max, number);
                        existingResult.sum += number;
                        existingResult.count++;
                        break;
                    }
                    else {
                        // Collision error, try next.
                        tableIndex = (tableIndex + 1) & (results.length - 1);
                    }
                }
            }

            // Skip new line.
            scanPtr++;
        }
    }

    private static void newEntry(Result[] results, HashMap<String, Result> cities, long nameAddress, int number, int hash, int nameLength) {
        Result r = new Result(nameAddress, number);
        results[hash] = r;
        byte[] bytes = new byte[nameLength];
        UNSAFE.copyMemory(null, nameAddress, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);
        cities.put(new String(bytes, StandardCharsets.UTF_8), r);
    }

    private static long[] getSegments(int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            long[] chunks = new long[numberOfChunks + 1];
            long mappedAddress = fileChannel.map(MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
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
