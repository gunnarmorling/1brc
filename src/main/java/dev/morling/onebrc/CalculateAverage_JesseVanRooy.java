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
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.IntStream;

//Disclaimer: The idea from the segmentation into #core amount of chunks came from previously submitted solutions.
public class CalculateAverage_JesseVanRooy {

    private static final String FILE = "./measurements.txt";

    private static final ValueLayout.OfByte DATA_LAYOUT = ValueLayout.JAVA_BYTE;

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

    public static class Result {
        long nameStart;
        long nameSize;
        String name;
        int min;
        int max;
        long sum;
        int count;

        double min() {
            return min / 10.0;
        }

        double max() {
            return max / 10.0;
        }

        double mean() {
            return (sum / 10.0) / count;
        }
    }

    public static class ThreadResult {
        Result[] results;
    }

    static final int MAP_SIZE = 16384;
    static final int MAP_MASK = MAP_SIZE - 1;
    static final int VALUE_CAPACITY = 10000;

    static void process(MemorySegment memorySegment, ThreadResult threadResult) {
        // initialize hash table
        final int[] keys = new int[MAP_SIZE];
        Arrays.fill(keys, -1);
        final Result[] values = new Result[MAP_SIZE];

        // pre-create the result objects
        final Result[] preCreatedResults = new Result[VALUE_CAPACITY];
        int usedPreCreatedResults = 0;
        for (int i = 0; i < VALUE_CAPACITY; i++)
            preCreatedResults[i] = new Result();

        // load address info
        final long size = memorySegment.byteSize();
        final long address = memorySegment.address();
        final long end = address + size;

        for (long index = address; index < end;) {
            final long nameStart = index;

            byte next = UNSAFE.getByte(index);

            // hash the city name
            int hash = 0;
            while (next != ';') {
                hash = (hash * 33) + next;

                index++;
                next = UNSAFE.getByte(index);
            }

            final long nameEnd = index;

            // skip the separator
            index++;
            next = UNSAFE.getByte(index);

            // check for negative
            boolean negative = next == '-';
            if (negative) {
                index++;
                next = UNSAFE.getByte(index);
            }

            // count the temperature
            int temperature = next - '0';
            index++;
            next = UNSAFE.getByte(index);

            if (next != '.') {
                temperature = (temperature * 10) + (next - '0');
                index++;
            }

            // skip the .
            index++;
            next = UNSAFE.getByte(index);

            // add the last digit to temperature
            temperature = (temperature * 10) + (next - '0');
            index++;

            // negate the temperature if needed
            if (negative) {
                temperature = -temperature;
            }

            // skip the newline
            index++;

            // insert into map
            for (int i = hash; i < hash + MAP_SIZE; i++) {
                int mapIndex = i & MAP_MASK;
                if (keys[mapIndex] == -1) {
                    Result result = preCreatedResults[usedPreCreatedResults++];
                    result.nameStart = nameStart;
                    result.nameSize = nameEnd - nameStart;
                    result.min = temperature;
                    result.max = temperature;
                    result.sum = temperature;
                    result.count = 1;

                    keys[mapIndex] = hash;
                    values[mapIndex] = result;
                    break;
                }
                if (keys[mapIndex] == hash) {
                    Result result = values[mapIndex];
                    result.min = Math.min(result.min, temperature);
                    result.max = Math.max(result.max, temperature);
                    result.sum += temperature;
                    result.count++;
                    break;
                }
            }
        }

        threadResult.results = Arrays.stream(values).filter(Objects::nonNull).toArray(Result[]::new);

        for (Result result : threadResult.results) {
            result.name = new String(memorySegment.asSlice(result.nameStart - address, result.nameSize).toArray(DATA_LAYOUT));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int numberOfChunks = Runtime.getRuntime().availableProcessors();

        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {

            long fileSize = fileChannel.size();
            MemorySegment allData = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());

            long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            long[] segmentBounds = new long[numberOfChunks + 1];

            segmentBounds[0] = 0;
            for (int i = 1; i < numberOfChunks; i++) {
                long chunkAddress = i * segmentSize;
                while (chunkAddress < fileSize && allData.getAtIndex(DATA_LAYOUT, chunkAddress++) != '\n') {
                }
                segmentBounds[i] = Math.min(chunkAddress, fileSize);
            }
            segmentBounds[numberOfChunks] = fileSize;

            ThreadResult[] threadResults = IntStream.range(0, numberOfChunks)
                    .parallel()
                    .mapToObj(i -> {
                        long size = segmentBounds[i + 1] - segmentBounds[i];
                        long offset = segmentBounds[i];
                        MemorySegment segment = allData.asSlice(offset, size);
                        ThreadResult result = new ThreadResult();
                        process(segment, result);
                        return result;
                    })
                    .toArray(ThreadResult[]::new);

            HashMap<String, Result> combinedResults = new HashMap<>(1024);

            for (int i = 0; i < numberOfChunks; i++) {
                for (Result result : threadResults[i].results) {
                    if (!combinedResults.containsKey(result.name)) {
                        Result newResult = new Result();
                        newResult.name = result.name;
                        newResult.min = result.min;
                        newResult.max = result.max;
                        newResult.sum = result.sum;
                        newResult.count = result.count;
                        combinedResults.put(result.name, newResult);
                    }
                    else {
                        Result existingResult = combinedResults.get(result.name);
                        existingResult.min = Math.min(existingResult.min, result.min);
                        existingResult.max = Math.max(existingResult.max, result.max);
                        existingResult.sum += result.sum;
                        existingResult.count += result.count;
                    }
                }
            }

            Result[] sortedResults = combinedResults.values().toArray(Result[]::new);
            Arrays.sort(sortedResults, Comparator.comparing(result -> result.name));

            System.out.print("{");

            for (int i = 0; i < sortedResults.length; i++) {
                Result sortedResult = sortedResults[i];
                if (i != 0) {
                    System.out.print(", ");
                }
                System.out.printf(Locale.US, "%s=%.1f/%.1f/%.1f", sortedResult.name, sortedResult.min(), sortedResult.mean(), sortedResult.max());
            }

            System.out.printf("}\n");
        }
    }
}
