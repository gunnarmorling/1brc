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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

//
// This implementation is partially inspired by
//
// - GavinRay97: 1BRC in Kotlin (memory mapping, chunking) | https://github.com/gunnarmorling/1brc/discussions/154
// - dannyvankooten: 1BRC in C (integer parsing, linear probing) | https://github.com/gunnarmorling/1brc/discussions/46
//
public class CalculateAverage_makohn {

    private static final String FILE = "./measurements.txt";

    private static class Measurement implements Comparable<Measurement> {
        final String city;
        int min;
        int max;
        int count = 1;
        int sum;

        Measurement(String city, int val) {
            this.city = city;
            this.min = val;
            this.max = val;
            this.sum = val;
        }

        @Override
        public String toString() {
            return STR."\{city}=\{round(min)}/\{round((1.0 * sum) / count)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }

        @Override
        public int compareTo(Measurement other) {
            return this.city.compareTo(other.city);
        }
    }

    // Convert a given byte array of temperature data to an int value
    // Since the temperate values only have one decimal, we can use integer arithmetic until the end
    //
    // buffer: [..., '-', '1', '9', '.', '7', ...]
    // -------------> offset
    // ............ = s
    //
    // We initialize a "pointer" s with the offset. Depending on whether the first char is a '-' or not, we set the
    // sign and increment the pointer.
    //
    // Then we only have to distinguish between one-digit and two-digit numbers.
    // Depending on that, we set an index for the respective parts of the number.
    //
    private static int toInt(byte[] in, int offset) {
        int sign = 1;
        int s = offset;
        if (in[s] == '-') {
            sign = -1;
            s++;
        }

        if (in[s + 1] == '.')
            return sign * ((in[s] - '0') * 10 + (in[s + 2] - '0'));

        return sign * ((in[s] - '0') * 100 + (in[s + 1] - '0') * 10 + (in[s + 3] - '0'));
    }

    // 10_000 distinct station names as per specification
    // We use the next power of two (2^14 = 16384) to allow for bit-masking our hash (instead of using modulo)
    private static final int MAX_STATIONS = 2 << 14;

    // Twice as big as the maximum number of stations
    private static final int MAP_CAPACITY = MAX_STATIONS * 2;

    // We start at 1 to allow for checking our hash-index map for > 0
    private static final int RES_FIRST_INDEX = 1;

    private static class ResultMap {
        final int[] map = new int[MAP_CAPACITY]; // hash -> index
        final Measurement[] measurements = new Measurement[MAX_STATIONS]; // index -> measurement
        private int lastIndex = 0;

        private void put(int hash, Measurement measurement) {
            lastIndex++;
            measurements[lastIndex] = measurement;
            map[hash] = lastIndex;
        }

        private boolean contains(int hash) {
            return map[hash] > 0;
        }

        private Measurement get(int hash) {
            return measurements[map[hash]];
        }
    }

    // We use linear probing as our hash-collision strategy
    //
    // We use MAP_CAPACITY - 1 as a bitmask to force the hash to be lower than our capacity
    // Let's consider a hash 16390. If our capacity is 2^14 = 16384, the hash is out of bounds.
    //
    // 16390 : 100000000000110
    // 16383 : 011111111111111
    // ....... 000000000000110 = 3
    private static int linearProbe(ResultMap res, String key) {
        var hash = key.hashCode() & (MAP_CAPACITY - 1);
        while (res.map[hash] > 0 && !(res.measurements[res.map[hash]].city.equals(key))) {
            hash = (hash + 1) & (MAP_CAPACITY - 1);
        }
        return hash;
    }

    // Custom Quicksort implementation, seems to be slightly faster than Arrays.sort
    private static void quickSort(Measurement[] arr, int begin, int end) {
        if (begin < end) {
            final var partitionIndex = partition(arr, begin, end);

            quickSort(arr, begin, partitionIndex - 1);
            quickSort(arr, partitionIndex + 1, end);
        }
    }

    private static int partition(Measurement[] arr, int begin, int end) {
        final var pivot = arr[end];
        int i = (begin - 1);

        for (int j = begin; j < end; j++) {
            if (arr[j].compareTo(pivot) <= 0) {
                i++;
                final var tmp = arr[i];
                arr[i] = arr[j];
                arr[j] = tmp;
            }
        }

        final var tmp = arr[i + 1];
        arr[i + 1] = arr[end];
        arr[end] = tmp;

        return i + 1;
    }

    private static Collection<ByteBuffer> getChunks(MemorySegment memory, long chunkSize, long fileSize) {
        final var chunks = new ArrayList<ByteBuffer>();
        var chunkStart = 0L;
        var chunkEnd = 0L;
        while (chunkStart < fileSize) {
            chunkEnd = Math.min((chunkStart + chunkSize), fileSize);
            // starting from the calculated chunkEnd, seek the next newline to get the real chunkEnd
            while (chunkEnd < fileSize && (memory.getAtIndex(ValueLayout.JAVA_BYTE, chunkEnd) & 0xFF) != '\n')
                chunkEnd++;
            // we have found our chunk boundaries, add a slice of memory with these boundaries to our list of chunks
            if (chunkEnd < fileSize)
                chunks.add(memory.asSlice(chunkStart, chunkEnd - chunkStart + 1).asByteBuffer());
            else
                // special case: we are at the end of the file
                chunks.add(memory.asSlice(chunkStart, chunkEnd - chunkStart).asByteBuffer());

            // next chunk
            chunkStart = chunkEnd + 1;
        }
        return chunks;
    }

    // Station name: <= 100 bytes
    // Temperature: <= 5 bytes
    //
    // Semicolon and new line are ignored
    private static final int MAX_BYTES_PER_ROW = 105;

    private static ResultMap processChunk(ByteBuffer chunk) {
        final var map = new ResultMap();
        final var buffer = new byte[MAX_BYTES_PER_ROW];
        var i = 0;
        var delimiter = 0;
        // Process the chunk byte by byte and store each line in buffer
        while (chunk.hasRemaining()) {
            final var c = chunk.get();
            // System.out.println((char) (c & 0xFF));
            switch (c & 0xFF) {
                // Memorize the position of the semicolon, such that we can divide the buffer afterward
                case ';' -> delimiter = i;
                // If we encounter newline, we can do the actual calculations for the current line
                case '\n' -> {
                    final var key = new String(buffer, 0, delimiter, StandardCharsets.UTF_8);
                    final var value = toInt(buffer, delimiter);
                    final var hash = linearProbe(map, key);
                    if (map.contains(hash)) {
                        final var current = map.get(hash);
                        current.min = Math.min(current.min, value);
                        current.max = Math.max(current.max, value);
                        current.count++;
                        current.sum += value;
                    }
                    else {
                        map.put(hash, new Measurement(key, value));
                    }
                    i = 0;
                    delimiter = 0;
                }
                default -> {
                    buffer[i] = c;
                    i++;
                }
            }
        }
        return map;
    }

    // File size is approximately 13 GB, ByteBuffer has a 2 GB limit
    // Chunks should have a maximum size of approximately 13 GB / 8 = 1.625 GB
    private static final int MIN_NUMBER_THREADS = 8;

    public static void main(String[] args) throws Exception {
        final var numProcessors = Math.max(Runtime.getRuntime().availableProcessors(), MIN_NUMBER_THREADS);
        // memory-map the input file
        try (final var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            final var fileSize = channel.size();
            final var chunkSize = (fileSize / numProcessors);
            final var mappedMemory = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            // process the mapped data concurrently in chunks. Each chunk is processed on a dedicated thread
            final var chunks = getChunks(mappedMemory, chunkSize, fileSize);
            final var processed = chunks
                    .parallelStream()
                    .map(CalculateAverage_makohn::processChunk)
                    .collect(Collectors.toList()); // materialize and thus synchronize
            // merge the results, we can initialize with the first result, to avoid redundant probing
            final var first = processed.removeFirst();
            final var res = processed
                    .stream()
                    .reduce(first, (acc, partial) -> {
                        for (int i = RES_FIRST_INDEX; i <= partial.lastIndex; i++) {
                            final var value = partial.measurements[i];
                            final var hash = linearProbe(acc, value.city);
                            if (acc.contains(hash)) {
                                final var cur = acc.get(hash);
                                cur.min = Math.min(cur.min, value.min);
                                cur.max = Math.max(cur.max, value.max);
                                cur.count += value.count;
                                cur.sum += value.sum;
                            }
                            else {
                                acc.put(hash, value);
                            }
                        }
                        return acc;
                    });

            quickSort(res.measurements, RES_FIRST_INDEX, res.lastIndex);
            final var sb = new StringBuilder("{");
            for (int i = RES_FIRST_INDEX; i < res.lastIndex; i++) {
                sb.append(res.measurements[i]).append(',').append(' ');
            }
            sb.append(res.measurements[res.lastIndex]).append('}');
            System.out.println(sb);
        }
    }
}
