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

import jdk.incubator.vector.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.foreign.ValueLayout.*;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class CalculateAverage_unbounded {
    private static final Path FILE = Path.of("./measurements.txt");
    private static final int MAX_STATION_NAME_LEN = 100;
    private static final int MAX_UNIQUE_STATIONS = 10000;

    // this is *really* expensive
    private static final OfInt BIG_ENDIAN_INT = JAVA_INT_UNALIGNED.withOrder(BIG_ENDIAN);
    private static final VectorSpecies<Byte> LINE_SCAN_SPECIES = ByteVector.SPECIES_256;
    private static final int LINE_SCAN_LEN = LINE_SCAN_SPECIES.length();
    private static final VectorSpecies<Integer> NAME_HASH_SPECIES = IntVector.SPECIES_256;
    private static final VectorSpecies<Short> HASH_LOOKUP_SPECIES = ShortVector.SPECIES_256;
    private static final VectorSpecies<Long> ACCUMULATOR_SPECIES = LongVector.SPECIES_256;

    private static final int CHUNK_SIZE = 16 * 1024 * 1024;

    // Arbitrarily chosen primes
    private static final int[] HASH_PRIMES = { 661, 1663, 2293, 3581, 5449, 5953, 6311, 6841, 7573, 7669, 7703, 7789, 7901, 8887, 8581, 8831 };
    private static final byte[] PREFIX_MASK = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, };
    private static final int[] DIGIT_MULTIPLIERS = {
            0, 10, 1, 1,
            100, 10, 1, 1,
            0, -10, 1, -1,
            -100, -10, 1, -1,
    };
    private static final int[] DIGIT_MASK = {
            0x000fff0f,
            0x0f0fff0f,
            0x000fff0f,
            0x0f0fff0f,
    };
    private static final int[] DIGIT_FLIPS = { 0, 0, -1, -1 };

    record Segment(long start, int len) {
    }

    static class StationStat {
        long count;
        long totalTemp;
        int min;
        int max;

        StationStat(long count, long totalTemp, int min, int max) {
            this.count = count;
            this.totalTemp = totalTemp;
            this.min = min;
            this.max = max;
        }

        StationStat merge(StationStat other) {
            this.count += other.count;
            this.totalTemp += other.totalTemp;
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            return this;
        }

        @Override
        public String toString() {
            return STR."\{min/10.0}/\{Math.round(1.0 * totalTemp / count)/10.0}/\{max/10.0}";
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        long fileSize = Files.size(FILE);
        int lastChunkSize = (int) Math.min(200, fileSize);
        int numSegments = (int) (fileSize / CHUNK_SIZE + 10);

        var segments = new ArrayBlockingQueue<Segment>((int) (fileSize / CHUNK_SIZE + 10));
        for (long i = 0; i < fileSize - lastChunkSize; i += CHUNK_SIZE) {
            segments.put(new Segment(i, (int) Math.min(CHUNK_SIZE, fileSize - i - lastChunkSize)));
        }

        int numThreads = Runtime.getRuntime().availableProcessors();
        var results = new ArrayBlockingQueue<Map<String, StationStat>>(numThreads);
        var toMerge = new ArrayList<Map<String, StationStat>>(numThreads + 1);
        try (var ch = FileChannel.open(FILE, StandardOpenOption.READ); var arena = Arena.ofConfined()) {
            var threads = IntStream.range(0, numThreads).mapToObj((ignored) -> new ProcessorThread(segments, ch, results::add)).toList();
            threads.forEach(Thread::start);

            // Process last piece without OOB
            int margin = lastChunkSize < fileSize ? 1 : 0;
            var mem = ch.map(FileChannel.MapMode.READ_ONLY, fileSize - lastChunkSize - margin, lastChunkSize + margin, arena);
            slowProcessChunk(mem, margin, lastChunkSize, toMerge::add);

            for (var thread : threads) {
                thread.join();
            }
        }

        results.drainTo(toMerge);
        var merged = toMerge.stream().reduce((a, b) -> {
            b.forEach((k, v) -> a.merge(k, v, StationStat::merge));
            return a;
        }).get();
        printResult(merged);
    }

    // Simple implementation for the end - so we don't need to worry about reading past the end of the file
    private static void slowProcessChunk(MemorySegment mem, int startPos, int endPos, Consumer<Map<String, StationStat>> report) {
        int index = scanForStartPos(mem, startPos);
        byte[] nameBuf = new byte[MAX_STATION_NAME_LEN];
        while (index < endPos) {
            int nameLen = 0;
            while (mem.get(JAVA_BYTE, index) != ';') {
                nameBuf[nameLen++] = mem.get(JAVA_BYTE, index);
                index++;
            }
            var name = new String(nameBuf, 0, nameLen);
            index++;
            StringBuilder numStr = new StringBuilder(5);
            while (mem.get(JAVA_BYTE, index) != '\n') {
                if (mem.get(JAVA_BYTE, index) != '.') {
                    numStr.append((char) mem.get(JAVA_BYTE, index));
                }
                index++;
            }
            index++;
            int num = Integer.parseInt(numStr.toString());
            var entry = new HashMap<String, StationStat>(1);
            entry.put(name, new StationStat(1, num, num, num));
            report.accept(entry);
        }
    }

    static class ProcessorThread extends Thread {

        static final int NUM_BUCKETS = 1024;
        static final int BUCKET_MASK = 0x3ff;
        static final int BUCKET_SIZE = 16;

        // n-way hash table state
        // 16 buckets, then 16 name pointers
        private final short[] hashTable = new short[2 * BUCKET_SIZE * NUM_BUCKETS];
        // storage of station name keys for hash collision check
        private final byte[] nameTable = new byte[MAX_UNIQUE_STATIONS * (MAX_STATION_NAME_LEN + 1)];
        // values for the hash key stable
        private final short[] stationIndexes = new short[BUCKET_SIZE * NUM_BUCKETS];
        private final int[] nextNamePos = { 0 };
        private final int[] nextStationIndex = { 0 };

        // Accumulator for (10s, 1s, (count*-2), .1s) per station
        private final long[] accumulators = new long[4 * MAX_UNIQUE_STATIONS];
        // min and max per station
        private final int[] minMax = new int[2 * MAX_UNIQUE_STATIONS];

        private final Queue<Segment> segments;
        private final FileChannel channel;
        private final Consumer<Map<String, StationStat>> report;

        ProcessorThread(Queue<Segment> segments, FileChannel channel, Consumer<Map<String, StationStat>> report) {
            this.segments = segments;
            this.channel = channel;
            this.report = report;
            for (int i = 0; i < minMax.length; i += 2) {
                minMax[i] = Integer.MAX_VALUE;
                minMax[i + 1] = Integer.MIN_VALUE;
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    var segment = segments.poll();
                    if (segment == null) {
                        break;
                    }
                    int startMargin = segment.start == 0 ? 0 : 1;
                    int endMargin = 64;
                    try (var arena = Arena.ofConfined()) {
                        var mem = channel.map(FileChannel.MapMode.READ_ONLY, segment.start - startMargin, segment.len + endMargin + startMargin, arena);
                        processChunk(mem, startMargin, segment.len + startMargin, hashTable, nameTable, stationIndexes, minMax, accumulators, nextNamePos, nextStationIndex);
                    }
                }
                report.accept(decodeResult(hashTable, nameTable, stationIndexes, accumulators, minMax));
            } catch (IOException e) {
                System.err.println(STR."I/O Exception: \{e}");
                throw new RuntimeException(e);
            }
        }

        private static void processChunk(MemorySegment mem, int startPos, int endPos, short[] hashTable, byte[] nameTable, short[] stationIndexes, int[] minMax,
                                         long[] accumulators, int[] nextNamePos, int[] nextStationIndex) {
            int index = scanForStartPos(mem, startPos);
            var primeVec = IntVector.fromArray(NAME_HASH_SPECIES, HASH_PRIMES, 0);
            while (index < endPos) {
                var lineVec = ByteVector.fromMemorySegment(LINE_SCAN_SPECIES, mem, index, ByteOrder.LITTLE_ENDIAN);
                int numPos = lineVec.eq((byte) ';').firstTrue() + 1;
                int nlPos = 0;
                int stationIndex;
                if (numPos != LINE_SCAN_LEN + 1) {
                    // Fast path, station name fits in one SIMD register
                    nlPos = lineVec.eq((byte) '\n').firstTrue();
                    if (nlPos == LINE_SCAN_LEN) {
                        while (mem.get(JAVA_BYTE, index + nlPos) != '\n') {
                            nlPos++;
                        }
                    }
                    var nameVec = lineVec.and(ByteVector.fromArray(LINE_SCAN_SPECIES, PREFIX_MASK, 33 - numPos));
                    int nameHash = nameVec.reinterpretAsInts().mul(primeVec).reduceLanes(VectorOperators.ADD);

                    stationIndex = fastLookupHash(nameHash, nameVec, hashTable, nameTable, stationIndexes, nextNamePos, nextStationIndex);
                }
                else {
                    // Slow path, station name larger than SIMD register
                    while (mem.get(JAVA_BYTE, index + numPos - 1) != ';')
                        numPos++;
                    while (mem.get(JAVA_BYTE, index + nlPos) != '\n')
                        nlPos++;

                    int nameHash = lineVec.reinterpretAsInts().mul(primeVec).reduceLanes(VectorOperators.ADD);
                    for (int i = LINE_SCAN_LEN; i < numPos - 1; i++) {
                        nameHash = nameHash * 33 + mem.get(JAVA_BYTE, index + i);
                    }
                    stationIndex = lookupHash(nameHash, mem.asSlice(index, numPos - 1), hashTable, nameTable, stationIndexes, nextNamePos, nextStationIndex);
                }
                boolean isNegative = mem.get(JAVA_BYTE, index + numPos) == '-';
                // format; 0: 9.9, 1: 99.9, 2: -9.9, 3: -99.9
                int numFormat = nlPos - numPos - 3 + (isNegative ? 1 : 0);

                // accumulate sums for mean
                var numPartsVec = ByteVector.fromMemorySegment(ByteVector.SPECIES_128, mem, index + nlPos - 4, ByteOrder.LITTLE_ENDIAN)
                        .sub((byte) '0')
                        .convert(VectorOperators.B2I, 0);
                var multiplyVec = IntVector.fromArray(IntVector.SPECIES_128, DIGIT_MULTIPLIERS, 4 * numFormat);
                var toAdd = numPartsVec.mul(multiplyVec).castShape(ACCUMULATOR_SPECIES, 0);
                var acc = LongVector.fromArray(ACCUMULATOR_SPECIES, accumulators, 4 * stationIndex);
                acc.add(toAdd).intoArray(accumulators, 4 * stationIndex);

                // record min/max
                // encode ASCII value to sortable format without parsing
                int encoded = (mem.get(BIG_ENDIAN_INT, index + nlPos - 4) & DIGIT_MASK[numFormat]) ^ DIGIT_FLIPS[numFormat];
                minMax[2 * stationIndex] = Math.min(minMax[2 * stationIndex], encoded);
                minMax[2 * stationIndex + 1] = Math.max(minMax[2 * stationIndex + 1], encoded);

                index += nlPos + 1;
            }
        }

        // Look up name that fits in a vector
        private static int fastLookupHash(int nameHash, ByteVector nameVec, short[] hashTable, byte[] nameTable, short[] stationIndexes, int[] nextNamePos,
                                          int[] nextStationIndex) {
            int bucketIdx = nameHash & BUCKET_MASK;
            short shortHash = (short) (0x8000 | (nameHash >> 16));

            // Look up the station name to find the index
            while (true) {
                var bucketVec = ShortVector.fromArray(HASH_LOOKUP_SPECIES, hashTable, 2 * BUCKET_SIZE * bucketIdx);
                var bucketPos = bucketVec.eq(shortHash).firstTrue();
                if (bucketPos != HASH_LOOKUP_SPECIES.length()) {
                    int slotNamePos = 32 * Short.toUnsignedInt(hashTable[2 * BUCKET_SIZE * bucketIdx + BUCKET_SIZE + bucketPos]);
                    var slotNameVec = ByteVector.fromArray(LINE_SCAN_SPECIES, nameTable, slotNamePos);
                    if (nameVec.eq(slotNameVec).allTrue()) {
                        // Hit
                        return stationIndexes[BUCKET_SIZE * bucketIdx + bucketPos];
                    }
                    else {
                        bucketPos = handleHashCollision(shortHash, bucketIdx, MemorySegment.ofArray(nameVec.toArray()), hashTable, nameTable);
                        if (bucketPos != -1) {
                            return stationIndexes[BUCKET_SIZE * bucketIdx + bucketPos];
                        }
                    }
                }
                var emptyPos = bucketVec.eq((short) 0).firstTrue();
                if (emptyPos != HASH_LOOKUP_SPECIES.length()) {
                    // Miss, insert
                    int stationIndex = nextStationIndex[0]++;
                    nameVec.intoArray(nameTable, nextNamePos[0]);
                    hashTable[2 * BUCKET_SIZE * bucketIdx + emptyPos] = shortHash;
                    hashTable[2 * BUCKET_SIZE * bucketIdx + BUCKET_SIZE + emptyPos] = (short) (nextNamePos[0] / 32);
                    stationIndexes[BUCKET_SIZE * bucketIdx + emptyPos] = (short) stationIndex;
                    nextNamePos[0] += nameVec.length();
                    return stationIndex;
                }
                // Try next bucket
                bucketIdx = (bucketIdx + 1) & BUCKET_MASK;
            }
        }

        // Look up long name
        private static int lookupHash(int nameHash, MemorySegment nameSeg, short[] hashTable, byte[] nameTable, short[] stationIndexes, int[] nextNamePos,
                                      int[] nextStationIndex) {
            int bucketIdx = nameHash & BUCKET_MASK;
            short shortHash = (short) (0x8000 | (nameHash >> 16));

            // Look up the station name to find the index
            while (true) {
                var bucketVec = ShortVector.fromArray(HASH_LOOKUP_SPECIES, hashTable, 2 * BUCKET_SIZE * bucketIdx);
                var bucketPos = bucketVec.eq(shortHash).firstTrue();
                if (bucketPos != HASH_LOOKUP_SPECIES.length()) {
                    int slotNamePos = 32 * Short.toUnsignedInt(hashTable[2 * BUCKET_SIZE * bucketIdx + BUCKET_SIZE + bucketPos]);
                    boolean match = true;
                    for (int i = 0; i < nameSeg.byteSize(); i++) {
                        if (nameSeg.get(JAVA_BYTE, i) != nameTable[slotNamePos + i]) {
                            match = false;
                        }
                    }
                    match = match && nameTable[slotNamePos + (int) nameSeg.byteSize()] == '\0';
                    if (match) {
                        // Hit
                        return stationIndexes[BUCKET_SIZE * bucketIdx + bucketPos];
                    }
                    else {
                        bucketPos = handleHashCollision(shortHash, bucketIdx, nameSeg, hashTable, nameTable);
                        if (bucketPos != -1) {
                            return stationIndexes[BUCKET_SIZE * bucketIdx + bucketPos];
                        }
                    }
                }
                var emptyPos = bucketVec.eq((short) 0).firstTrue();
                if (emptyPos != HASH_LOOKUP_SPECIES.length()) {
                    // Miss, insert
                    int stationIndex = nextStationIndex[0]++;
                    hashTable[2 * BUCKET_SIZE * bucketIdx + emptyPos] = shortHash;
                    hashTable[2 * BUCKET_SIZE * bucketIdx + BUCKET_SIZE + emptyPos] = (short) (nextNamePos[0] / 32);
                    stationIndexes[BUCKET_SIZE * bucketIdx + emptyPos] = (short) stationIndex;
                    for (int i = 0; i < nameSeg.byteSize(); i++) {
                        nameTable[nextNamePos[0]++] = nameSeg.get(JAVA_BYTE, i);
                    }
                    nameTable[nextNamePos[0]++] = '\0';
                    while (nextNamePos[0] % 32 != 0)
                        nextNamePos[0]++;
                    return stationIndex;
                }
                // Try next bucket
                bucketIdx = (bucketIdx + 1) & BUCKET_MASK;
            }
        }

        private static int handleHashCollision(short shortHash, int bucketIdx, MemorySegment nameSeg, short[] hashTable, byte[] nameTable) {
            for (int i = 0; i < BUCKET_SIZE; i++) {
                if (hashTable[2 * BUCKET_SIZE * bucketIdx + i] == shortHash) {
                    int namePos = 32 * Short.toUnsignedInt(hashTable[2 * BUCKET_SIZE * bucketIdx + BUCKET_SIZE + i]);
                    if (Arrays.equals(nameSeg.toArray(JAVA_BYTE), Arrays.copyOfRange(nameTable, namePos, namePos + (int) nameSeg.byteSize()))
                            && nameTable[namePos + (int) nameSeg.byteSize()] == '\0') {
                        return i;
                    }
                }
            }
            return -1;
        }
    }

    // Find next record
    private static int scanForStartPos(MemorySegment mem, int startPos) {
        if (startPos == 0) {
            return startPos;
        }
        while (mem.get(JAVA_BYTE, startPos - 1) != '\n') {
            startPos++;
        }
        return startPos;
    }

    // Decode the accumulator values to StationStats
    private static Map<String, StationStat> decodeResult(short[] hashTable, byte[] nameTable, short[] stationIndexes, long[] accumulators, int[] minMax) {
        var result = new HashMap<String, StationStat>(MAX_UNIQUE_STATIONS);
        for (int i = 0; i < hashTable.length; i += 32) {
            for (int j = 0; j < 16; j++) {
                if (hashTable[i + j] != 0) {
                    int namePos = 32 * Short.toUnsignedInt(hashTable[i + j + 16]);
                    int nameLen = 1;
                    while (nameTable[namePos + nameLen] != '\0') {
                        nameLen++;
                    }
                    int stationIdx = stationIndexes[i / 2 + j];
                    // Number of '-2' valued dots seen
                    long count = accumulators[4 * stationIdx + 2] / -2;
                    long total = accumulators[4 * stationIdx];
                    total += accumulators[4 * stationIdx + 1];
                    total += accumulators[4 * stationIdx + 3];
                    int min = decodeInteger(minMax[2 * stationIdx]);
                    int max = decodeInteger(minMax[2 * stationIdx + 1]);
                    result.put(new String(nameTable, namePos, nameLen), new StationStat(count, total, min, max));
                }
            }
        }
        return result;
    }

    private static int decodeInteger(int encoded) {
        int mask = encoded >> 31;
        int orig = (encoded ^ mask) & 0x7fffffff;
        int val = (orig & 0xff) + ((orig >> 16) & 0xff) * 10 + ((orig >> 24) & 0xff) * 100;
        return val * (mask | 1);
    }

    private static void printResult(Map<String, StationStat> stats) {
        System.out.print("{");
        System.out.print(
            stats.keySet().stream().sorted()
                    .map(key -> {
                        var s = stats.get(key);
                        return STR."\{key}=\{s}";
                    })
                    .collect(Collectors.joining(", "))
        );
        System.out.println("}");
    }
}
