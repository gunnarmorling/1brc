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
 * sun.misc.Unsafe to directly access the mapped memory. Uses a long at a time when checking for collision.
 * <p>
 * Runs in 0.60s on my Intel i9-13900K
 * Perf stats:
 *     34,716,719,245      cpu_core/cycles/
 *     40,776,530,892      cpu_atom/cycles/
 */
public class CalculateAverage_thomaswue {
    private static final String FILE = "./measurements.txt";

    // Holding the current result for a single city.
    private static class Result {
        long lastNameLong, secondLastNameLong, nameAddress;
        int nameLength, remainingShift;
        int min, max, count;
        long sum;

        private Result(long nameAddress) {
            this.nameAddress = nameAddress;
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
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

        public String calcName() {
            return new Scanner(nameAddress, nameAddress + nameLength).getString(nameLength);
        }
    }

    public static void main(String[] args) throws IOException {
        // Calculate input segments.
        int numberOfChunks = Runtime.getRuntime().availableProcessors();
        long[] chunks = getSegments(numberOfChunks);

        // Parallel processing of segments.
        List<List<Result>> allResults = IntStream.range(0, chunks.length - 1).mapToObj(chunkIndex -> parseLoop(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .map(resultArray -> {
                    List<Result> results = new ArrayList<>();
                    for (Result r : resultArray) {
                        if (r != null) {
                            results.add(r);
                        }
                    }
                    return results;
                }).parallel().toList();

        // Final output.
        System.out.println(accumulateResults(allResults));
    }

    // Accumulate results sequentially for simplicity.
    private static TreeMap<String, Result> accumulateResults(List<List<Result>> allResults) {
        TreeMap<String, Result> result = new TreeMap<>();
        for (List<Result> resultArr : allResults) {
            for (Result r : resultArr) {
                String name = r.calcName();
                Result current = result.putIfAbsent(name, r);
                if (current != null) {
                    current.add(r);
                }
            }
        }
        return result;
    }

    // Main parse loop.
    private static Result[] parseLoop(long chunkStart, long chunkEnd) {
        Result[] results = new Result[1 << 17];
        Scanner scanner = new Scanner(chunkStart, chunkEnd);
        long word = scanner.getLong();
        int pos = findDelimiter(word);
        while (scanner.hasNext()) {
            long nameAddress = scanner.pos();
            long hash = 0;

            // Search for ';', one long at a time.
            if (pos != 8) {
                scanner.add(pos);
                word = mask(word, pos);
                hash = word;

                int number = scanNumber(scanner);
                long nextWord = scanner.getLong();
                int nextPos = findDelimiter(nextWord);

                Result existingResult = results[hashToIndex(hash, results)];
                if (existingResult != null && existingResult.lastNameLong == word) {
                    word = nextWord;
                    pos = nextPos;
                    record(existingResult, number);
                    continue;
                }

                scanner.setPos(nameAddress + pos);
            }
            else {
                scanner.add(8);
                hash ^= word;
                long prevWord = word;
                word = scanner.getLong();
                pos = findDelimiter(word);
                if (pos != 8) {
                    scanner.add(pos);
                    word = mask(word, pos);
                    hash ^= word;

                    Result existingResult = results[hashToIndex(hash, results)];
                    if (existingResult != null && existingResult.lastNameLong == word && existingResult.secondLastNameLong == prevWord) {
                        int number = scanNumber(scanner);
                        word = scanner.getLong();
                        pos = findDelimiter(word);
                        record(existingResult, number);
                        continue;
                    }
                }
                else {
                    scanner.add(8);
                    hash ^= word;
                    while (true) {
                        word = scanner.getLong();
                        pos = findDelimiter(word);
                        if (pos != 8) {
                            scanner.add(pos);
                            word = mask(word, pos);
                            hash ^= word;
                            break;
                        }
                        else {
                            scanner.add(8);
                            hash ^= word;
                        }
                    }
                }
            }

            // Save length of name for later.
            int nameLength = (int) (scanner.pos() - nameAddress);
            scanner.add(1);

            long numberWord = scanner.getLong();
            int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
            int number = convertIntoNumber(decimalSepPos, numberWord);
            scanner.add((decimalSepPos >>> 3) + 3);

            // Final calculation for index into hash table.
            int tableIndex = hashToIndex(hash, results);
            outer: while (true) {
                Result existingResult = results[tableIndex];
                if (existingResult == null) {
                    existingResult = newEntry(results, nameAddress, tableIndex, nameLength, scanner);
                }
                // Check for collision.
                int i = 0;
                for (; i < nameLength + 1 - 8; i += 8) {
                    if (scanner.getLongAt(existingResult.nameAddress + i) != scanner.getLongAt(nameAddress + i)) {
                        tableIndex = (tableIndex + 31) & (results.length - 1);
                        continue outer;
                    }
                }
                if (((existingResult.lastNameLong ^ scanner.getLongAt(nameAddress + i)) << existingResult.remainingShift) == 0) {
                    record(existingResult, number);
                    break;
                }
                else {
                    // Collision error, try next.
                    tableIndex = (tableIndex + 31) & (results.length - 1);
                }
            }

            word = scanner.getLong();
            pos = findDelimiter(word);
        }
        return results;
    }

    private static int scanNumber(Scanner scanPtr) {
        scanPtr.add(1);
        long numberWord = scanPtr.getLong();
        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        int number = convertIntoNumber(decimalSepPos, numberWord);
        scanPtr.add((decimalSepPos >>> 3) + 3);
        return number;
    }

    private static void record(Result existingResult, int number) {
        existingResult.min = Math.min(existingResult.min, number);
        existingResult.max = Math.max(existingResult.max, number);
        existingResult.sum += number;
        existingResult.count++;
    }

    private static int hashToIndex(long hash, Result[] results) {
        int hashAsInt = (int) (hash ^ (hash >>> 28));
        int finalHash = (hashAsInt ^ (hashAsInt >>> 15));
        return (finalHash & (results.length - 1));
    }

    private static long mask(long word, int pos) {
        return word & (-1L >>> ((8 - pos - 1) << 3));
    }

    // Special method to convert a number in the specific format into an int value without branches created by
    // Quan Anh Mai.
    private static int convertIntoNumber(int decimalSepPos, long numberWord) {
        int shift = 28 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~numberWord << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii code
        // to actual digit value in each byte
        long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;

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
        return (int) value;
    }

    private static int findDelimiter(long word) {
        long input = word ^ 0x3B3B3B3B3B3B3B3BL;
        long tmp = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
        return Long.numberOfTrailingZeros(tmp) >>> 3;
    }

    private static Result newEntry(Result[] results, long nameAddress, int hash, int nameLength, Scanner scanner) {
        Result r = new Result(nameAddress);
        results[hash] = r;

        int i = 0;
        for (; i < nameLength + 1 - 8; i += 8) {
            r.secondLastNameLong = (scanner.getLongAt(nameAddress + i));
        }
        r.remainingShift = (64 - (nameLength + 1 - i) << 3);
        r.lastNameLong = (scanner.getLongAt(nameAddress + i) & (-1L >>> r.remainingShift));
        r.nameLength = nameLength;
        return r;
    }

    private static long[] getSegments(int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            long[] chunks = new long[numberOfChunks + 1];
            long mappedAddress = fileChannel.map(MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = mappedAddress;
            long endAddress = mappedAddress + fileSize;
            Scanner s = new Scanner(mappedAddress, mappedAddress + fileSize);
            for (int i = 1; i < numberOfChunks; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (chunkAddress < endAddress && (s.getLongAt(chunkAddress++) & 0xFF) != '\n') {
                    // nop
                }
                chunks[i] = Math.min(chunkAddress, endAddress);
            }
            chunks[numberOfChunks] = endAddress;
            return chunks;
        }
    }

    private static class Scanner {

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

        long pos, end;

        public Scanner(long start, long end) {
            this.pos = start;
            this.end = end;
        }

        boolean hasNext() {
            return pos < end;
        }

        long pos() {
            return pos;
        }

        void add(int delta) {
            pos += delta;
        }

        long getLong() {
            return UNSAFE.getLong(pos);
        }

        long getLongAt(long pos) {
            return UNSAFE.getLong(pos);
        }

        public String getString(int nameLength) {
            byte[] bytes = new byte[nameLength];
            UNSAFE.copyMemory(null, pos, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        public void setPos(long l) {
            this.pos = l;
        }
    }
}