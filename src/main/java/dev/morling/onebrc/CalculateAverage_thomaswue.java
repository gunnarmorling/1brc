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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Simple solution that memory maps the input file, then splits it into one segment per available core and uses
 * sun.misc.Unsafe to directly access the mapped memory. Uses a long at a time when checking for collision.
 * <p>
 * Runs in 0.45s on my Intel i9-13900K
 * Perf stats:
 *     33,128,048,027      cpu_core/cycles/
 *     38,668,037,020      cpu_atom/cycles/
 */
public class CalculateAverage_thomaswue {
    private static final String FILE = "./measurements.txt";
    private static final int MIN_TEMP = -999;
    private static final int MAX_TEMP = 999;

    // Holding the current result for a single city.
    private static class Result {
        long lastNameLong, secondLastNameLong;
        long min, max;
        long sum;
        int count;
        long[] name;
        String nameAsString;

        private Result() {
            this.min = MAX_TEMP;
            this.max = MIN_TEMP;
        }

        public String toString() {
            return round(((double) min) / 10.0) + "/" + round((((double) sum) / 10.0) / count) + "/" + round(((double) max) / 10.0);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // Accumulate another result into this one.
        private void add(Result other) {
            if (other.min < min) {
                min = other.min;
            }
            if (other.max > max) {
                max = other.max;
            }
            sum += other.sum;
            count += other.count;
        }

        public String calcName() {
            if (nameAsString == null) {
                ByteBuffer bb = ByteBuffer.allocate(name.length * Long.BYTES).order(ByteOrder.nativeOrder());
                bb.asLongBuffer().put(name);
                byte[] array = bb.array();
                int i = 0;
                while (array[i++] != ';')
                    ;
                nameAsString = new String(array, 0, i - 1, StandardCharsets.UTF_8);
            }
            return nameAsString;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }
        // Calculate input segments.
        int numberOfChunks = Runtime.getRuntime().availableProcessors();
        long[] chunks = getSegments(numberOfChunks);

        // Parallel processing of segments.
        List<List<Result>> allResults = IntStream.range(0, chunks.length - 1).mapToObj(chunkIndex -> parseLoop(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .map(resultArray -> {
                    List<Result> results = new ArrayList<>(500);
                    for (Result r : resultArray) {
                        if (r != null) {
                            r.calcName();
                            results.add(r);
                        }
                    }
                    return results;
                }).parallel().toList();

        // Final output.
        System.out.println(accumulateResults(allResults));
        System.out.close();
    }

    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder()
                .command(workerCommand)
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start()
                .getInputStream()
                .transferTo(System.out);
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
        long pos = findDelimiter(word);

        outermost: while (scanner.hasNext()) {
            long hash;
            long nameAddress;
            while (true) {
                nameAddress = scanner.pos();
                // Search for ';', one long at a time.
                if (pos != 0) {
                    pos = Long.numberOfTrailingZeros(pos) >>> 3;
                    scanner.add(pos);
                    word = mask(word, pos);
                    hash = word;

                    int index = hashToIndex(hash, results);
                    long number = scanNumber(scanner);
                    Result existingResult = results[index];
                    long nextWord = scanner.getLong();
                    long nextPos = findDelimiter(nextWord);

                    if (existingResult != null && existingResult.lastNameLong == word) {
                        word = nextWord;
                        pos = nextPos;
                        record(existingResult, number);
                    }
                    else {
                        scanner.setPos(nameAddress + pos);
                        break;
                    }
                }
                else {
                    scanner.add(8);
                    hash = word;
                    long prevWord = word;
                    word = scanner.getLong();
                    pos = findDelimiter(word);
                    if (pos != 0) {
                        pos = Long.numberOfTrailingZeros(pos) >>> 3;
                        scanner.add(pos);
                        word = mask(word, pos);
                        hash ^= word;
                        int index = hashToIndex(hash, results);
                        long number = scanNumber(scanner);
                        Result existingResult = results[index];
                        long nextWord = scanner.getLong();
                        long nextDelimiterWord = findDelimiter(nextWord);

                        if (existingResult != null && existingResult.lastNameLong == word && existingResult.secondLastNameLong == prevWord) {
                            word = nextWord;
                            pos = nextDelimiterWord;
                            record(existingResult, number);
                        }
                        else {
                            scanner.setPos(nameAddress + pos + 8);
                            break;
                        }
                    }
                    else {
                        scanner.add(8);
                        hash ^= word;
                        while (true) {
                            word = scanner.getLong();
                            pos = findDelimiter(word);
                            if (pos != 0) {
                                pos = Long.numberOfTrailingZeros(pos) >>> 3;
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
                        break;
                    }
                }

                if (!scanner.hasNext()) {
                    break outermost;
                }
            }

            // Save length of name for later.
            int nameLength = (int) (scanner.pos() - nameAddress);
            long number = scanNumber(scanner);

            // Final calculation for index into hash table.
            int tableIndex = hashToIndex(hash, results);
            outer: while (true) {
                Result existingResult = results[tableIndex];
                if (existingResult == null) {
                    existingResult = newEntry(results, nameAddress, tableIndex, nameLength, scanner);
                }
                // Check for collision.
                int i = 0;
                long[] name = existingResult.name;
                for (; i < nameLength + 1 - 8; i += 8) {
                    if (scanner.getLongAt(i, name) != scanner.getLongAt(nameAddress + i)) {
                        tableIndex = (tableIndex + 31) & (results.length - 1);
                        continue outer;
                    }
                }

                int remainingShift = (64 - (nameLength + 1 - i) << 3);
                if (((existingResult.lastNameLong ^ (scanner.getLongAt(nameAddress + i) << remainingShift)) == 0)) {
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

    private static long scanNumber(Scanner scanPtr) {
        scanPtr.add(1);
        long numberWord = scanPtr.getLong();
        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        long number = convertIntoNumber(decimalSepPos, numberWord);
        scanPtr.add((decimalSepPos >>> 3) + 3);
        return number;
    }

    private static void record(Result existingResult, long number) {
        if (number < existingResult.min) {
            existingResult.min = number;
        }
        if (number > existingResult.max) {
            existingResult.max = number;
        }
        existingResult.sum += number;
        existingResult.count++;
    }

    private static int hashToIndex(long hash, Result[] results) {
        long hashAsInt = hash ^ (hash >>> 37) ^ (hash >>> 17);
        return (int) (hashAsInt & (results.length - 1));
    }

    private static long mask(long word, long pos) {
        return (word << ((7 - pos) << 3));
    }

    // Special method to convert a number in the ascii number into an int without branches created by Quan Anh Mai.
    private static long convertIntoNumber(int decimalSepPos, long numberWord) {
        int shift = 28 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~numberWord << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii to digit value
        long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;
        // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
        // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
        // 0x000000UU00TTHH00 + 0x00UU00TTHH000000 * 10 + 0xUU00TTHH00000000 * 100
        long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        return (absValue ^ signed) - signed;
    }

    private static long findDelimiter(long word) {
        long input = word ^ 0x3B3B3B3B3B3B3B3BL;
        long tmp = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
        return tmp;
    }

    private static Result newEntry(Result[] results, long nameAddress, int hash, int nameLength, Scanner scanner) {
        Result r = new Result();
        results[hash] = r;
        long[] name = new long[(nameLength / Long.BYTES) + 1];
        int pos = 0;
        int i = 0;
        for (; i < nameLength + 1 - Long.BYTES; i += Long.BYTES) {
            name[pos++] = scanner.getLongAt(nameAddress + i);
        }

        if (pos > 0) {
            r.secondLastNameLong = name[pos - 1];
        }

        int remainingShift = (64 - (nameLength + 1 - i) << 3);
        long lastWord = (scanner.getLongAt(nameAddress + i) << remainingShift);
        r.lastNameLong = lastWord;
        name[pos] = lastWord >> remainingShift;
        r.name = name;
        return r;
    }

    private static long[] getSegments(int numberOfChunks) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            long segmentSize = (fileSize + numberOfChunks - 1) / numberOfChunks;
            long[] chunks = new long[numberOfChunks + 1];
            long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, java.lang.foreign.Arena.global()).address();
            chunks[0] = mappedAddress;
            long endAddress = mappedAddress + fileSize;
            Scanner s = new Scanner(mappedAddress, mappedAddress + fileSize);
            for (int i = 1; i < numberOfChunks; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (chunkAddress < endAddress && (s.getLongAt(chunkAddress++) & 0xFF) != '\n')
                    ;
                chunks[i] = Math.min(chunkAddress, endAddress);
            }
            chunks[numberOfChunks] = endAddress;
            return chunks;
        }
    }

    private static class Scanner {

        private static final sun.misc.Unsafe UNSAFE = initUnsafe();

        private static sun.misc.Unsafe initUnsafe() {
            try {
                java.lang.reflect.Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (sun.misc.Unsafe) theUnsafe.get(sun.misc.Unsafe.class);
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

        void add(long delta) {
            pos += delta;
        }

        long getLong() {
            return UNSAFE.getLong(pos);
        }

        long getLongAt(long pos) {
            return UNSAFE.getLong(pos);
        }

        long getLongAt(long pos, long[] array) {
            return UNSAFE.getLong(array, pos + sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET);
        }

        void setPos(long l) {
            this.pos = l;
        }
    }
}