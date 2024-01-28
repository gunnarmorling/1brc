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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Simple solution that memory maps the input file, then splits it into one segment per available core and uses
 * sun.misc.Unsafe to directly access the mapped memory. Uses a long at a time when checking for collision.
 * <p>
 * Runs in 0.41s on my Intel i9-13900K
 * Perf stats:
 *     25,286,227,376      cpu_core/cycles/
 *     26,833,723,225      cpu_atom/cycles/
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

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }
        // Calculate input segments.
        int numberOfWorkers = Runtime.getRuntime().availableProcessors();
        final AtomicLong cursor = new AtomicLong();
        final long fileEnd;
        final long fileStart;

        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            fileStart = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, java.lang.foreign.Arena.global()).address();
            cursor.set(fileStart);
            fileEnd = fileStart + fileSize;
        }

        // Parallel processing of segments.
        Thread[] threads = new Thread[numberOfWorkers];
        List<Result>[] allResults = new List[numberOfWorkers];
        for (int i = 0; i < threads.length; ++i) {
            final int index = i;
            threads[i] = new Thread(() -> {
                Result[] resultArray = parseLoop(cursor, fileEnd, fileStart);
                List<Result> results = new ArrayList<>(500);
                for (Result r : resultArray) {
                    if (r != null) {
                        r.calcName();
                        results.add(r);
                    }
                }
                allResults[index] = results;
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

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
        new ProcessBuilder().command(workerCommand).inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start().getInputStream().transferTo(System.out);
    }

    // Accumulate results sequentially for simplicity.
    private static TreeMap<String, Result> accumulateResults(List<Result>[] allResults) {
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

    private static Result findResult(long initialWord, long initialPos, Scanner scanner, Result[] results) {

        Result existingResult;
        long word = initialWord;
        long pos = initialPos;
        long hash;
        long nameAddress = scanner.pos();

        // Search for ';', one long at a time.
        if (pos != 0) {
            pos = Long.numberOfTrailingZeros(pos) >>> 3;
            scanner.add(pos);
            word = mask(word, pos);
            hash = word;

            int index = hashToIndex(hash, results);
            existingResult = results[index];

            if (existingResult != null && existingResult.lastNameLong == word) {
                return existingResult;
            }
            else {
                scanner.setPos(nameAddress + pos);
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
                existingResult = results[index];

                if (existingResult != null && existingResult.lastNameLong == word && existingResult.secondLastNameLong == prevWord) {
                    return existingResult;
                }
                else {
                    scanner.setPos(nameAddress + pos + 8);
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
            }
        }

        // Save length of name for later.
        int nameLength = (int) (scanner.pos() - nameAddress);

        // Final calculation for index into hash table.
        int tableIndex = hashToIndex(hash, results);
        outer: while (true) {
            existingResult = results[tableIndex];
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
                break;
            }
            else {
                // Collision error, try next.
                tableIndex = (tableIndex + 31) & (results.length - 1);
            }
        }
        return existingResult;
    }

    private static long nextNL(long prev) {
        while (true) {
            long currentWord = Scanner.UNSAFE.getLong(prev);
            long pos = findNewLine(currentWord);
            if (pos != 0) {
                prev += Long.numberOfTrailingZeros(pos) >>> 3;
                break;
            }
            else {
                prev += 8;
            }
        }
        return prev;
    }

    private static final int SEGMENT_SIZE = 1024 * 1024 * 2;

    // Main parse loop.
    private static Result[] parseLoop(AtomicLong counter, long fileEnd, long fileStart) {
        Result[] results = new Result[1 << 17];

        while (true) {
            long current = counter.addAndGet(SEGMENT_SIZE) - SEGMENT_SIZE;

            if (current >= fileEnd) {
                return results;
            }

            long segmentEnd = nextNL(Math.min(fileEnd - 1, current + SEGMENT_SIZE));
            long segmentStart;
            if (current == fileStart) {
                segmentStart = current;
            }
            else {
                segmentStart = nextNL(current) + 1;
            }

            long dist = (segmentEnd - segmentStart) / 3;
            long midPoint1 = nextNL(segmentStart + dist);
            long midPoint2 = nextNL(segmentStart + dist + dist);

            Scanner scanner1 = new Scanner(segmentStart, midPoint1);
            Scanner scanner2 = new Scanner(midPoint1 + 1, midPoint2);
            Scanner scanner3 = new Scanner(midPoint2 + 1, segmentEnd);
            while (true) {
                if (!scanner1.hasNext()) {
                    break;
                }
                if (!scanner2.hasNext()) {
                    break;
                }
                if (!scanner3.hasNext()) {
                    break;
                }

                long word1 = scanner1.getLong();
                long word2 = scanner2.getLong();
                long word3 = scanner3.getLong();
                long pos1 = findDelimiter(word1);
                long pos2 = findDelimiter(word2);
                long pos3 = findDelimiter(word3);
                Result existingResult1 = findResult(word1, pos1, scanner1, results);
                Result existingResult2 = findResult(word2, pos2, scanner2, results);
                Result existingResult3 = findResult(word3, pos3, scanner3, results);
                long number1 = scanNumber(scanner1);
                long number2 = scanNumber(scanner2);
                long number3 = scanNumber(scanner3);
                record(existingResult1, number1);
                record(existingResult2, number2);
                record(existingResult3, number3);
            }

            while (scanner1.hasNext()) {
                long word = scanner1.getLong();
                long pos = findDelimiter(word);
                record(findResult(word, pos, scanner1, results), scanNumber(scanner1));
            }

            while (scanner2.hasNext()) {
                long word = scanner2.getLong();
                long pos = findDelimiter(word);
                record(findResult(word, pos, scanner2, results), scanNumber(scanner2));
            }

            while (scanner3.hasNext()) {
                long word = scanner3.getLong();
                long pos = findDelimiter(word);
                record(findResult(word, pos, scanner3, results), scanNumber(scanner3));
            }
        }
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

    private static long findNewLine(long word) {
        long input = word ^ 0x0A0A0A0A0A0A0A0AL;
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