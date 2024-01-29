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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Solution based on thomaswue solution, commit:
 * commit d0a28599c293d3afe3291fc3cf169a7b25ae9ae6
 * Author: Thomas Wuerthinger
 * Date:   Sun Jan 21 20:13:48 2024 +0100
 *
 * The goal here was to try to improve the runtime of his 10k
 * solution of: 00:04.516
 * 
 * With Thomas latest changes, his time is probably much better
 * already, and maybe even 1st place for the 10k too.
 * See: https://github.com/gunnarmorling/1brc/pull/606
 * 
 * But as I was already coding something, I'll submit just to
 * see if it will be faster than his *previous* 10k time of
 * 00:04.516
 * 
 * Changes:
 *   It's a similar idea of my previous solution, that if you split
 * the chunks evenly, some threads might finish much faster and
 * stay idle, so:
 *   1) Create more chunks than threads, so the ones that finish first
 * can do something;
 *   2) Decrease chunk sizes as we get closer to the end of the file.
 */
public class CalculateAverage_tivrfoa {
    private static final String FILE = "./measurements.txt";
    private static final int MIN_TEMP = -999;
    private static final int MAX_TEMP = 999;

    // Holding the current result for a single city.
    private static class Result {
        long lastNameLong, secondLastNameLong;
        long[] name;
        int count;
        short min, max;
        long sum;

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
            ByteBuffer bb = ByteBuffer.allocate(name.length * Long.BYTES).order(ByteOrder.nativeOrder());
            bb.asLongBuffer().put(name);
            byte[] array = bb.array();
            int i = 0;
            while (array[i++] != ';')
                ;
            return new String(array, 0, i - 1, StandardCharsets.UTF_8);
        }
    }

    private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();
    private static final AtomicInteger chunkIdx = new AtomicInteger();
    private static long[] chunks;
    private static int numChunks;

    private static final class SolveChunk extends Thread {
        private long chunkStart, chunkEnd;
        private Result[] results = new Result[10_000];
        private Result[] buckets = new Result[1 << 17];
        private int resIdx = 0;

        public SolveChunk(long chunkStart, long chunkEnd) {
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
        }

        @Override
        public void run() {
            parseLoop();
            int chunk = chunkIdx.getAndIncrement();
            if (chunk < numChunks) {
                chunkStart = chunks[chunk];
                chunkEnd = chunks[chunk + 1];
                run();
            }
        }

        private void parseLoop() {
            Scanner scanner = new Scanner(chunkStart, chunkEnd);
            long word = scanner.getLong();
            long pos = findDelimiter(word);
            while (scanner.hasNext()) {
                long nameAddress = scanner.pos();
                long hash = 0;

                // Search for ';', one long at a time.
                if (pos != 0) {
                    pos = Long.numberOfTrailingZeros(pos) >>> 3;
                    scanner.add(pos);
                    word = mask(word, pos);
                    hash = word;

                    int number = scanNumber(scanner);
                    long nextWord = scanner.getLong();
                    long nextPos = findDelimiter(nextWord);

                    Result existingResult = buckets[hashToIndex(hash, buckets)];
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
                    hash = word;
                    long prevWord = word;
                    word = scanner.getLong();
                    pos = findDelimiter(word);
                    if (pos != 0) {
                        pos = Long.numberOfTrailingZeros(pos) >>> 3;
                        scanner.add(pos);
                        word = mask(word, pos);
                        hash ^= word;

                        Result existingResult = buckets[hashToIndex(hash, buckets)];
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
                int number = scanNumber(scanner);

                // Final calculation for index into hash table.
                int tableIndex = hashToIndex(hash, buckets);
                outer: while (true) {
                    Result existingResult = buckets[tableIndex];
                    if (existingResult == null) {
                        existingResult = newEntry(buckets, nameAddress, tableIndex, nameLength, scanner);
                        results[resIdx++] = existingResult;
                    }
                    // Check for collision.
                    int i = 0;
                    int namePos = 0;
                    for (; i < nameLength + 1 - 8; i += 8) {
                        if (namePos >= existingResult.name.length || existingResult.name[namePos++] != scanner.getLongAt(nameAddress + i)) {
                            tableIndex = (tableIndex + 31) & (buckets.length - 1);
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
                        tableIndex = (tableIndex + 31) & (buckets.length - 1);
                    }
                }

                word = scanner.getLong();
                pos = findDelimiter(word);
            }
        }
    }

    private static void mergeIntoFinalMap(TreeMap<String, Result> map, Result[] newResults) {
        for (var r : newResults) {
            if (r == null)
                return;
            Result current = map.putIfAbsent(r.calcName(), r);
            if (current != null) {
                current.add(r);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        boolean runTrick = true;
        for (var arg : args) {
            if (arg.equals("--worker")) {
                runTrick = false;
                break;
            }
        }
        if (runTrick) {
            spawnWorker();
            return;
        }

        chunks = getSegments(NUM_CPUS);
        numChunks = chunks.length - 1;
        final SolveChunk[] threads = new SolveChunk[NUM_CPUS];
        chunkIdx.set(NUM_CPUS);
        for (int i = 0; i < NUM_CPUS; i++) {
            threads[i] = new SolveChunk(chunks[i], chunks[i + 1]);
            threads[i].start();
        }

        TreeMap<String, Result> map = new TreeMap<>();
        for (int i = 0; i < NUM_CPUS; ++i) {
            threads[i].join();
            mergeIntoFinalMap(map, threads[i].results);
        }

        System.out.println(map);
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

    private static int scanNumber(Scanner scanPtr) {
        scanPtr.add(1);
        long numberWord = scanPtr.getLong();
        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        int number = convertIntoNumber(decimalSepPos, numberWord);
        scanPtr.add((decimalSepPos >>> 3) + 3);
        return number;
    }

    private static void record(Result existingResult, int number) {
        if (number < existingResult.min) {
            existingResult.min = (short) number;
        }
        if (number > existingResult.max) {
            existingResult.max = (short) number;
        }
        existingResult.sum += number;
        existingResult.count++;
    }

    private static int hashToIndex(long hash, Result[] results) {
        int hashAsInt = (int) (hash ^ (hash >>> 28));
        int finalHash = (hashAsInt ^ (hashAsInt >>> 17));
        return (finalHash & (results.length - 1));
    }

    private static long mask(long word, long pos) {
        return (word << ((7 - pos) << 3));
    }

    // Special method to convert a number in the ascii number into an int without branches created by Quan Anh Mai.
    private static int convertIntoNumber(int decimalSepPos, long numberWord) {
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
        long value = (absValue ^ signed) - signed;
        return (int) value;
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

    /**
     *  - Split 70% of the file in even chunks for all cpus;
     *  - Create smaller chunks for the remainder of the file.  
     */
    private static long[] getSegments(int cpus) throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            final long fileSize = fileChannel.size();
            final long part1 = (long) (fileSize * 0.7);
            final long part2 = (long) (fileSize * 0.2);
            final long part3 = fileSize - part1 - part2;
            final long bigChunkSize = (part1 - 1) / cpus;
            final long smallChunkSize1 = (part2 - 1) / (cpus * 3);
            final long smallChunkSize2 = (part3 - 1) / (cpus * 3);
            final int numChunks = cpus + cpus * 3 + cpus * 3;
            final long[] sizes = new long[numChunks];
            int l = 0, r = cpus;
            Arrays.fill(sizes, l, r, bigChunkSize);
            l = r;
            r = l + cpus * 3;
            Arrays.fill(sizes, l, r, smallChunkSize1);
            l = r;
            r = l + cpus * 3;
            Arrays.fill(sizes, l, r, smallChunkSize2);
            final long[] chunks = new long[sizes.length + 1];
            final long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, java.lang.foreign.Arena.global()).address();
            chunks[0] = mappedAddress;
            final long endAddress = mappedAddress + fileSize;
            final Scanner s = new Scanner(mappedAddress, mappedAddress + fileSize);
            for (int i = 1, sizeIdx = 0; i < chunks.length - 1; ++i, sizeIdx = (sizeIdx + 1) % sizes.length) {
                long chunkAddress = chunks[i - 1] + sizes[sizeIdx];
                // Align to first row start.
                while (chunkAddress < endAddress && (s.getLongAt(chunkAddress++) & 0xFF) != '\n')
                    ;
                chunks[i] = Math.min(chunkAddress, endAddress);
                // System.err.printf("Chunk size %d\n", chunks[i] - chunks[i - 1]);
            }
            chunks[chunks.length - 1] = endAddress;
            // System.err.printf("Chunk size %d\n", chunks[chunks.length - 1] - chunks[chunks.length - 2]);
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

        void setPos(long l) {
            this.pos = l;
        }
    }
}
