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

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;

public final class CalculateAverage_JaimePolidura {
    private static final String FILE = "./measurements.txt";
    private static final Unsafe UNSAFE = initUnsafe();
    private static final long SEMICOLON_PATTERN = 0X3B3B3B3B3B3B3B3BL;

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Worker[] workers = createWorkers();

        startWorkers(workers);
        joinWorkers(workers);

        Map<String, Result> results = mergeWorkersResults(workers);
        printResults(results);
    }

    private static void joinWorkers(Worker[] workers) throws InterruptedException {
        for (int i = 0; i < workers.length; i++) {
            workers[i].join();
        }
    }

    private static void startWorkers(Worker[] workers) {
        for (int i = 0; i < workers.length; i++) {
            workers[i].start();
        }
    }

    private static Worker[] createWorkers() throws Exception {
        FileChannel channel = new RandomAccessFile(FILE, "r").getChannel();
        MemorySegment mmappedFile = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.global());

        int nWorkers = channel.size() > 1024 * 1024 ? Runtime.getRuntime().availableProcessors() : 1;
        Worker[] workers = new Worker[nWorkers];
        long quantityPerWorker = Math.floorDiv(channel.size(), nWorkers);
        long quantityLastWorker = quantityPerWorker + (channel.size() % nWorkers);

        for (int i = 0; i < nWorkers; i++) {
            boolean isLastWorker = i == nWorkers - 1;

            long startAddr = mmappedFile.address() + quantityPerWorker * i;
            long endAddr = startAddr + (isLastWorker ? quantityLastWorker : quantityPerWorker);
            workers[i] = new Worker(mmappedFile, channel.size(), startAddr, endAddr);
            workers[i].setPriority(Thread.MAX_PRIORITY);
        }

        return workers;
    }

    private static Map<String, Result> mergeWorkersResults(Worker[] workers) {
        Map<String, Result> mergedResults = new TreeMap<>();

        for (int i = 0; i < workers.length; i++) {
            Worker worker = workers[i];

            for (Result entry : worker.results.entries) {
                if (entry != null) {
                    String name = new String(entry.name, 0, entry.nameLength);
                    Result alreadyExistingResult = mergedResults.get(name);
                    if (alreadyExistingResult != null) {
                        alreadyExistingResult.min = Math.min(alreadyExistingResult.min, entry.min);
                        alreadyExistingResult.max = Math.max(alreadyExistingResult.max, entry.max);
                        alreadyExistingResult.count = alreadyExistingResult.count + entry.count;
                        alreadyExistingResult.sum = alreadyExistingResult.sum + entry.sum;
                    }
                    else {
                        mergedResults.put(name, entry);
                    }
                }
            }
        }

        return mergedResults;
    }

    private static void printResults(Map<String, Result> results) {
        StringBuilder stringBuilder = new StringBuilder(results.size() * 32);
        stringBuilder.append('{');

        for (Map.Entry<String, Result> entry : results.entrySet()) {
            if (stringBuilder.length() > 1) {
                stringBuilder.append(", ");
            }

            Result result = entry.getValue();
            stringBuilder.append(entry.getKey())
                    .append('=')
                    .append(round(((double) result.min) / 10.0))
                    .append('/')
                    .append(round((double) result.sum / (result.count * 10)))
                    .append('/')
                    .append(round(((double) result.max) / 10.0d));

        }

        stringBuilder.append('}');

        System.out.println(stringBuilder);
    }

    static class Worker extends Thread {
        private final byte[] lastParsedNameBytes = new byte[100];
        private int lastParsedNameLength;
        private long lastParsedNameHash;
        private int lastParsedTemperature;

        private final SimpleMap results;
        private final MemorySegment mmappedFile;
        private final long mmappedFileSize;
        private long currentAddr; // Will point to beginning of string
        private long endAddr; // Will point to \n

        public Worker(MemorySegment mmappedFile, long mmappedFileSize, long startAddr, long endAddr) {
            super("Worker[" + startAddr + ", " + endAddr + "]");

            this.mmappedFileSize = mmappedFileSize;
            this.mmappedFile = mmappedFile;
            this.currentAddr = startAddr;
            this.endAddr = endAddr;

            this.results = new SimpleMap(roundUpToPowerOfTwo(1 << 16)); // 2^16
        }

        @Override
        public void run() {
            adjustStartAddr();
            adjustEndAddr();

            if (this.currentAddr >= endAddr) {
                return;
            }

            while (currentAddr < endAddr) {
                parseName();
                parseTemperature();

                this.currentAddr++; // We don't want it to point to \n

                results.put(this.lastParsedNameHash, this.lastParsedNameBytes, this.lastParsedNameLength, this.lastParsedTemperature);
            }
        }

        // Idea from Quan Anh Mai's implementation
        private void parseTemperature() {
            long numberWord = UNSAFE.getLong(currentAddr);

            // The 4th binary digit of the ascii (Starting from left) of a digit is 1 while '.' is 0
            int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
            // 28 = 4 + 8 * 3 (4 bytes is the number of tail zeros in the byte of decimalPos)
            // xxxn.nn- shift: 28 - 28 = 0
            // xxxxxn.n shift: 28 - 12 = 16
            // xxxxn.nn shift: 28 - 20 = 8
            int shift = 28 - decimalSepPos;

            // Negative in ASCII: 00101101 2D. In ascii every digit starts with hex digit 3
            // So in order to know if a number is positive, we simpy need the first bit of the 2º half
            // If signed is 0 the number is positive. If it is negative signed will be -1.
            long signed = (~numberWord << 59) >> 63;

            // If signed is 0 (positive), designMask will be 0xFFFFFFFFFFFFFFFF (-256)
            // If signed is -1, all 1s (negative), designMask will be 0xFFFFFFFFFFFFFF00 (-1)
            long designMask = ~(signed & 0xFF);

            // Align the number to a fixed position
            // (x represents any non-related character, _ represents 0x00, n represents the actual digit and - negative)
            // xxxn.nn- -> xxxn.nn-
            // xxxxxn.n -> xxxn.n__
            // xxxxn.nn -> xxxn.nn_
            long numberAligned = (numberWord & designMask) << shift;

            // We convert ascii representation to number value
            long numberConvertedFromAscii = numberAligned & 0x0F000F0F00L;

            // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
            // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
            // 0x000000UU00TTHH00 +
            // 0x00UU00TTHH000000 * 10 +
            // 0xUU00TTHH00000000 * 100
            // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
            // This results in our value lies in the bit 32 to 41 of this product
            // That was close :)
            long absValue = ((numberConvertedFromAscii * 0x640a0001) >>> 32) & 0x3FF;

            long signedValue = (absValue ^ signed) - signed;

            this.currentAddr += (((decimalSepPos - 4) / 8) + 2);

            this.lastParsedTemperature = (int) signedValue;
        }

        // I first saw this idea in Artsiom Korzun's implementation
        private void parseName() {
            this.lastParsedNameHash = 0;

            long totalWordHash = 0;
            int totalWordLength = 0;

            for (;;) {
                long actualWord = UNSAFE.getLong(currentAddr + totalWordLength);
                long hasSemicolon = hasByte(actualWord, SEMICOLON_PATTERN);

                if (hasSemicolon != 0) {
                    int actualLength = Long.numberOfTrailingZeros(hasSemicolon) >> 3;
                    if (actualLength == 0) {
                        actualWord = 0;
                    }

                    actualWord = mask(actualWord, actualLength);

                    UNSAFE.putLong(this.lastParsedNameBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + totalWordLength, actualWord);

                    totalWordHash ^= actualWord;
                    totalWordLength += actualLength;

                    this.lastParsedNameLength = totalWordLength;
                    this.lastParsedNameHash = totalWordHash;
                    this.currentAddr += totalWordLength + 1; // +1 Because we don't want to point to ';'

                    break;
                }
                else {
                    UNSAFE.putLong(this.lastParsedNameBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + totalWordLength, actualWord);

                    totalWordLength += 8;
                    totalWordHash ^= actualWord;
                }
            }
        }

        // Removes "garbage" of a word byte
        private long mask(long word, int length) {
            int shift = (8 - length) * 8;
            return (word << shift) >> shift;
        }

        private long hasByte(long word, long pattern) {
            long patternMatch = word ^ pattern;
            return (patternMatch - 0x0101010101010101L) & (~patternMatch & 0x8080808080808080L);
        }

        private void adjustStartAddr() {
            if (currentAddr == this.mmappedFile.address()) {
                return;
            }

            while (UNSAFE.getByte(currentAddr) != '\n' && currentAddr != endAddr) {
                currentAddr++;
            }

            currentAddr++; // We want it to point to the first character instead of \n
        }

        private void adjustEndAddr() {
            long endAddressMmappedFile = mmappedFile.address() + mmappedFileSize;
            if (endAddr >= endAddressMmappedFile) {
                return;
            }

            while (UNSAFE.getByte(endAddr) != '\n' && endAddr != endAddressMmappedFile) {
                endAddr++;
            }
        }
    }

    static class SimpleMap {
        private final Result[] entries;
        private final long size;

        public SimpleMap(int size) {
            this.entries = new Result[size];
            this.size = size;
        }

        public void put(long hashToPut, byte[] nameToPut, int nameLength, int valueToPut) {
            int index = toIndex(hashToPut);

            for (;;) {
                Result actualEntry = entries[index];

                if (actualEntry == null) {
                    byte[] nameToPutCopy = new byte[nameLength];
                    UNSAFE.copyMemory(nameToPut, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameToPutCopy, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);

                    entries[index] = new Result(hashToPut, nameToPutCopy, nameLength, valueToPut,
                            valueToPut, valueToPut, 1);
                    return;
                }
                if (actualEntry.isSameName(nameToPut, nameLength)) {
                    actualEntry.min = Math.min(actualEntry.min, valueToPut);
                    actualEntry.max = Math.max(actualEntry.max, valueToPut);
                    actualEntry.count++;
                    actualEntry.sum = actualEntry.sum + valueToPut;
                    return;
                }

                index = toIndex(index + 31);
            }
        }

        private int toIndex(long hash) {
            return (int) (((hash >> 32) ^ ((int) hash)) & (this.size - 1));
        }
    }

    static class Result {
        public byte[] name;
        public int nameLength;
        public int max;
        public int min;
        public int sum;
        public int count;
        public long hash;

        public Result(long hash, byte[] name, int nameLength, int max, int min, int sum, int occ) {
            this.nameLength = nameLength;
            this.count = occ;
            this.hash = hash;
            this.name = name;
            this.max = max;
            this.min = min;
            this.sum = sum;
        }

        public boolean isSameName(byte[] otherNameBytes, int otherNameLength) {
            return this.nameLength == otherNameLength && isSameNameBytes(otherNameBytes);
        }

        private boolean isSameNameBytes(byte[] otherNameBytes) {
            for (int i = 0; i < this.nameLength; i += 8) {
                long thisNameBytesAsLong = UNSAFE.getLong(this.name, Unsafe.ARRAY_BYTE_BASE_OFFSET + i);
                long otherNameBytesAsLong = UNSAFE.getLong(otherNameBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + i);

                int isPositiveAsInt = (((8 - nameLength + i) >> 31) & 1) ^ 0x01;
                int shift = ((8 - nameLength + i) * isPositiveAsInt) * 8;
                otherNameBytesAsLong = (otherNameBytesAsLong << shift) >>> shift;

                if (thisNameBytesAsLong != otherNameBytesAsLong) {
                    return false;
                }
            }

            return true;
        }
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static int roundUpToPowerOfTwo(int number) {
        if (number <= 0) {
            return 1;
        }

        number--;
        number |= number >> 1;
        number |= number >> 2;
        number |= number >> 4;
        number |= number >> 8;
        number |= number >> 16;

        return number + 1;
    }
}
