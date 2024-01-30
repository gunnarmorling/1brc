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

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.util.Arrays.asList;

public class CalculateAverage_mtopolnik {
    private static final Unsafe UNSAFE = unsafe();
    private static final int MAX_NAME_LEN = 100;
    private static final int STATS_TABLE_SIZE = 1 << 16;
    private static final int TABLE_INDEX_MASK = STATS_TABLE_SIZE - 1;
    private static final String MEASUREMENTS_TXT = "measurements.txt";

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equals("--worker")) {
            calculate();
            System.out.close();
            return;
        }
        var curProcInfo = ProcessHandle.current().info();
        var cmdLine = new ArrayList<String>();
        cmdLine.add(curProcInfo.command().get());
        cmdLine.addAll(asList(curProcInfo.arguments().get()));
        cmdLine.add("--worker");
        var process = new ProcessBuilder()
                .command(cmdLine)
                .inheritIO().redirectOutput(PIPE)
                .start()
                .getInputStream().transferTo(System.out);

    }

    static void calculate() throws Exception {
        final File file = new File(MEASUREMENTS_TXT);
        final long length = file.length();
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final var results = new StationStats[chunkCount][];
        final var chunkStartOffsets = new long[chunkCount];
        try (var raf = new RandomAccessFile(file, "r")) {
            final var inputBase = raf.getChannel().map(MapMode.READ_ONLY, 0, length, Arena.global()).address();
            for (int i = 1; i < chunkStartOffsets.length; i++) {
                var start = length * i / chunkStartOffsets.length;
                raf.seek(start);
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }
            var threads = new Thread[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(inputBase + chunkStart, inputBase + chunkLimit, results, i));
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        }
        mergeSortAndPrint(results);
    }

    private static class ChunkProcessor implements Runnable {
        private static final int CACHELINE_SIZE = 64;

        private final long inputBase;
        private final long inputSize;
        private final StationStats[][] results;
        private final int myIndex;

        private StatsAccessor stats;

        ChunkProcessor(long chunkStart, long chunkLimit, StationStats[][] results, int myIndex) {
            this.inputBase = chunkStart;
            this.inputSize = chunkLimit - chunkStart;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            try (Arena confinedArena = Arena.ofConfined()) {
                long totalAllocated = 0;
                String threadName = Thread.currentThread().getName();
                long statsByteSize = STATS_TABLE_SIZE * StatsAccessor.SIZEOF;
                var diagnosticString = String.format("Thread %s needs %,d bytes", threadName, statsByteSize);
                try {
                    stats = new StatsAccessor(confinedArena.allocate(statsByteSize, CACHELINE_SIZE));
                }
                catch (OutOfMemoryError e) {
                    System.err.print(diagnosticString);
                    throw e;
                }
                processChunk();
                exportResults();
            }
        }

        private void processChunk() {
            final long inputSize = this.inputSize;
            final long inputBase = this.inputBase;
            long cursor = 0;
            long lastNameWord;
            while (cursor < inputSize) {
                long nameStartAddress = inputBase + cursor;
                long nameWord0 = UNSAFE.getLong(nameStartAddress);
                long nameWord1 = 0;
                long matchBits = semicolonMatchBits(nameWord0);
                long hash;
                int nameLen;
                int temperature;
                if (matchBits != 0) {
                    nameLen = nameLen(matchBits);
                    nameWord0 = maskWord(nameWord0, matchBits);
                    cursor += nameLen;
                    long tempWord = UNSAFE.getLong(inputBase + cursor);
                    int dotPos = dotPos(tempWord);
                    temperature = parseTemperature(tempWord, dotPos);
                    cursor += (dotPos >> 3) + 3;
                    hash = hash(nameWord0);
                    if (stats.gotoName0(hash, nameWord0)) {
                        stats.observe(temperature);
                        continue;
                    }
                    lastNameWord = nameWord0;
                }
                else { // nameLen > 8
                    hash = hash(nameWord0);
                    nameWord1 = UNSAFE.getLong(nameStartAddress + Long.BYTES);
                    matchBits = semicolonMatchBits(nameWord1);
                    if (matchBits != 0) {
                        nameLen = Long.BYTES + nameLen(matchBits);
                        nameWord1 = maskWord(nameWord1, matchBits);
                        cursor += nameLen;
                        long tempWord = UNSAFE.getLong(inputBase + cursor);
                        int dotPos = dotPos(tempWord);
                        temperature = parseTemperature(tempWord, dotPos);
                        cursor += (dotPos >> 3) + 3;
                        if (stats.gotoName1(hash, nameWord0, nameWord1)) {
                            stats.observe(temperature);
                            continue;
                        }
                        lastNameWord = nameWord1;
                    }
                    else { // nameLen > 16
                        nameLen = 2 * Long.BYTES;
                        while (true) {
                            lastNameWord = UNSAFE.getLong(nameStartAddress + nameLen);
                            matchBits = semicolonMatchBits(lastNameWord);
                            if (matchBits != 0) {
                                nameLen += nameLen(matchBits);
                                lastNameWord = maskWord(lastNameWord, matchBits);
                                cursor += nameLen;
                                long tempWord = UNSAFE.getLong(inputBase + cursor);
                                int dotPos = dotPos(tempWord);
                                temperature = parseTemperature(tempWord, dotPos);
                                cursor += (dotPos >> 3) + 3;
                                break;
                            }
                            nameLen += Long.BYTES;
                        }
                    }
                }
                stats.gotoAndObserve(hash, nameStartAddress, nameLen, nameWord0, nameWord1, lastNameWord, temperature);
            }
        }

        private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
        private static final long BROADCAST_0x01 = 0x0101010101010101L;
        private static final long BROADCAST_0x80 = 0x8080808080808080L;

        private static long semicolonMatchBits(long word) {
            long diff = word ^ BROADCAST_SEMICOLON;
            return (diff - BROADCAST_0x01) & (~diff & BROADCAST_0x80);
        }

        // credit: artsiomkorzun
        private static long maskWord(long word, long matchBits) {
            long mask = matchBits ^ (matchBits - 1);
            return word & mask;
        }

        // credit: merykitty
        private static int dotPos(long word) {
            return Long.numberOfTrailingZeros(~word & 0x10101000);
        }

        // credit: merykitty
        private static int parseTemperature(long word, int dotPos) {
            final long signed = (~word << 59) >> 63;
            final long removeSignMask = ~(signed & 0xFF);
            final long digits = ((word & removeSignMask) << (28 - dotPos)) & 0x0F000F0F00L;
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            return (int) ((absValue ^ signed) - signed);
        }

        private static int nameLen(long separator) {
            return (Long.numberOfTrailingZeros(separator) >>> 3) + 1;
        }

        private static long hash(long word) {
            return Long.rotateLeft(word * 0x51_7c_c1_b7_27_22_0a_95L, 17);
        }

        // Copies the results from native memory to Java heap and puts them into the results array.
        private void exportResults() {
            var exportedStats = new ArrayList<StationStats>(10_000);
            for (int i = 0; i < STATS_TABLE_SIZE; i++) {
                stats.gotoIndex(i);
                if (stats.nameLen() == 0) {
                    continue;
                }
                var sum = stats.sum();
                var count = stats.count();
                var min = stats.min();
                var max = stats.max();
                var name = stats.exportNameString();
                var stationStats = new StationStats();
                stationStats.name = name;
                stationStats.sum = sum;
                stationStats.count = count;
                stationStats.min = min;
                stationStats.max = max;
                exportedStats.add(stationStats);
            }
            StationStats[] exported = exportedStats.toArray(new StationStats[0]);
            Arrays.sort(exported);
            results[myIndex] = exported;
        }
    }

    static class StatsAccessor {
        static final int NAME_SLOT_SIZE = 104;
        static final long HASH_OFFSET = 0;
        static final long NAMELEN_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long SUM_OFFSET = NAMELEN_OFFSET + Integer.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Short.BYTES;
        static final long NAME_OFFSET = MAX_OFFSET + Short.BYTES;
        static final long SIZEOF = (NAME_OFFSET + NAME_SLOT_SIZE - 1) / 8 * 8 + 8;

        static final int ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

        private final long address;
        private long slotBase;

        StatsAccessor(MemorySegment memSeg) {
            memSeg.fill((byte) 0);
            this.address = memSeg.address();
        }

        void gotoIndex(int index) {
            slotBase = address + index * SIZEOF;
        }

        private boolean gotoName0(long hash, long nameWord0) {
            gotoIndex((int) (hash & TABLE_INDEX_MASK));
            return hash() == hash && nameWord0() == nameWord0;
        }

        private boolean gotoName1(long hash, long nameWord0, long nameWord1) {
            gotoIndex((int) (hash & TABLE_INDEX_MASK));
            return hash() == hash && nameWord0() == nameWord0 && nameWord1() == nameWord1;
        }

        long hash() {
            return UNSAFE.getLong(slotBase + HASH_OFFSET);
        }

        int nameLen() {
            return UNSAFE.getInt(slotBase + NAMELEN_OFFSET);
        }

        int sum() {
            return UNSAFE.getInt(slotBase + SUM_OFFSET);
        }

        int count() {
            return UNSAFE.getInt(slotBase + COUNT_OFFSET);
        }

        short min() {
            return UNSAFE.getShort(slotBase + MIN_OFFSET);
        }

        short max() {
            return UNSAFE.getShort(slotBase + MAX_OFFSET);
        }

        long nameAddress() {
            return slotBase + NAME_OFFSET;
        }

        long nameWord0() {
            return UNSAFE.getLong(nameAddress());
        }

        long nameWord1() {
            return UNSAFE.getLong(nameAddress() + Long.BYTES);
        }

        String exportNameString() {
            final var bytes = new byte[nameLen() - 1];
            UNSAFE.copyMemory(null, nameAddress(), bytes, ARRAY_BASE_OFFSET, bytes.length);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        void setHash(long hash) {
            UNSAFE.putLong(slotBase + HASH_OFFSET, hash);
        }

        void setNameLen(int nameLen) {
            UNSAFE.putInt(slotBase + NAMELEN_OFFSET, nameLen);
        }

        void setSum(int sum) {
            UNSAFE.putInt(slotBase + SUM_OFFSET, sum);
        }

        void setCount(int count) {
            UNSAFE.putInt(slotBase + COUNT_OFFSET, count);
        }

        void setMin(short min) {
            UNSAFE.putShort(slotBase + MIN_OFFSET, min);
        }

        void setMax(short max) {
            UNSAFE.putShort(slotBase + MAX_OFFSET, max);
        }

        void gotoAndObserve(
                            long hash, long nameStartAddress, int nameLen, long nameWord0, long nameWord1, long lastNameWord,
                            int temperature) {
            int tableIndex = (int) (hash & TABLE_INDEX_MASK);
            while (true) {
                gotoIndex(tableIndex);
                if (hash() == hash && nameLen() == nameLen && nameEquals(
                        nameAddress(), nameStartAddress, nameLen, nameWord0, nameWord1, lastNameWord)) {
                    observe(temperature);
                    break;
                }
                if (nameLen() != 0) {
                    tableIndex = (tableIndex + 1) & TABLE_INDEX_MASK;
                    continue;
                }
                initialize(hash, nameLen, nameStartAddress, temperature);
                break;
            }
        }

        void initialize(long hash, long nameLen, long nameStartAddress, int temperature) {
            setHash(hash);
            setNameLen((int) nameLen);
            setSum(temperature);
            setCount(1);
            setMin((short) temperature);
            setMax((short) temperature);
            UNSAFE.copyMemory(nameStartAddress, nameAddress(), nameLen);
        }

        void observe(int temperature) {
            setSum(sum() + temperature);
            setCount(count() + 1);
            setMin((short) Integer.min(min(), temperature));
            setMax((short) Integer.max(max(), temperature));
        }

        private static boolean nameEquals(
                                          long statsAddr, long inputAddr, long len, long inputWord1, long inputWord2, long lastInputWord) {
            boolean mismatch1 = inputWord1 != UNSAFE.getLong(statsAddr);
            boolean mismatch2 = inputWord2 != UNSAFE.getLong(statsAddr + Long.BYTES);
            if (len <= 2 * Long.BYTES) {
                return !(mismatch1 | mismatch2);
            }
            int i = 2 * Long.BYTES;
            for (; i <= len - Long.BYTES; i += Long.BYTES) {
                if (UNSAFE.getLong(inputAddr + i) != UNSAFE.getLong(statsAddr + i)) {
                    return false;
                }
            }
            return i == len || lastInputWord == UNSAFE.getLong(statsAddr + i);
        }
    }

    private static void mergeSortAndPrint(StationStats[][] results) {
        var onFirst = true;
        System.out.print('{');
        var cursors = new int[results.length];
        var indexOfMin = 0;
        StationStats curr = null;
        int exhaustedCount;
        while (true) {
            exhaustedCount = 0;
            StationStats min = null;
            for (int i = 0; i < cursors.length; i++) {
                if (cursors[i] == results[i].length) {
                    exhaustedCount++;
                    continue;
                }
                StationStats candidate = results[i][cursors[i]];
                if (min == null || min.compareTo(candidate) > 0) {
                    indexOfMin = i;
                    min = candidate;
                }
            }
            if (exhaustedCount == cursors.length) {
                if (!onFirst) {
                    System.out.print(", ");
                }
                System.out.print(curr);
                break;
            }
            cursors[indexOfMin]++;
            if (curr == null) {
                curr = min;
            }
            else if (min.equals(curr)) {
                curr.sum += min.sum;
                curr.count += min.count;
                curr.min = Integer.min(curr.min, min.min);
                curr.max = Integer.max(curr.max, min.max);
            }
            else {
                if (onFirst) {
                    onFirst = false;
                }
                else {
                    System.out.print(", ");
                }
                System.out.print(curr);
                curr = min;
            }
        }
        System.out.println('}');
    }

    static class StationStats implements Comparable<StationStats> {
        String name;
        long sum;
        int count;
        int min;
        int max;

        @Override
        public String toString() {
            return String.format("%s=%.1f/%.1f/%.1f", name, min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }

    private static String longToString(long word) {
        final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
        buf.clear();
        buf.putLong(word);
        return new String(buf.array(), StandardCharsets.UTF_8);
    }
}
