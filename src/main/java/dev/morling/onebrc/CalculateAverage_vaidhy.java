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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

public class CalculateAverage_vaidhy<I, T> {

    private static final class HashEntry {
        private long startAddress;
        private long endAddress;
        private long hash;

        IntSummaryStatistics value;
    }

    private static class PrimitiveHashMap {
        private final HashEntry[] entries;
        private final int twoPow;

        private int size = 0;

        PrimitiveHashMap(int twoPow) {
            this.twoPow = twoPow;
            this.entries = new HashEntry[1 << twoPow];
            for (int i = 0; i < entries.length; i++) {
                this.entries[i] = new HashEntry();
            }
        }

        public HashEntry find(long startAddress, long endAddress, long hash) {
            int len = entries.length;
            int h = Long.hashCode(hash);
            int i = (h ^ (h >> twoPow)) & (len - 1);

            do {
                HashEntry entry = entries[i];
                if (entry.value == null) {
                    entry.startAddress = startAddress;
                    entry.endAddress = endAddress;
                    entry.hash = hash;
                    ++size;
                    return entry;
                }
                if (entry.hash == hash) {
                    long entryLength = entry.endAddress - entry.startAddress;
                    long lookupLength = endAddress - startAddress;
                    if ((entryLength == lookupLength)) {
                        boolean found = compareEntryKeys(startAddress, endAddress, entry);
                        if (found) {
                            return entry;
                        }
                    }
                }
                i++;
                if (i == len) {
                    i = 0;
                }
            } while (i != hash);
            return null;
        }

        private static boolean compareEntryKeys(long startAddress, long endAddress, HashEntry entry) {
            long entryIndex = entry.startAddress;
            long lookupIndex = startAddress;

            for (; (lookupIndex + 7) < endAddress; lookupIndex += 8) {
                if (UNSAFE.getLong(entryIndex) != UNSAFE.getLong(lookupIndex)) {
                    return false;
                }
                entryIndex += 8;
            }
            for (; lookupIndex < endAddress; lookupIndex++) {
                if (UNSAFE.getByte(entryIndex) != UNSAFE.getByte(lookupIndex)) {
                    return false;
                }
                entryIndex++;
            }

            return true;
        }
    }

    private static final String FILE = "./measurements.txt";

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int R3 = 33;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;

    private static final long DEFAULT_SEED = 104729;

    /*
     * @vaidhy
     *
     * Powerful Murmur Hash:
     *
     * To use full MurMur strength:
     *
     * long hash = DEFAULT_SEED
     * for (long value : list) {
     * hash = murmurHash(hash, value);
     * }
     * hash = murmurHashFinalize(hash, list.size())
     *
     * To use a faster hash inspired by murmur:
     *
     * long hash = DEFAULT_SEED
     * for (long value : list) {
     * hash = simpleHash(hash, value);
     * }
     *
     */
    private static long murmurMix64(long hash) {
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);
        return hash;
    }

    private static long murmurHash(long hash, long nextData) {
        hash = hash ^ Long.rotateLeft((nextData * C1), R1) * C2;
        return Long.rotateLeft(hash, R2) * M + N1;
    }

    private static long murmurHashFinalize(long hash, int length) {
        hash ^= length;
        hash = murmurMix64(hash);
        return hash;
    }

    private static long simpleHash(long hash, long nextData) {
        return (hash ^ Long.rotateLeft((nextData * C1), R1)) * C2;
    }

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

    private static final Unsafe UNSAFE = initUnsafe();

    private static int parseDouble(long startAddress, long endAddress) {
        int normalized;
        int length = (int) (endAddress - startAddress);
        if (length == 5) {
            normalized = (UNSAFE.getByte(startAddress + 1) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 2) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 4) ^ 0x30);
            normalized = -normalized;
            return normalized;
        }
        if (length == 3) {
            normalized = (UNSAFE.getByte(startAddress) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 2) ^ 0x30);
            return normalized;
        }

        if (UNSAFE.getByte(startAddress) == '-') {
            normalized = (UNSAFE.getByte(startAddress + 1) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 3) ^ 0x30);
            normalized = -normalized;
            return normalized;
        }
        else {
            normalized = (UNSAFE.getByte(startAddress) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 1) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 3) ^ 0x30);
            return normalized;
        }
    }

    interface MapReduce<I> {

        void process(long keyStartAddress, long keyEndAddress, long hash, int temperature);

        I result();
    }

    private final FileService fileService;
    private final Supplier<MapReduce<I>> chunkProcessCreator;
    private final Function<List<I>, T> reducer;

    interface FileService {
        long length();

        long address();
    }

    CalculateAverage_vaidhy(FileService fileService,
                            Supplier<MapReduce<I>> mapReduce,
                            Function<List<I>, T> reducer) {
        this.fileService = fileService;
        this.chunkProcessCreator = mapReduce;
        this.reducer = reducer;
    }

    static class LineStream {
        private final long fileEnd;
        private final long chunkEnd;

        private long position;
        private long hash;

        private final ByteBuffer buf = ByteBuffer
                .allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN);

        public LineStream(FileService fileService, long offset, long chunkSize) {
            long fileStart = fileService.address();
            this.fileEnd = fileStart + fileService.length();
            this.chunkEnd = fileStart + offset + chunkSize;
            this.position = fileStart + offset;
            this.hash = 0;
        }

        public boolean hasNext() {
            return position <= chunkEnd;
        }

        public long findSemi() {
            long h = DEFAULT_SEED;
            buf.rewind();

            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == ';') {
                    int discard = buf.remaining();
                    buf.rewind();
                    long nextData = (buf.getLong() << discard) >>> discard;
                    hash = simpleHash(h, nextData);
                    position = i + 1;
                    return i;
                }
                if (buf.hasRemaining()) {
                    buf.put(ch);
                }
                else {
                    buf.flip();
                    long nextData = buf.getLong();
                    h = simpleHash(h, nextData);
                    buf.rewind();
                }
            }
            hash = h;
            position = fileEnd;
            return fileEnd;
        }

        public long skipLine() {
            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == 0x0a) {
                    position = i + 1;
                    return i;
                }
            }
            position = fileEnd;
            return fileEnd;
        }

        public long findTemperature() {
            position += 3;
            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == 0x0a) {
                    position = i + 1;
                    return i;
                }
            }
            position = fileEnd;
            return fileEnd;
        }
    }

    private static final long START_BYTE_INDICATOR = 0x0101_0101_0101_0101L;
    private static final long END_BYTE_INDICATOR = START_BYTE_INDICATOR << 7;

    private static final long NEW_LINE_DETECTION = START_BYTE_INDICATOR * '\n';

    private static final long SEMI_DETECTION = START_BYTE_INDICATOR * ';';

    private static final long ALL_ONES = 0xffff_ffff_ffff_ffffL;

    private int findByte(long data, long pattern, int readOffset) {
        data >>>= (readOffset << 3);
        long match = data ^ pattern;
        long mask = (match - START_BYTE_INDICATOR) & ((~match) & END_BYTE_INDICATOR);

        if (mask == 0) {
            // Not Found
            return -1;
        }
        else {
            // Found
            return readOffset + (Long.numberOfTrailingZeros(mask) >>> 3);
        }

    }

    private int findSemi(long data, int readOffset) {
        return findByte(data, SEMI_DETECTION, readOffset);
    }

    private int findNewLine(long data, int readOffset) {
        return findByte(data, NEW_LINE_DETECTION, readOffset);
    }

    private void bigWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        long fileStart = fileService.address();
        long chunkEnd = fileStart + offset + chunkSize;
        long newRecordStart = fileStart + offset;
        long position = fileStart + offset;
        long fileEnd = fileStart + fileService.length();

        int nextReadOffset = 0;

        long data = UNSAFE.getLong(position);

        if (offset != 0) {
            boolean foundNewLine = false;
            for (; position < chunkEnd; position += 8) {
                data = UNSAFE.getLong(position);
                int newLinePosition = findNewLine(data, nextReadOffset);
                if (newLinePosition != -1) {
                    newRecordStart = position + newLinePosition + 1;
                    nextReadOffset = newLinePosition + 1;

                    if (nextReadOffset == 8) {
                        position += 8;
                        nextReadOffset = 0;
                        data = UNSAFE.getLong(position);
                    }
                    foundNewLine = true;
                    break;
                }
            }
            if (!foundNewLine) {
                return;
            }
        }

        boolean newLineToken = false;
        // false means looking for semi Colon
        // true means looking for new line.

        long stationEnd = offset;

        long hash = DEFAULT_SEED;
        long prevRelevant = 0;
        int prevBytes = 0;

        while (true) {
            if (newLineToken) {
                int newLinePosition = findNewLine(data, nextReadOffset);
                if (newLinePosition == -1) {
                    nextReadOffset = 0;
                    position += 8;
                    data = UNSAFE.getLong(position);
                }
                else {
                    long temperatureEnd = position + newLinePosition;
                    int temperature = parseDouble(stationEnd + 1, temperatureEnd);
                    // System.out.println("Big worker!");
                    lineConsumer.process(newRecordStart, stationEnd, hash, temperature);
                    newLineToken = false;

                    nextReadOffset = newLinePosition + 1;
                    newRecordStart = temperatureEnd + 1;

                    if (newRecordStart > chunkEnd || newRecordStart >= fileEnd) {
                        break;
                    }

                    hash = DEFAULT_SEED;

                    prevRelevant = 0;
                    prevBytes = 0;

                    if (nextReadOffset == 8) {
                        nextReadOffset = 0;
                        position += 8;
                        data = UNSAFE.getLong(position);
                    }
                }
            }
            else {
                int semiPosition = findSemi(data, nextReadOffset);

                // excessBytes = 5
                // prevData = aaax_xxxx

                // nextData = (nextData << excessBytes) | (data <<< (8 - excessBytes));

                // prevRelevant = 0000_0aaa

                // nextReadOffset = 2 ( 8 - NRO = 6 useful)
                // prevBytes = 3 (6 useful + 3 available = 9 - meaning 1 extra)

                if (semiPosition == -1) {
                    long currRelevant = data >>> (nextReadOffset << 3);
                    currRelevant <<= (prevBytes << 3);

                    prevRelevant = prevRelevant | currRelevant;
                    int newPrevBytes = prevBytes + (8 - nextReadOffset);

                    if (newPrevBytes >= 8) {
                        // System.out.println(unsafeToString(position - prevBytes, position + 8 - prevBytes));
                        // System.out.println(Long.toHexString(prevRelevant));
                        hash = simpleHash(hash, prevRelevant);

                        prevBytes = (newPrevBytes - 8);
                        if (prevBytes != 0) {
                            prevRelevant = (data >>> ((8 - prevBytes) << 3));
                        }
                        else {
                            prevRelevant = 0;
                        }
                    }
                    else {
                        prevBytes = newPrevBytes;
                    }

                    nextReadOffset = 0;
                    position += 8;
                    data = UNSAFE.getLong(position);
                }
                else {
                    // currentData = xxxx_x;aaN
                    if (semiPosition != 0) {
                        long currRelevant = (data & (ALL_ONES >>> ((8 - semiPosition) << 3))) >>> (nextReadOffset << 3);
                        // 0000_00aa

                        // 0aaa_0000;
                        long currUsable = currRelevant << (prevBytes << 3);

                        long toHash = prevRelevant | currUsable;

                        // System.out.println(unsafeToString(position - prevBytes, position + 8 - prevBytes));
                        // System.out.println(Long.toHexString(toHash));
                        //
                        // 0aaa_bbbb;

                        hash = simpleHash(hash, toHash);
                        // if (toHash == 0x6f506b696e7661a0L) {
                        // System.out.println("Debug");
                        // }

                        int newPrevBytes = prevBytes + (semiPosition - nextReadOffset);
                        if (newPrevBytes > 8) {

                            long remaining = currRelevant >>> ((8 - prevBytes) << 3);
                            hash = simpleHash(hash, remaining);
                        }
                    }
                    else {
                        hash = simpleHash(hash, prevRelevant);
                    }

                    prevRelevant = 0;
                    prevBytes = 0;

                    stationEnd = position + semiPosition;
                    nextReadOffset = semiPosition + 1;
                    newLineToken = true;

                    // String key = unsafeToString(newRecordStart, stationEnd);
                    // if (key.equals("id4058")) {
                    // System.out.println(key + " hash: " + hash);
                    // }

                    if (nextReadOffset == 8) {
                        nextReadOffset = 0;
                        position += 8;
                        data = UNSAFE.getLong(position);
                    }

                }
            }
            //
            // long semiPosition = findSemi(data, newLinePosition);
            // if (semiPosition != 0) {
            // pointerEnd = position + semiPosition;
            //
            // newLinePosition = findNewLine(data, semiPosition);
            //
            // }
        }

        // if (offset != 0) {
        // if (lineStream.hasNext()) {
        // // Skip the first line.
        // lineStream.skipLine();
        // }
        // else {
        // // No lines then do nothing.
        // return;
        // }
        // }
        // while (lineStream.hasNext()) {
        // long keyStartAddress = lineStream.position;
        // long keyEndAddress = lineStream.findSemi();
        // long keySuffix = lineStream.suffix;
        // int keyHash = lineStream.hash;
        // long valueStartAddress = lineStream.position;
        // long valueEndAddress = lineStream.findTemperature();
        // int temperature = parseDouble(valueStartAddress, valueEndAddress);
        // lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, temperature, keySuffix);
        // }

    }

    private void smallWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        LineStream lineStream = new LineStream(fileService, offset, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.skipLine();
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            long keyStartAddress = lineStream.position;
            long keyEndAddress = lineStream.findSemi();
            long keyHash = lineStream.hash;
            long valueStartAddress = lineStream.position;
            long valueEndAddress = lineStream.findTemperature();
            int temperature = parseDouble(valueStartAddress, valueEndAddress);
            // System.out.println("Small worker!");
            lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, temperature);
        }
    }

    // file size = 7
    // (0,0) (0,0) small chunk= (0,7)
    // a;0.1\n

    public T master(int shards, ExecutorService executor) {
        List<Future<I>> summaries = new ArrayList<>();
        long len = fileService.length();

        if (len > 128) {
            long bigChunk = Math.floorDiv(len, shards);
            long bigChunkReAlign = bigChunk & 0xffff_ffff_ffff_fff8L;

            long smallChunkStart = bigChunkReAlign * shards;
            long smallChunkSize = len - smallChunkStart;

            for (long offset = 0; offset < smallChunkStart; offset += bigChunkReAlign) {
                MapReduce<I> mr = chunkProcessCreator.get();
                final long transferOffset = offset;
                Future<I> task = executor.submit(() -> {
                    bigWorker(transferOffset, bigChunkReAlign, mr);
                    return mr.result();
                });
                summaries.add(task);
            }

            MapReduce<I> mrLast = chunkProcessCreator.get();
            Future<I> lastTask = executor.submit(() -> {
                smallWorker(smallChunkStart, smallChunkSize - 1, mrLast);
                return mrLast.result();
            });
            summaries.add(lastTask);
        }
        else {

            MapReduce<I> mrLast = chunkProcessCreator.get();
            Future<I> lastTask = executor.submit(() -> {
                smallWorker(0, len - 1, mrLast);
                return mrLast.result();
            });
            summaries.add(lastTask);
        }

        List<I> summariesDone = summaries.stream()
                .map(task -> {
                    try {
                        return task.get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        return reducer.apply(summariesDone);
    }

    static class DiskFileService implements FileService {
        private final long fileSize;
        private final long mappedAddress;

        DiskFileService(String fileName) throws IOException {
            FileChannel fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
            this.fileSize = fileChannel.size();
            this.mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize, Arena.global()).address();
        }

        @Override
        public long length() {
            return fileSize;
        }

        @Override
        public long address() {
            return mappedAddress;
        }
    }

    private static class ChunkProcessorImpl implements MapReduce<PrimitiveHashMap> {

        // 1 << 14 > 10,000 so it works
        private final PrimitiveHashMap statistics = new PrimitiveHashMap(14);

        @Override
        public void process(long keyStartAddress, long keyEndAddress, long hash, int temperature) {
            // System.out.println(unsafeToString(keyStartAddress, keyEndAddress) + " --> " + temperature + " hash: " + hash);
            HashEntry entry = statistics.find(keyStartAddress, keyEndAddress, hash);
            if (entry == null) {
                throw new IllegalStateException("Hash table too small :(");
            }
            if (entry.value == null) {
                entry.value = new IntSummaryStatistics();
            }
            entry.value.accept(temperature);

            // IntSummaryStatistics stats = verify.computeIfAbsent(key, ignore -> new IntSummaryStatistics());
            // stats.accept(temperature);
            // if (stats.getCount() != entry.value.getCount()) {
            // System.out.println("Trouble");
            // }
        }

        @Override
        public PrimitiveHashMap result() {
            return statistics;
        }
    }

    public static void main(String[] args) throws IOException {
        DiskFileService diskFileService = new DiskFileService(FILE);

        CalculateAverage_vaidhy<PrimitiveHashMap, Map<String, IntSummaryStatistics>> calculateAverageVaidhy = new CalculateAverage_vaidhy<>(
                diskFileService,
                ChunkProcessorImpl::new,
                CalculateAverage_vaidhy::combineOutputs);

        int proc = 2 * Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(proc);
        Map<String, IntSummaryStatistics> output = calculateAverageVaidhy.master(proc, executor);
        executor.shutdown();

        Map<String, String> outputStr = toPrintMap(output);
        System.out.println(outputStr);
    }

    private static Map<String, String> toPrintMap(Map<String, IntSummaryStatistics> output) {

        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<String, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(entry.getKey(),
                    STR."\{stat.getMin() / 10.0}/\{Math.round(stat.getAverage()) / 10.0}/\{stat.getMax() / 10.0}");
        }
        return outputStr;
    }

    private static Map<String, IntSummaryStatistics> combineOutputs(
                                                                    List<PrimitiveHashMap> list) {

        Map<String, IntSummaryStatistics> output = new HashMap<>(10000);
        for (PrimitiveHashMap map : list) {
            for (HashEntry entry : map.entries) {
                if (entry.value != null) {
                    String keyStr = unsafeToString(entry.startAddress, entry.endAddress);

                    output.compute(keyStr, (ignore, val) -> {
                        if (val == null) {
                            return entry.value;
                        }
                        else {
                            val.combine(entry.value);
                            return val;
                        }
                    });
                }
            }
        }

        return output;
    }

    private static String unsafeToString(long startAddress, long endAddress) {
        byte[] keyBytes = new byte[(int) (endAddress - startAddress)];
        for (int i = 0; i < keyBytes.length; i++) {
            keyBytes[i] = UNSAFE.getByte(startAddress + i);
        }
        return new String(keyBytes, StandardCharsets.UTF_8);
    }
}
