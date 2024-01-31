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
        private long keyLength;
        private long suffix;
        private int next;
        IntSummaryStatistics value;
    }

    private static class PrimitiveHashMap {
        private final HashEntry[] entries;
        private final long[] hashes;

        private final int twoPow;
        private int next = -1;

        PrimitiveHashMap(int twoPow) {
            this.twoPow = twoPow;
            this.entries = new HashEntry[1 << twoPow];
            this.hashes = new long[1 << twoPow];
            for (int i = 0; i < entries.length; i++) {
                this.entries[i] = new HashEntry();
            }
        }

        public IntSummaryStatistics find(long startAddress, long endAddress, long hash, long suffix) {
            int len = entries.length;
            int h = Long.hashCode(hash);
            int initialIndex = (h ^ (h >> twoPow)) & (len - 1);
            int i = initialIndex;
            long lookupLength = endAddress - startAddress;

            long hashEntry = hashes[i];

            if (hashEntry == hash) {
                HashEntry entry = entries[i];
                if (lookupLength <= 7) {
                    // This works because
                    // hash = suffix , when simpleHash is just xor.
                    // Since length is not 8, suffix will have a 0 at the end.
                    // Since utf-8 strings can't have 0 in middle of a string this means
                    // we can stop here.
                    return entry.value;
                }
                boolean found = (entry.suffix == suffix &&
                        compareEntryKeys(startAddress, endAddress, entry.startAddress));
                if (found) {
                    return entry.value;
                }
            }

            if (hashEntry == 0) {
                HashEntry entry = entries[i];
                entry.startAddress = startAddress;
                entry.keyLength = lookupLength;
                hashes[i] = hash;
                entry.suffix = suffix;
                entry.next = next;
                this.next = i;
                entry.value = new IntSummaryStatistics();
                return entry.value;
            }

            i++;
            if (i == len) {
                i = 0;
            }

            if (i == initialIndex) {
                return null;
            }

            do {
                hashEntry = hashes[i];
                if (hashEntry == hash) {
                    HashEntry entry = entries[i];
                    if (lookupLength <= 7) {
                        return entry.value;
                    }
                    boolean found = (entry.suffix == suffix &&
                            compareEntryKeys(startAddress, endAddress, entry.startAddress));
                    if (found) {
                        return entry.value;
                    }
                }
                if (hashEntry == 0) {
                    HashEntry entry = entries[i];
                    entry.startAddress = startAddress;
                    entry.keyLength = lookupLength;
                    hashes[i] = hash;
                    entry.suffix = suffix;
                    entry.next = next;
                    this.next = i;
                    entry.value = new IntSummaryStatistics();
                    return entry.value;
                }

                i++;
                if (i == len) {
                    i = 0;
                }
            } while (i != initialIndex);
            return null;
        }

        private static boolean compareEntryKeys(long startAddress, long endAddress, long entryStartAddress) {
            long entryIndex = entryStartAddress;
            long lookupIndex = startAddress;
            long endAddressStop = endAddress - 7;

            for (; lookupIndex < endAddressStop; lookupIndex += 8) {
                if (UNSAFE.getLong(entryIndex) != UNSAFE.getLong(lookupIndex)) {
                    return false;
                }
                entryIndex += 8;
            }

            return true;
        }

        public Iterable<HashEntry> entrySet() {
            return () -> new Iterator<>() {
                int scan = next;

                @Override
                public boolean hasNext() {
                    return scan != -1;
                }

                @Override
                public HashEntry next() {
                    HashEntry entry = entries[scan];
                    scan = entry.next;
                    return entry;
                }
            };
        }
    }

    private static final String FILE = "./measurements.txt";

    private static long simpleHash(long hash, long nextData) {
        return hash ^ nextData;
        // return (hash ^ Long.rotateLeft((nextData * C1), R1)) * C2;
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

        void process(long keyStartAddress, long keyEndAddress, long hash, long suffix, int temperature);

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

        private long suffix;

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
            long h = 0;
            buf.rewind();

            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == ';') {
                    int discard = buf.remaining();
                    buf.rewind();
                    long nextData = (buf.getLong() << discard) >>> discard;
                    this.suffix = nextData;
                    this.hash = simpleHash(h, nextData);
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
            this.hash = h;
            this.suffix = buf.getLong();
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

    private long findByteOctet(long data, long pattern) {
        long match = data ^ pattern;
        return (match - START_BYTE_INDICATOR) & ((~match) & END_BYTE_INDICATOR);
    }

    private void bigWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        long chunkStart = offset + fileService.address();
        long chunkEnd = chunkStart + chunkSize;
        long fileEnd = fileService.address() + fileService.length();
        long stopPoint = Math.min(chunkEnd + 1, fileEnd);

        boolean skip = offset != 0;
        for (long position = chunkStart; position < stopPoint;) {
            if (skip) {
                long data = UNSAFE.getLong(position);
                long newLineMask = findByteOctet(data, NEW_LINE_DETECTION);
                if (newLineMask != 0) {
                    int newLinePosition = Long.numberOfTrailingZeros(newLineMask) >>> 3;
                    skip = false;
                    position = position + newLinePosition + 1;
                }
                else {
                    position = position + 8;
                }
                continue;
            }

            long stationStart = position;
            long stationEnd = -1;
            long hash = 0;
            long suffix = 0;
            do {
                long data = UNSAFE.getLong(position);
                long semiMask = findByteOctet(data, SEMI_DETECTION);
                if (semiMask != 0) {
                    int semiPosition = Long.numberOfTrailingZeros(semiMask) >>> 3;
                    stationEnd = position + semiPosition;
                    position = stationEnd + 1;

                    if (semiPosition != 0) {
                        suffix = data & (ALL_ONES >>> (64 - (semiPosition << 3)));
                    }
                    else {
                        suffix = UNSAFE.getLong(position - 8);
                    }
                    hash = simpleHash(hash, suffix);
                    break;
                }
                else {
                    hash = simpleHash(hash, data);
                    position = position + 8;
                }
            } while (true);

            int temperature = 0;
            {
                byte ch = UNSAFE.getByte(position++);
                boolean negative = false;
                if (ch == '-') {
                    negative = true;
                    ch = UNSAFE.getByte(position++);
                }
                do {
                    if (ch != '.') {
                        temperature *= 10;
                        temperature += (ch ^ '0');
                    }
                    ch = UNSAFE.getByte(position++);
                } while (ch != '\n');
                if (negative) {
                    temperature = -temperature;
                }
            }

            lineConsumer.process(stationStart, stationEnd, hash, suffix, temperature);
        }
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
            long suffix = lineStream.suffix;
            long valueStartAddress = lineStream.position;
            long valueEndAddress = lineStream.findTemperature();
            int temperature = parseDouble(valueStartAddress, valueEndAddress);
            // System.out.println("Small worker!");
            lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, suffix, temperature);
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
        private final PrimitiveHashMap statistics = new PrimitiveHashMap(15);

        @Override
        public void process(long keyStartAddress, long keyEndAddress, long hash, long suffix, int temperature) {
            IntSummaryStatistics stats = statistics.find(keyStartAddress, keyEndAddress, hash, suffix);
            stats.accept(temperature);
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

        int proc = Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(proc);
        Map<String, IntSummaryStatistics> output = calculateAverageVaidhy.master(2 * proc, executor);
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

        Map<String, IntSummaryStatistics> output = HashMap.newHashMap(10000);
        for (PrimitiveHashMap map : list) {
            for (HashEntry entry : map.entrySet()) {
                if (entry.value != null) {
                    String keyStr = unsafeToString(entry.startAddress,
                            entry.startAddress + entry.keyLength);

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
