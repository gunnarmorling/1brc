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
        private long suffix;
        private int hash;

        IntSummaryStatistics value;
    }

    private static class PrimitiveHashMap {
        private final HashEntry[] entries;
        private final int twoPow;

        PrimitiveHashMap(int twoPow) {
            this.twoPow = twoPow;
            this.entries = new HashEntry[1 << twoPow];
            for (int i = 0; i < entries.length; i++) {
                this.entries[i] = new HashEntry();
            }
        }

        public HashEntry find(long startAddress, long endAddress, long suffix, int hash) {
            int len = entries.length;
            int i = (hash ^ (hash >> twoPow)) & (len - 1);

            do {
                HashEntry entry = entries[i];
                if (entry.value == null) {
                    return entry;
                }
                if (entry.hash == hash) {
                    long entryLength = entry.endAddress - entry.startAddress;
                    long lookupLength = endAddress - startAddress;
                    if ((entryLength == lookupLength) && (entry.suffix == suffix)) {
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

        void process(long keyStartAddress, long keyEndAddress, int hash, int temperature, long suffix);

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
        private int hash;
        private long suffix;
        byte[] b = new byte[4];

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
            int h = 0;
            long s = 0;
            long i = position;
            while ((i + 3) < fileEnd) {
                // Adding 16 as it is the offset for primitive arrays
                ByteBuffer.wrap(b).putInt(UNSAFE.getInt(i));

                if (b[3] == 0x3B) {
                    break;
                }
                i++;
                h = ((h << 5) - h) ^ b[3];
                s = (s << 8) ^ b[3];

                if (b[2] == 0x3B) {
                    break;
                }
                i++;
                h = ((h << 5) - h) ^ b[2];
                s = (s << 8) ^ b[2];

                if (b[1] == 0x3B) {
                    break;
                }
                i++;
                h = ((h << 5) - h) ^ b[1];
                s = (s << 8) ^ b[1];

                if (b[0] == 0x3B) {
                    break;
                }
                i++;
                h = ((h << 5) - h) ^ b[0];
                s = (s << 8) ^ b[0];
            }

            this.hash = h;
            this.suffix = s;
            position = i + 1;
            return i;
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

    private long findNewLine(long data, int readOffset) {
        data = data >>> (8 - readOffset);

    }



    private void bigWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        long chunkEnd = offset + chunkSize;
        long newRecordStart = offset;
        long position = offset;

        int nextReadOffset = -1;
        int excessBytes = 0;

        long data;
        long prevData = 0;

        if (offset != 0) {
            for (; position < chunkEnd; position += 8) {
                data = UNSAFE.getLong(position);
                int newLinePosition = findNewLine(data, 0);
                if (newLinePosition != -1) {
                    newRecordStart = position + newLinePosition + 1;
                    nextReadOffset = newLinePosition + 1;

                    if (nextReadOffset == 8) {
                        position += 8;
                        prevData = data;
                        data = UNSAFE.getLong(position);
                    } else {
                        excessBytes = nextReadOffset;
                    }
                    break;
                }
            }
            if (nextReadOffset == -1) {
                return;
            }
        }


        boolean newLineToken = false;
        // false means looking for semi Colon
        // true means looking for new line.

        long stationEnd = offset;

        long hash = DEFAULT_SEED;

        long suffix = 0;

        while (true) {
            if (newLineToken) {
                int newLinePosition = findNewLine(data, nextReadOffset);
                if (newLinePosition == -1) {
                    nextReadOffset = 0;
                    position += 8;
                    data = UNSAFE.getLong(position);
                    continue;
                } else {

                    long temperatureEnd = position + newLinePosition;

                    // TODO:
                    // station = [newRecordStart, stationEnd )
                    // temperature = [stationEnd + 1, temperatureEnd )
                    // parseDouble
                    // insert
                    int temperature = parseDouble(stationEnd + 1, temperatureEnd);
                    lineConsumer.process(newRecordStart, stationEnd, hash, temperature, suffix);
                    newLineToken = false;

                    nextReadOffset = newLinePosition + 1;
                    newRecordStart = temperatureEnd + 1;

                    hash = DEFAULT_SEED;

                    if (position >= chunkEnd) {
                        break;
                    }
                    if (nextReadOffset == 8) {
                        nextReadOffset = 0;
                        position += 8;
                        data = UNSAFE.getLong(position);
                    }
                }
            } else {
                int semiPosition = findSemi(data, nextReadOffset);

                // excessBytes = 5
                // prevData = aaax_xxxx

                // nextData = (nextData << excessBytes) | (data <<< (8 - excessBytes));

                long prevRelevant = prevData & (0xffff_ffffL << excessBytes);
                // prevRelevant = aaa0_0000

                if (semiPosition == -1) {
                    // currentData = bbbb_bbbb
                    long currRelevant = data >>> (8 - excessBytes);
                    // currRelevant = 000b_bbbb
                    long toHash = prevRelevant | currRelevant;
                    // toHash = sssb_bbbb

                    hash = simpleHash(hash, toHash);

                    nextReadOffset = 0;
                    position += 8;
                    data = UNSAFE.getLong(position);
                } else {
                    // currentData = b;xx_xxxx
                    long currRelevant = data >>> (8 - excessBytes);
                    // currRelevant = 000b_;xxx
                    currRelevant &= (0xffff_ffff << (8 - semiPosition));

                    long toHash = prevRelevant | currRelevant;
                    // toHash = sssb_bbbb

                    nextData &= ((0xfffff_ffffL) << (8 - (excessBytes + semiPosition)));
                    suffix = nextData;

                    hash = simpleHash(hash, nextData);


                    stationEnd = position + semiPosition;
                    nextReadOffset = semiPosition + 1;
                    newLineToken = true;

                    if (nextReadOffset == 8) {
                        nextReadOffset = 0;
                        position += 8;
                        data = UNSAFE.getLong(position);
                    }

                }
            }
//
//            long semiPosition = findSemi(data, newLinePosition);
//            if (semiPosition != 0) {
//                pointerEnd = position + semiPosition;
//
//                newLinePosition = findNewLine(data, semiPosition);
//
//            }
        }







//        if (offset != 0) {
//            if (lineStream.hasNext()) {
//                // Skip the first line.
//                lineStream.skipLine();
//            }
//            else {
//                // No lines then do nothing.
//                return;
//            }
//        }
//        while (lineStream.hasNext()) {
//            long keyStartAddress = lineStream.position;
//            long keyEndAddress = lineStream.findSemi();
//            long keySuffix = lineStream.suffix;
//            int keyHash = lineStream.hash;
//            long valueStartAddress = lineStream.position;
//            long valueEndAddress = lineStream.findTemperature();
//            int temperature = parseDouble(valueStartAddress, valueEndAddress);
//            lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, temperature, keySuffix);
//        }

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
            long keySuffix = lineStream.suffix;
            int keyHash = lineStream.hash;
            long valueStartAddress = lineStream.position;
            long valueEndAddress = lineStream.findTemperature();
            int temperature = parseDouble(valueStartAddress, valueEndAddress);
            lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, temperature, keySuffix);
        }
    }

    public T master(int shards, ExecutorService executor) {
        long len = fileService.length();

        long fileEndAligned = (len - 128) & 0xffff_ffff_ffff_fff8L;
        long bigChunk = Math.floorDiv(fileEndAligned, shards);
        long bigChunkReAlign = bigChunk & 0xffff_ffff_ffff_fff8L;

        long smallChunkStart = bigChunkReAlign * shards;
        long smallChunkSize = len - smallChunkStart;

        System.out.println(UNSAFE.addressSize());
        System.out.println(fileService.address() % UNSAFE.addressSize());

        List<Future<I>> summaries = new ArrayList<>();

        for (long offset = 0; offset < fileEndAligned; offset += bigChunkReAlign) {
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
        public void process(long keyStartAddress, long keyEndAddress, int hash, int temperature, long suffix) {
            HashEntry entry = statistics.find(keyStartAddress, keyEndAddress, suffix, hash);
            if (entry == null) {
                throw new IllegalStateException("Hash table too small :(");
            }
            if (entry.value == null) {
                entry.startAddress = keyStartAddress;
                entry.endAddress = keyEndAddress;
                entry.suffix = suffix;
                entry.hash = hash;
                entry.value = new IntSummaryStatistics();
            }
            entry.value.accept(temperature);
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
