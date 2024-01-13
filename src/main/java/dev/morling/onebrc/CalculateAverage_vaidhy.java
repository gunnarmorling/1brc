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
            return position <= chunkEnd && position < fileEnd;
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

    private void worker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
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

    public T master(long chunkSize, ExecutorService executor) {
        long len = fileService.length();
        List<Future<I>> summaries = new ArrayList<>();

        for (long offset = 0; offset < len; offset += chunkSize) {
            long workerLength = Math.min(len, offset + chunkSize) - offset;
            MapReduce<I> mr = chunkProcessCreator.get();
            final long transferOffset = offset;
            Future<I> task = executor.submit(() -> {
                worker(transferOffset, workerLength, mr);
                return mr.result();
            });
            summaries.add(task);
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

        long fileSize = diskFileService.length();
        long chunkSize = Math.ceilDiv(fileSize, proc);

        ExecutorService executor = Executors.newFixedThreadPool(proc);
        Map<String, IntSummaryStatistics> output = calculateAverageVaidhy.master(chunkSize, executor);
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
