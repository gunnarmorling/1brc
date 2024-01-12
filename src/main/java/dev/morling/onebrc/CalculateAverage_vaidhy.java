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
        HashEntry[] entries;

        PrimitiveHashMap(int capacity) {
            this.entries = new HashEntry[capacity];
            for (int i = 0; i < capacity; i++) {
                this.entries[i] = new HashEntry();
            }
        }

        public HashEntry find(long startAddress, long endAddress, long hash) {
            int len = entries.length;
            int i = Math.floorMod(hash, len);
            long lookupLength = endAddress - startAddress;

            do {
                HashEntry entry = entries[i];
                if (entry.value == null) {
                    return entry;
                }
                if (entry.hash == hash) {
                    long entryLength = endAddress - startAddress;
                    if (entryLength == lookupLength) {
                        long entryIndex = entry.startAddress;
                        long lookupIndex = startAddress;
                        boolean found = true;
                        for (; lookupIndex < endAddress; lookupIndex++) {
                            if (UNSAFE.getByte(entryIndex) != UNSAFE.getByte(lookupIndex)) {
                                found = false;
                                break;
                            }
                            entryIndex++;
                        }
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
    //
    // record ByteSlice(long from, long to, int hCode) {
    //
    // @Override
    // public int hashCode() {
    // if (hCode != 0) { return hCode; }
    // int h = 0;
    // for (long i = from; i < to; i++) {
    // h = (h * 31) ^ UNSAFE.getByte(i);
    // }
    // return h;
    // }
    //
    // public int length() {
    // return (int) (to - from);
    // }
    //
    // public byte get(int i) {
    // return UNSAFE.getByte(from + i);
    // }
    //
    // @Override
    // public boolean equals(Object o) {
    // if (o instanceof ByteSlice bs) {
    // int len = this.length();
    // if (bs.length() != len) {
    // return false;
    // }
    // for (int i = 0; i < len; i++) {
    // if (this.get(i) != bs.get(i)) {
    // return false;
    // }
    // }
    // return true;
    // } else {
    // return false;
    // }
    // }
    //
    // @Override
    // public String toString() {
    // byte[] copy = new byte[this.length()];
    // for (int i = 0; i < copy.length; i++) {
    // copy[i] = get(i);
    // }
    // return new String(copy, StandardCharsets.UTF_8);
    // }
    // }

    private static int parseDouble(long startAddress, long endAddress) {
        int normalized = 0;
        boolean sign = true;
        long index = startAddress;
        if (UNSAFE.getByte(index) == '-') {
            index++;
            sign = false;
        }
        for (; index < endAddress; index++) {
            byte ch = UNSAFE.getByte(index);
            if (ch != '.') {
                normalized = normalized * 10 + (ch - '0');
            }
        }
        if (!sign) {
            normalized = -normalized;
        }
        return normalized;
    }

    interface MapReduce<I> {

        void process(long keyStartAddress, long keyEndAddress, int hash, int temperature);

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

    /**
     * Reads lines from a given character stream, hasNext() is always
     * efficient, all work is done only in next().
     */
    // Space complexity: O(max line length) in next() call, structure is O(1)
    // not counting charStream as it is only a reference, we will count that
    // in worker space.
    static class LineStream {
        private final long fileEnd;
        private final long chunkEnd;

        private long position;
        private int hash;

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

        public long find(byte ch, boolean computeHash) {
            int h = 0;
            byte inCh;
            for (long i = position; i < fileEnd; i++) {
                if ((inCh = UNSAFE.getByte(i)) == ch) {
                    try {
                        return i;
                    }
                    finally {
                        position = i + 1;
                    }
                }
                if (computeHash) {
                    h = (h * 31) ^ inCh;
                    this.hash = h;
                }
            }

            try {
                return fileEnd;
            }
            finally {
                position = fileEnd;
            }
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    private void worker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        LineStream lineStream = new LineStream(fileService, offset, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.find((byte) '\n', false);
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            long keyStartAddress = lineStream.position;
            long keyEndAddress = lineStream.find((byte) ';', true);
            int keyHash = lineStream.hash;
            long valueStartAddress = lineStream.position;
            long valueEndAddress = lineStream.find((byte) '\n', false);
            int temperature = parseDouble(valueStartAddress, valueEndAddress);
            lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, temperature);
        }
    }

    // Space complexity: O(number of workers), not counting
    // workers space assuming they are running in different hosts.
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

    /// SAMPLE CANDIDATE CODE ENDS

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

    public static class ChunkProcessorImpl implements MapReduce<PrimitiveHashMap> {

        private final PrimitiveHashMap statistics = new PrimitiveHashMap(1024);

        @Override
        public void process(long keyStartAddress, long keyEndAddress, int hash, int temperature) {
            HashEntry entry = statistics.find(keyStartAddress, keyStartAddress, hash);
            if (entry == null) {
                throw new IllegalStateException("Hash table too small :(");
            }
            if (entry.value == null) {
                entry.startAddress = keyStartAddress;
                entry.endAddress = keyEndAddress;
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

        int shards = proc;
        long fileSize = diskFileService.length();
        long chunkSize = Math.ceilDiv(fileSize, shards);

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
