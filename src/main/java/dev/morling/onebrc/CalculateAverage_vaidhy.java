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

public class CalculateAverage_vaidhy<T> {

    private record Entry(long startAddress, long endAddress, IntSummaryStatistics value) {
    }

    private static class PrimitiveHash {
        Entry [] entries;

        PrimitiveHash(int capacity) {
            this.entries = new Entry[capacity];
        }

        public IntSummaryStatistics get(long startAddress, long endAddress, int hash) {
            int i = hash, len = entries.length;
            do {
                Entry entry = entries[i];
                if (entry.startAddress == 0) {
                    return null;
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

    record ByteSlice(long from, long to, int hCode) {

        @Override
        public int hashCode() {
            if (hCode != 0) { return hCode; }
            int h = 0;
            for (long i = from; i < to; i++) {
                h = (h * 31) ^ UNSAFE.getByte(i);
            }
            return h;
        }

        public int length() {
            return (int) (to - from);
        }

        public byte get(int i) {
            return UNSAFE.getByte(from + i);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof ByteSlice bs) {
                int len = this.length();
                if (bs.length() != len) {
                    return false;
                }
                for (int i = 0; i < len; i++) {
                    if (this.get(i) != bs.get(i)) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            byte[] copy = new byte[this.length()];
            for (int i = 0; i < copy.length; i++) {
                copy[i] = get(i);
            }
            return new String(copy, StandardCharsets.UTF_8);
        }
    }

    private static int parseDouble(ByteSlice slice) {
        int normalized = 0;
        boolean sign = true;
        int index = 0;
        if (slice.get(index) == '-') {
            index++;
            sign = false;
        }
        int length = slice.length();
        for (; index < length; index++) {
            byte ch = slice.get(index);
            if (ch != '.') {
                normalized = normalized * 10 + (ch - '0');
            }
        }
        if (!sign) {
            normalized = -normalized;
        }
        return normalized;
    }

    interface MapReduce<T> {

        void process(ByteSlice key, ByteSlice value);

        T result();
    }

    private final FileService fileService;
    private final Supplier<MapReduce<T>> chunkProcessCreator;
    private final Function<List<T>, T> reducer;

    interface FileService {
        long length();

        long address();
    }

    public CalculateAverage_vaidhy(FileService fileService,
                                   Supplier<MapReduce<T>> mapReduce,
                                   Function<List<T>, T> reducer) {
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

        public LineStream(FileService fileService, long offset, long chunkSize) {
            long fileStart = fileService.address();
            this.fileEnd = fileStart + fileService.length();
            this.chunkEnd = fileStart + offset + chunkSize;
            this.position = fileStart + offset;
        }

        public boolean hasNext() {
            return position <= chunkEnd && position < fileEnd;
        }

        public ByteSlice until(byte ch, boolean computeHash) {
            int h = 0;
            byte inCh;
            for (long i = position; i < fileEnd; i++) {
                if ((inCh = UNSAFE.getByte(i)) == ch) {
                    try {
                        return new ByteSlice(position, i, h);
                    }
                    finally {
                        position = i + 1;
                    }
                }
                if (computeHash) {
                    h = (h * 31) ^ inCh;
                }
            }

            try {
                return new ByteSlice(position, fileEnd, h);
            }
            finally {
                position = fileEnd;
            }
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    private void worker(long offset, long chunkSize, MapReduce<T> lineConsumer) {
        LineStream lineStream = new LineStream(fileService, offset, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.until((byte) '\n', false);
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            ByteSlice key = lineStream.until((byte) ';', true);
            ByteSlice value = lineStream.until((byte) '\n', false);
            lineConsumer.process(key, value);
        }
    }

    // Space complexity: O(number of workers), not counting
    // workers space assuming they are running in different hosts.
    public T master(long chunkSize, ExecutorService executor) {
        long len = fileService.length();
        List<Future<T>> summaries = new ArrayList<>();

        for (long offset = 0; offset < len; offset += chunkSize) {
            long workerLength = Math.min(len, offset + chunkSize) - offset;
            MapReduce<T> mr = chunkProcessCreator.get();
            final long transferOffset = offset;
            Future<T> task = executor.submit(() -> {
                worker(transferOffset, workerLength, mr);
                return mr.result();
            });
            summaries.add(task);
        }
        List<T> summariesDone = summaries.stream()
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

    public static class ChunkProcessorImpl implements MapReduce<Map<ByteSlice, IntSummaryStatistics>> {

        private final Map<ByteSlice, IntSummaryStatistics> statistics = new HashMap<>(10000);

        @Override
        public void process(ByteSlice station, ByteSlice value) {
            int temperature = parseDouble(value);
            IntSummaryStatistics stats = statistics.get(station);
            if (stats == null) {
                stats = new IntSummaryStatistics();
                statistics.put(station, stats);
            }
            stats.accept(temperature);
        }

        @Override
        public Map<ByteSlice, IntSummaryStatistics> result() {
            return statistics;
        }
    }

    public static void main(String[] args) throws IOException {
        DiskFileService diskFileService = new DiskFileService(FILE);

        CalculateAverage_vaidhy<Map<ByteSlice, IntSummaryStatistics>> calculateAverageVaidhy = new CalculateAverage_vaidhy<>(
                diskFileService,
                ChunkProcessorImpl::new,
                CalculateAverage_vaidhy::combineOutputs);

        int proc = 2 * Runtime.getRuntime().availableProcessors();

        int shards = proc;
        long fileSize = diskFileService.length();
        long chunkSize = Math.ceilDiv(fileSize, shards);

        ExecutorService executor = Executors.newFixedThreadPool(proc);
        Map<ByteSlice, IntSummaryStatistics> output = calculateAverageVaidhy.master(chunkSize, executor);
        executor.shutdown();

        Map<String, String> outputStr = toPrintMap(output);
        System.out.println(outputStr);
    }

    private static Map<String, String> toPrintMap(Map<ByteSlice, IntSummaryStatistics> output) {

        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<ByteSlice, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(entry.getKey().toString(),
                    STR."\{stat.getMin() / 10.0}/\{Math.round(stat.getAverage()) / 10.0}/\{stat.getMax() / 10.0}");
        }
        return outputStr;
    }

    private static Map<ByteSlice, IntSummaryStatistics> combineOutputs(List<Map<ByteSlice, IntSummaryStatistics>> list) {
        Map<ByteSlice, IntSummaryStatistics> output = new HashMap<>(10000);
        for (Map<ByteSlice, IntSummaryStatistics> map : list) {
            for (Map.Entry<ByteSlice, IntSummaryStatistics> entry : map.entrySet()) {
                output.compute(entry.getKey(), (ignore, val) -> {
                    if (val == null) {
                        return entry.getValue();
                    }
                    else {
                        val.combine(entry.getValue());
                        return val;
                    }
                });
            }
        }

        return output;
    }
}
