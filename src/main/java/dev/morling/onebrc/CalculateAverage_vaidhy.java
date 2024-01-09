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
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CalculateAverage_vaidhy<T> {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

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

    private final FileService fileService;
    private final Supplier<MapReduce<T>> chunkProcessCreator;
    private final Function<List<T>, T> reducer;

    interface MapReduce<T> extends Consumer<EfficientString> {
        T result();
    }

    interface FileService {
        long length();

        byte getByte(long position);
    }

    public CalculateAverage_vaidhy(FileService fileService,
                                   Supplier<MapReduce<T>> mapReduce,
                                   Function<List<T>, T> reducer) {
        this.fileService = fileService;
        this.chunkProcessCreator = mapReduce;
        this.reducer = reducer;
    }

    /// SAMPLE CANDIDATE CODE STARTS

    /**
     * Reads from a given offset till the end, it calls server in
     * blocks of scanSize whenever cursor passes the current block.
     * Typically when hasNext() is called. hasNext() is efficient
     * in the sense calling second time is cheap if next() is not
     * called in between. Cheap in the sense no call to server is
     * made.
     */
    // Space complexity = O(scanSize)
    static class ByteStream {

        private final FileService fileService;
        private long position;
        private long fileLength;

        public ByteStream(FileService fileService, long offset) {
            this.fileService = fileService;
            this.fileLength = fileService.length();
            this.position = offset;
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
        }

        public boolean hasNext() {
            return position < fileLength;
        }

        public byte next() {
            return fileService.getByte(position++);
        }
    }

    /**
     * Reads lines from a given character stream, hasNext() is always
     * efficient, all work is done only in next().
     */
    // Space complexity: O(max line length) in next() call, structure is O(1)
    // not counting charStream as it is only a reference, we will count that
    // in worker space.
    static class LineStream implements Iterator<EfficientString> {
        private final ByteStream byteStream;
        private int readIndex;
        private final long length;

        public LineStream(ByteStream byteStream, long length) {
            this.byteStream = byteStream;
            this.readIndex = 0;
            this.length = length;
        }

        @Override
        public boolean hasNext() {
            return readIndex <= length && byteStream.hasNext();
        }

        @Override
        public EfficientString next() {
            byte[] line = new byte[128];
            int i = 0;
            while (byteStream.hasNext()) {
                byte ch = byteStream.next();
                readIndex++;
                if (ch == 0x0a) {
                    break;
                }
                line[i++] = ch;
            }
            return new EfficientString(line, i);
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    public void worker(long offset, long chunkSize, Consumer<EfficientString> lineConsumer) {
        ByteStream byteStream = new ByteStream(fileService, offset);
        Iterator<EfficientString> lineStream = new LineStream(byteStream, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.next();
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            lineConsumer.accept(lineStream.next());
        }
    }

    // Space complexity: O(number of workers), not counting
    // workers space assuming they are running in different hosts.
    public T master(long chunkSize) {
        long len = fileService.length();
        List<ForkJoinTask<T>> summaries = new ArrayList<>();
        ForkJoinPool commonPool = ForkJoinPool.commonPool();

        for (long offset = 0; offset < len; offset += chunkSize) {
            long workerLength = Math.min(len, offset + chunkSize) - offset;
            MapReduce<T> mr = chunkProcessCreator.get();
            final long transferOffset = offset;
            ForkJoinTask<T> task = commonPool.submit(() -> {
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

        private final FileChannel fileChannel;
        private final long mappedAddress;

        DiskFileService(String fileName) throws IOException {
            this.fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
            this.mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileChannel.size(), Arena.global()).address();
        }

        @Override
        public long length() {
            try {
                return this.fileChannel.size();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte getByte(long position) {
            return UNSAFE.getByte(mappedAddress + position);
        }
    }

    record EfficientString(byte[] arr, int length) {

        @Override
        public int hashCode() {
            int h = 0;
            for (int i = 0; i < length; i++) {
                h = (h * 32) + arr[i];
            }
            return h;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            EfficientString eso = (EfficientString) o;
            if (eso.length != this.length) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (arr[i] != eso.arr[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final EfficientString EMPTY = new EfficientString(new byte[0], 0);

    public static class ChunkProcessorImpl implements MapReduce<Map<EfficientString, IntSummaryStatistics>> {

        private final Map<EfficientString, IntSummaryStatistics> statistics = new HashMap<>(10000);

        @Override
        public void accept(EfficientString line) {
            EfficientString station = getStation(line);

            int normalized = parseDouble(line.arr,
                    station.length + 1, line.length);

            updateStats(station, normalized);
        }

        private void updateStats(EfficientString station, int normalized) {
            IntSummaryStatistics stats = statistics.get(station);
            if (stats == null) {
                stats = new IntSummaryStatistics();
                statistics.put(station, stats);
            }
            stats.accept(normalized);
        }

        private static EfficientString getStation(EfficientString line) {
            for (int i = 0; i < line.length; i++) {
                if (line.arr[i] == ';') {
                    return new EfficientString(line.arr, i);
                }
            }
            return EMPTY;
        }

        private static int parseDouble(byte[] value, int offset, int length) {
            int normalized = 0;
            int index = offset;
            boolean sign = true;
            if (value[index] == '-') {
                index++;
                sign = false;
            }
            // boolean hasDot = false;
            for (; index < length; index++) {
                byte ch = value[index];
                if (ch != '.') {
                    normalized = normalized * 10 + (ch - '0');
                }
                // else {
                // hasDot = true;
                // }
            }
            // if (!hasDot) {
            // normalized *= 10;
            // }
            if (!sign) {
                normalized = -normalized;
            }
            return normalized;
        }

        @Override
        public Map<EfficientString, IntSummaryStatistics> result() {
            return statistics;
        }
    }

    public static void main(String[] args) throws IOException {
        DiskFileService diskFileService = new DiskFileService(FILE);

        CalculateAverage_vaidhy<Map<EfficientString, IntSummaryStatistics>> calculateAverageVaidhy = new CalculateAverage_vaidhy<>(
                diskFileService,
                ChunkProcessorImpl::new,
                CalculateAverage_vaidhy::combineOutputs);

        int proc = ForkJoinPool.commonPool().getParallelism();
        long fileSize = diskFileService.length();
        long chunkSize = Math.ceilDiv(fileSize, proc);
        Map<EfficientString, IntSummaryStatistics> output = calculateAverageVaidhy.master(chunkSize);
        Map<String, String> outputStr = toPrintMap(output);
        System.out.println(outputStr);
    }

    private static Map<String, String> toPrintMap(Map<EfficientString, IntSummaryStatistics> output) {

        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<EfficientString, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(new String(
                    Arrays.copyOf(entry.getKey().arr, entry.getKey().length), StandardCharsets.UTF_8),
                    (stat.getMin() / 10.0) + "/" +
                            (Math.round(stat.getAverage()) / 10.0) + "/" +
                            (stat.getMax() / 10.0));
        }
        return outputStr;
    }

    private static Map<EfficientString, IntSummaryStatistics> combineOutputs(List<Map<EfficientString, IntSummaryStatistics>> list) {
        Map<EfficientString, IntSummaryStatistics> output = new HashMap<>(10000);
        for (Map<EfficientString, IntSummaryStatistics> map : list) {
            for (Map.Entry<EfficientString, IntSummaryStatistics> entry : map.entrySet()) {
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
