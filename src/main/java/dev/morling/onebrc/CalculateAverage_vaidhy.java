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
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
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

    private final FileService fileService;
    private final Supplier<MapReduce<T>> chunkProcessCreator;
    private final Function<List<T>, T> reducer;

    interface MapReduce<T> extends Consumer<EfficientString> {
        T result();
    }

    interface FileService {
        /**
         * Returns the size of the file in number of characters.
         * (Extra credit: assume byte size instead of char size)
         */
        // Possible implementation for byte case in HTTP:
        // byte size = Content-Length header. using HEAD or empty Range.
        long length();

        /**
         * Returns substring of the file from character indices.
         * Expects 0 <= start <= start + length <= fileSize
         * (Extra credit: assume sub-byte array instead of sub char array)
         */
        // Possible implementation for byte case in HTTP:
        // Using Http Request header "Range", typically used for continuing
        // partial downloads.
        byte[] range(long offset, int length);
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
        private long offset;
        private final long scanSize;
        private int index = 0;
        private byte[] currentChunk = new byte[0];
        private final long fileLength;

        public ByteStream(FileService fileService, long offset, int scanSize) {
            this.fileService = fileService;
            this.offset = offset;
            this.scanSize = scanSize;
            this.fileLength = fileService.length();
            if (scanSize <= 0) {
                throw new IllegalArgumentException("scan size must be > 0");
            }
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
        }

        public boolean hasNext() {
            while (index >= currentChunk.length) {
                if (offset < fileLength) {
                    int scanWindow = (int) (Math.min(offset + scanSize, fileLength) - offset);
                    currentChunk = fileService.range(offset, scanWindow);
                    offset += scanWindow;
                    index = 0;
                }
                else {
                    return false;
                }
            }
            return true;
        }

        public byte next() {
            if (hasNext()) {
                byte ch = currentChunk[index];
                index++;
                return ch;
            }
            else {
                throw new NoSuchElementException();
            }
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
            if (hasNext()) {
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
            else {
                throw new NoSuchElementException();
            }
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    public void worker(long offset, long chunkSize, int scanSize, Consumer<EfficientString> lineConsumer) {
        ByteStream byteStream = new ByteStream(fileService, offset, scanSize);
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
    public T master(long chunkSize, int scanSize) {
        long len = fileService.length();
        List<ForkJoinTask<T>> summaries = new ArrayList<>();
        ForkJoinPool commonPool = ForkJoinPool.commonPool();

        for (long offset = 0; offset < len; offset += chunkSize) {
            long workerLength = Math.min(len, offset + chunkSize) - offset;
            MapReduce<T> mr = chunkProcessCreator.get();
            final long transferOffset = offset;
            ForkJoinTask<T> task = commonPool.submit(() -> {
                worker(transferOffset, workerLength, scanSize, mr);
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

        DiskFileService(String fileName) throws IOException {
            this.fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
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
        public byte[] range(long offset, int length) {
            byte[] newArr = new byte[length];
            ByteBuffer outputBuffer = ByteBuffer.wrap(newArr);
            try {
                fileChannel.transferTo(offset, length, new WritableByteChannel() {
                    @Override
                    public int write(ByteBuffer src) {
                        int rem = src.remaining();
                        outputBuffer.put(src);
                        return rem;
                    }

                    @Override
                    public boolean isOpen() {
                        return true;
                    }

                    @Override
                    public void close() {
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return newArr;
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
            return Arrays.equals(this.arr, 0, this.length,
                    eso.arr, 0, eso.length);
        }
    }

    private static final EfficientString EMPTY = new EfficientString(new byte[0], 0);

    public static class ChunkProcessorImpl implements MapReduce<Map<EfficientString, IntSummaryStatistics>> {

        private final Map<EfficientString, IntSummaryStatistics> statistics = new HashMap<>(10000);

        @Override
        public void accept(EfficientString line) {
            EfficientString station = EMPTY;

            int i;
            for (i = 0; i < line.length; i++) {
                if (line.arr[i] == ';') {
                    station = new EfficientString(line.arr, i);
                    break;
                }
            }

            int normalized = parseDoubleNew(line.arr, i + 1, line.length);

            IntSummaryStatistics stats = statistics.get(station);
            if (stats == null) {
                stats = new IntSummaryStatistics();
                statistics.put(station, stats);
            }
            stats.accept(normalized);
        }

        private static int parseDoubleNew(byte[] value, int offset, int length) {
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
        Map<EfficientString, IntSummaryStatistics> output = calculateAverageVaidhy.master(chunkSize,
                Math.min(10 * 1024 * 1024, (int) chunkSize));
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
