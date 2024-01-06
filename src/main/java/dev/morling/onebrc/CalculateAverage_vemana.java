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
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CalculateAverage_vemana {

    public static void main(String[] args) throws Exception {
        System.out.println(new Runner(Path.of("measurements.txt"),
                24 /* chunkSizeBits */,
                14 /* hashtableSizeBits */).run());
    }

    public record ByteRange(MappedByteBuffer byteBuffer, long start, long end) {

    }

    public static class ChunkProcessor {

        private final SerialLazyChunkQueue chunkQueue;
        private final ChunkProcessorState state;
        private final int threadIdx;

        public ChunkProcessor(
                              SerialLazyChunkQueue chunkQueue, int hashtableSizeBits, int threadIdx) {
            this.chunkQueue = chunkQueue;
            this.threadIdx = threadIdx;
            this.state = new ChunkProcessorState(hashtableSizeBits);
        }

        public Result process() {
            ByteRange range;
            while ((range = chunkQueue.take(threadIdx)) != null) {
                process(range);
                // justRead(range);
            }
            return result();
        }

        private byte justRead(ByteRange range) {
            MappedByteBuffer mmb = range.byteBuffer;
            int pos = 0, len = mmb.capacity();
            // System.err.println(STR."""
            // Handling byterange in Thread \{threadIdx}: \{range.start()} to \{range.end()}.
            // Capacity = \{mmb.capacity()}
            // Limit = \{mmb.limit()}
            // """);
            byte ret = 0;
            while (pos < len) {
                ret |= mmb.get(pos++);
            }
            return ret;
        }

        private void process(ByteRange range) {
            MappedByteBuffer mbb = range.byteBuffer;
            int nextPos = 0;
            int end = mbb.capacity();
            while (nextPos < end) {
                nextPos = state.addDataPoint(mbb, nextPos, end);
            }
        }

        private Result result() {
            return state.result();
        }
    }

    public static class ChunkProcessorState {

        private final byte[] cityBytes = new byte[512]; // should fit a city name, 4bytes/UTF-8 char
        private final byte[][] cityNames;
        private final int slotsMask;
        private final Stat[] stats;

        public ChunkProcessorState(int slotsBits) {
            this.stats = new Stat[1 << slotsBits];
            this.cityNames = new byte[1 << slotsBits][];
            this.slotsMask = (1 << slotsBits) - 1;
        }

        public int addDataPoint(MappedByteBuffer mbb, int nextPos, int endPos) {
            byte nextByte;

            int prevPos = nextPos - 1;
            int hash = 0;

            // Read City
            int cityLen = 0;
            while ((nextByte = mbb.get(++prevPos)) != ';') {
                cityBytes[cityLen++] = nextByte;
                hash = (hash << 5) - hash + nextByte;
            }

            // Read temperature
            long temperature = 0;
            boolean negative = mbb.get(prevPos + 1) == '-';
            if (negative) {
                ++prevPos;
            }

            while ((nextByte = mbb.get(++prevPos)) != '\n') {
                if (nextByte != '.') {
                    temperature = temperature * 10 + (nextByte - '0');
                }
            }

            recordDataPoint(cityLen, hash & slotsMask, negative ? -temperature : temperature);

            return prevPos + 1;
        }

        public Result result() {
            int N = stats.length;
            TreeMap<String, Stat> map = new TreeMap<>();
            for (int i = 0; i < N; i++) {
                if (stats[i] != null) {
                    map.put(new String(cityNames[i]), stats[i]);
                }
            }
            return new Result(map);
        }

        private int hash(int len) {
            int hash = 0;
            for (int i = 0; i < len; i++) {
                hash = (hash << 5) - hash + cityBytes[i];
            }
            return hash & slotsMask;
        }

        private int linearProbe(int len, int hash) {
            for (int i = hash;; i = (i + 1) & slotsMask) {
                var curBytes = cityNames[i];
                if (curBytes == null) {
                    cityNames[i] = Arrays.copyOf(cityBytes, len);
                    return i;
                }
                else {
                    if (len == curBytes.length && Arrays.equals(cityBytes, 0, len, curBytes, 0, len)) {
                        return i;
                    }
                }
            }
        }

        private void recordDataPoint(int length, int hash, long temp) {
            int index = linearProbe(length, hash);
            var stat = stats[index];
            if (stat == null) {
                stats[index] = Stat.firstReading(temp);
            }
            else {
                stat.mergeReading(temp);
            }
        }
    }

  public record Result(Map<String, Stat> tempStats) {

    @Override
    public String toString() {
      return this.tempStats().entrySet().stream().sorted(Entry.comparingByKey())
                 .map(entry -> "%s=%s".formatted(entry.getKey(), entry.getValue()))
                 .collect(Collectors.joining(", ", "{", "}"));
    }
  }

    public static class Runner {

        private final int chunkSizeBits;
        private final int hashtableSizeBits;
        private final Path inputFile;

        public Runner(Path inputFile, int chunkSizeBits, int hashtableSizeBits) {
            this.inputFile = inputFile;
            this.chunkSizeBits = chunkSizeBits;
            this.hashtableSizeBits = hashtableSizeBits;
        }

        Result run() throws Exception {
            int processors = Runtime.getRuntime().availableProcessors();
            SerialLazyChunkQueue chunkQueue = new SerialLazyChunkQueue(1L << chunkSizeBits, inputFile, processors);

            List<Future<Result>> results = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(processors);
            for (int i = 0; i < processors; i++) {
                final int I = i;
                final Callable<Result> callable = () -> new ChunkProcessor(chunkQueue,
                        hashtableSizeBits,
                        I).process();
                results.add(executorService.submit(callable));
            }
            executorService.shutdown();
            return merge(results);
        }

        private Result merge(List<Future<Result>> results)
                throws ExecutionException, InterruptedException {
            // Merge the results
            Map<String, Stat> map = null;
            for (int i = 0; i < results.size(); i++) {
                if (i == 0) {
                    map = new TreeMap<>(results.get(0).get().tempStats());
                }
                else {
                    for (Entry<String, Stat> entry : results.get(i).get().tempStats().entrySet()) {
                        map.compute(entry.getKey(), (key, value) -> value == null
                                ? entry.getValue()
                                : Stat.merge(value, entry.getValue()));
                    }
                }
            }
            return new Result(map);
        }
    }

    public static class SerialLazyChunkQueue {

        private static long roundToNearestHigherMultipleOf(long divisor, long value) {
            return (value + divisor - 1) / divisor * divisor;
        }

        private final long chunkSize;
        private final AtomicLong commonPool;
        private final long commonPoolStart;
        private final long fileSize;
        private final long jumpSize;
        private final MemorySegment memory;
        private final long[] nextStarts;
        private final RandomAccessFile raf;
        private final int threads;

        public SerialLazyChunkQueue(long chunkSize, Path filePath, int threads) throws IOException {
            this.chunkSize = chunkSize;
            this.raf = new RandomAccessFile(filePath.toFile(), "r");
            this.fileSize = raf.length();
            this.memory = raf.getChannel().map(MapMode.READ_ONLY, 0, fileSize, Arena.global());
            this.threads = threads;
            this.jumpSize = chunkSize * threads;
            this.nextStarts = new long[threads * 8]; // thread idx -> 8*idx to avoid cache line conflict
            this.commonPoolStart = Math.min(roundToNearestHigherMultipleOf(chunkSize,
                    fileSize * 90 / 100),
                    fileSize);
            this.commonPool = new AtomicLong(commonPoolStart);
            for (int i = 0; i < threads; i++) {
                nextStarts[i << 3] = chunkSize * i;
            }
        }

        public ByteRange take(int idx) {
            // Try for thread local range
            int pos = idx << 3;
            long rangeStart = nextStarts[pos];
            nextStarts[pos] += jumpSize;

            // If that's over, try from shared range
            if (rangeStart >= commonPoolStart) {
                rangeStart = commonPool.getAndAdd(chunkSize);
                // If that's exhausted too, nothing remains!
                if (rangeStart >= fileSize) {
                    return null;
                }
            }

            // Align to line boundaries
            if (rangeStart > 0) {
                rangeStart = 1 + nextNewLine(rangeStart, fileSize);
            }

            long rangeEnd = Math.min(rangeStart + chunkSize, fileSize);
            if (rangeEnd < fileSize) {
                rangeEnd = 1 + nextNewLine(rangeEnd, fileSize);
            }

            try {
                return new ByteRange(raf.getChannel()
                        .map(MapMode.READ_ONLY, rangeStart, rangeEnd - rangeStart),
                        rangeStart, rangeEnd);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private long nextNewLine(long position, long maxPos) {
            while (position < maxPos && memory.get(ValueLayout.JAVA_BYTE, position) != '\n') {
                position++;
            }
            return position;
        }
    }

    public static class Stat {

        public static Stat firstReading(long temp) {
            return new Stat(temp, temp, temp, 1);
        }

        public static Stat merge(Stat left, Stat right) {
            return new Stat(
                    Math.min(left.min, right.min),
                    Math.max(left.max, right.max),
                    left.sum + right.sum,
                    left.count + right.count);
        }

        private long count;
        private long min, max, sum;

        public Stat(long min, long max, long sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        // Caution: Mutates
        public void mergeReading(long curTemp) {
            min = Math.min(min, curTemp);
            max = Math.max(max, curTemp);
            sum += curTemp;
            count++;
        }

        @Override
        public String toString() {
            return "%.1f/%.1f/%.1f".formatted(min / 10.0, sum / 10.0 / count, max / 10.0);
        }
    }
}
