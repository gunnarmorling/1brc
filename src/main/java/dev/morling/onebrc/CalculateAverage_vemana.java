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
                14 /* hashtableSizeBits */).getSummaryStatistics());
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

        public Result processChunk() {
            ByteRange range;
            while ((range = chunkQueue.take(threadIdx)) != null) {
                processRange(range);
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

        private void processRange(ByteRange range) {
            MappedByteBuffer mbb = range.byteBuffer;
            int nextPos = 0;
            int end = mbb.capacity();
            while (nextPos < end) {
                nextPos = state.processLine(mbb, nextPos, end);
            }
        }

        private Result result() {
            return state.result();
        }
    }

    public static class ChunkProcessorState {

        private final byte[][] cityNames;
        private final int slotsMask;
        private final Stat[] stats;
        private int hashHits = 0, hashMisses = 0;

        public ChunkProcessorState(int slotsBits) {
            this.stats = new Stat[1 << slotsBits];
            this.cityNames = new byte[1 << slotsBits][];
            this.slotsMask = (1 << slotsBits) - 1;
        }

        public int processLine(MappedByteBuffer mmb, int nextPos, int endPos) {
            int originalPos = nextPos;
            byte nextByte;
            int hash = 0;

            // Read City and Hash
            while ((nextByte = mmb.get(nextPos++)) != ';') {
                hash = (hash << 5) - hash + nextByte;
            }
            int cityLen = nextPos - 1 - originalPos;

            // Read temperature
            int temperature = 0;
            boolean negative = mmb.get(nextPos) == '-';
            if (negative) {
                nextPos++;
            }

            while ((nextByte = mmb.get(nextPos++)) != '\n') {
                if (nextByte != '.') {
                    temperature = temperature * 10 + (nextByte - '0');
                }
            }

            linearProbe(cityLen,
                    hash & slotsMask,
                    negative ? -temperature : temperature,
                    mmb,
                    originalPos);

            return nextPos;
        }

        public Result result() {
            int N = stats.length;
            TreeMap<String, Stat> map = new TreeMap<>();
            for (int i = 0; i < N; i++) {
                if (stats[i] != null) {
                    map.put(new String(cityNames[i]), stats[i]);
                }
            }
            // System.err.println(STR."Hashhits = \{hashHits}, misses = \{hashMisses}");
            return new Result(map);
        }

        private byte[] copyFrom(MappedByteBuffer mmb, int offsetInMmb, int len) {
            byte[] out = new byte[len];
            for (int i = 0; i < len; i++) {
                out[i] = mmb.get(offsetInMmb + i);
            }
            return out;
        }

        private boolean equals(byte[] left, MappedByteBuffer right, int offsetInMmb, int len) {
            for (int i = 0; i < len; i++) {
                if (left[i] != right.get(offsetInMmb + i)) {
                    return false;
                }
            }
            return true;
        }

        // DEATH BY a 1000 cuts
        // 15 ms for stat merging
        // 20 ms for slot probing
        // 20 ms for copying city name into needle (avoided this, but paying in manual arrayequals)
        // 10 ms for hash
        // 5 ms for temperature
        // 1100 ms for just the basic overhead of running through all the data
        // Hash hits are quite good, only about 1% need probing
        private void linearProbe(int len, int hash, int temp, MappedByteBuffer mmb, int offsetInMmb) {
            for (int i = hash;; i = (i + 1) & slotsMask) {
                var curBytes = cityNames[i];
                if (curBytes == null) {
                    // We can no longer do cityNames[i] =Arrays.copyOf(..) since the cityname is encoded
                    // as (mmb, offsetInMmb, len) instead of in a previously allocated byte[]
                    // Even still, this allocation appears very costly
                    cityNames[i] = copyFrom(mmb, offsetInMmb, len);
                    stats[i] = Stat.firstReading(temp);
                    return;
                }
                else {
                    // Overall, this tradeoff seems better than Arrays.equals(..)
                    // City name param is encoded as (mmb, offsetnInMmb, len)
                    // This avoids copying it into a (previously allocated) byte[]
                    // The downside is that we have to manually implement 'equals' and it can lose out
                    // to vectorized 'equals'; but the trade off seems to work in this particular case
                    if (len == curBytes.length && equals(curBytes, mmb, offsetInMmb, len)) {
                        stats[i].mergeReading(temp);
                        return;
                    }
                }
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

        Result getSummaryStatistics() throws Exception {
            int processors = Runtime.getRuntime().availableProcessors();
            SerialLazyChunkQueue chunkQueue = new SerialLazyChunkQueue(1L << chunkSizeBits, inputFile, processors);

            List<Future<Result>> results = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(processors);
            for (int i = 0; i < processors; i++) {
                final int I = i;
                final Callable<Result> callable = () -> new ChunkProcessor(chunkQueue,
                        hashtableSizeBits,
                        I).processChunk();
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

        public static Stat firstReading(int temp) {
            return new Stat(temp, temp, temp, 1);
        }

        public static Stat merge(Stat left, Stat right) {
            return new Stat(
                    Math.min(left.min, right.min),
                    Math.max(left.max, right.max),
                    left.sum + right.sum,
                    left.count + right.count);
        }

        private long count, sum;
        private int min, max;

        public Stat(int min, int max, long sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        // Caution: Mutates
        public void mergeReading(int curTemp) {
            // min = Math.min(min, curTemp);
            // max = Math.max(max, curTemp);

            // Assuming random values for curTemp,
            // min (&max) gets updated roughly log(N)/N fraction of the time (a small number)
            // So, make it branch-predictor friendly
            // In the worst case, there will be at-most one branch misprediction.
            // On Ryzen 5950X this seems to save 0.5s of CPU time across 16 cores over Math.min/max
            if (curTemp > min) { // Mostly passes. On branch misprediction, just update min.
                if (curTemp > max) { // Mostly fails. On branch misprediction, just update max.
                    max = curTemp;
                }
            }
            else {
                min = curTemp;
            }
            sum += curTemp;
            count++;
        }

        @Override
        public String toString() {
            return "%.1f/%.1f/%.1f".formatted(min / 10.0, sum / 10.0 / count, max / 10.0);
        }
    }
}
