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
import java.nio.ByteOrder;
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

    public static void checkArg(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    public static void main(String[] args) throws Exception {
        // First process in large chunks without coordination among threads
        // Use chunkSizeBits for the large-chunk size
        int chunkSizeBits = 20;

        // For the last commonChunkFraction fraction of total work, use smaller chunk sizes
        double commonChunkFraction = 0;

        // Use commonChunkSizeBits for the small-chunk size
        int commonChunkSizeBits = 18;

        // Size of the hashtable (attempt to fit in L3)
        int hashtableSizeBits = 14;

        if (args.length > 0) {
            chunkSizeBits = Integer.parseInt(args[0]);
        }

        if (args.length > 1) {
            commonChunkFraction = Double.parseDouble(args[1]);
        }

        if (args.length > 2) {
            commonChunkSizeBits = Integer.parseInt(args[2]);
        }

        if (args.length > 3) {
            hashtableSizeBits = Integer.parseInt(args[3]);
        }
        //
        // System.err.println(STR."""
        // Using the following parameters:
        // - chunkSizeBits = \{chunkSizeBits}
        // - commonChunkFraction = \{commonChunkFraction}
        // - commonChunkSizeBits = \{commonChunkSizeBits}
        // - hashtableSizeBits = \{hashtableSizeBits}
        // """);

        System.out.println(new Runner(Path.of("measurements.txt"),
                chunkSizeBits,
                commonChunkFraction,
                commonChunkSizeBits,
                hashtableSizeBits).getSummaryStatistics());
    }

    interface LazyShardQueue {

        ByteRange take(int shardIdx);
    }

    // Mutable to avoid allocation
    public static class ByteRange {

        private static final int BUF_SIZE = 1 << 30;

        private final long fileSize;
        private final RandomAccessFile raf;

        // ***************** What this is doing and why *****************
        // Reading from ByteBuffer appears faster from MemorySegment, but ByteBuffer can only be
        // Integer.MAX_VALUE long; Creating one byteBuffer per chunk kills native memory quota
        // and JVM crashes without futher parameters.
        //
        // So, in this solution, create a sliding window of bytebuffers:
        // - Create a large bytebuffer that spans the chunk
        // - If the next chunk falls outside the byteBuffer, create another byteBuffer that spans the
        // chunk. Because chunks are allocated serially, a single large (1<<30) byteBuffer spans
        // many successive chunks.
        // - In fact, for serial chunk allocation (which is friendly to page faulting anyway),
        // the number of created ByteBuffers doesn't exceed [size of shard/(1<<30)] which is less than
        // 100/thread and is comfortably below what the JVM can handle (65K) without further param
        // tuning
        // - This enables (relatively) allocation free chunking implementation. Our chunking impl uses
        // fine grained chunking for the last say X% of work to avoid being hostage to stragglers

        // The PUBLIC API
        public MappedByteBuffer byteBuffer;
        public boolean byteBufferRangeIsExact; // true iff byteBuffer is the exact whole range
        public int endInBuf; // where the chunk ends inside the buffer
        public int startInBuf; // where the chunk starts inside the buffer
        // Private State
        private long bufferEnd; // byteBuffer's ending coordinate
        private long bufferStart; // byteBuffer's begin coordinate

        // Uninitialized; for mutability
        public ByteRange(RandomAccessFile raf) {
            this.raf = raf;
            try {
                this.fileSize = raf.length();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            bufferEnd = bufferStart = -1;
        }

        public void setRange(long rangeStart, long rangeEnd, boolean sizeBufferExactly) {
            if (rangeEnd + 1024 > bufferEnd || rangeStart < bufferStart) {
                bufferStart = rangeStart;
                bufferEnd = Math.min(bufferStart + BUF_SIZE, fileSize);
                setByteBufferToRange(bufferStart, bufferEnd);
            }

            if (rangeStart > 0) {
                rangeStart = 1 + nextNewLine(rangeStart);
            }

            if (rangeEnd < fileSize) {
                rangeEnd = 1 + nextNewLine(rangeEnd);
            }
            else {
                rangeEnd = fileSize;
            }

            if (sizeBufferExactly) {
                bufferStart = rangeStart;
                bufferEnd = rangeEnd;
                setByteBufferToRange(bufferStart, bufferEnd);
            }

            startInBuf = (int) (rangeStart - bufferStart);
            endInBuf = (int) (rangeEnd - bufferStart);
            byteBufferRangeIsExact = sizeBufferExactly;
            // assertInvariants();
        }

    @Override
    public String toString() {
      return STR."""
          ByteRange {
            startInBuf = \{startInBuf}
            endInBuf = \{endInBuf}
          }
          """;
    }

    private void assertInvariants() {
      if (startInBuf > endInBuf) {
        throw new IllegalArgumentException("reversed");
      }
      if (startInBuf < 0 || startInBuf + bufferStart >= bufferEnd) {
        throw new IllegalArgumentException("bufferStart exception");
      }
      if (endInBuf < 0) {
        throw new IllegalArgumentException("negative bufferEnd exception");
      }
      if (bufferStart + endInBuf > bufferEnd) {
        System.err.println(STR."""
        Bad data:
        bufferStart = \{bufferStart}
        endInBuf=\{endInBuf}
        bufferEnd = \{bufferEnd}
        bufferSize = \{bufferEnd - bufferStart}
        """);
        throw new IllegalArgumentException("bad bufferEnd exception");
      }
    }

        private long nextNewLine(long pos) {
            int nextPos = (int) (pos - bufferStart);
            while (byteBuffer.get(nextPos) != '\n') {
                nextPos++;
            }
            return nextPos + bufferStart;
        }

        private void setByteBufferToRange(long start, long end) {
            try {
                byteBuffer = raf.getChannel()
                        .map(MapMode.READ_ONLY, start, end - start);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
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

        private final double commonChunkFraction;
        private final int commonChunkSizeBits;
        private final int hashtableSizeBits;
        private final Path inputFile;
        private final int shardSizeBits;

        public Runner(
                      Path inputFile, int chunkSizeBits, double commonChunkFraction,
                      int commonChunkSizeBits, int hashtableSizeBits) {
            this.inputFile = inputFile;
            this.shardSizeBits = chunkSizeBits;
            this.commonChunkFraction = commonChunkFraction;
            this.commonChunkSizeBits = commonChunkSizeBits;
            this.hashtableSizeBits = hashtableSizeBits;
        }

        Result getSummaryStatistics() throws Exception {
            int processors = Runtime.getRuntime().availableProcessors();
            LazyShardQueue shardQueue = new SerialLazyShardQueue(1L << shardSizeBits,
                    inputFile,
                    processors,
                    commonChunkFraction,
                    commonChunkSizeBits);

            List<Future<Result>> results = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(processors,
                    runnable -> {
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        return thread;
                    });

            long[] finishTimes = new long[processors];

            for (int i = 0; i < processors; i++) {
                final int I = i;
                final Callable<Result> callable = () -> {
                    Result result = new ShardProcessor(shardQueue,
                            hashtableSizeBits,
                            I).processShard();
                    finishTimes[I] = System.nanoTime();
                    return result;
                };
                results.add(executorService.submit(callable));
            }
            // printFinishTimes(finishTimes);
            return executorService.submit(() -> merge(results)).get();
        }

        private Result merge(List<Future<Result>> results)
                throws ExecutionException, InterruptedException {
            Map<String, Stat> output = null;
            boolean[] isDone = new boolean[results.size()];
            int remaining = results.size();
            while (remaining > 0) {
                for (int i = 0; i < results.size(); i++) {
                    if (!isDone[i] && results.get(i).isDone()) {
                        isDone[i] = true;
                        remaining--;
                        if (output == null) {
                            output = new TreeMap<>(results.get(i).get().tempStats());
                        }
                        else {
                            for (Entry<String, Stat> entry : results.get(i).get().tempStats().entrySet()) {
                                output.compute(entry.getKey(), (key, value) -> value == null
                                        ? entry.getValue()
                                        : Stat.merge(value, entry.getValue()));
                            }
                        }
                    }
                }
            }
            return new Result(output);
        }

    private void printFinishTimes(long[] finishTimes) {
      Arrays.sort(finishTimes);
      int n = finishTimes.length;
      System.err.println(STR."Finish time delta between threads: \{(finishTimes[n - 1] - finishTimes[0]) / 1_000_000}ms");
    }
    }

    public static class SerialLazyShardQueue implements LazyShardQueue {

        private static long roundToNearestHigherMultipleOf(long divisor, long value) {
            return (value + divisor - 1) / divisor * divisor;
        }

        private final ByteRange[] byteRanges;
        private final long chunkSize;
        private final long commonChunkSize;
        private final AtomicLong commonPool;
        private final long fileSize;
        private final long[] nextStarts;

        public SerialLazyShardQueue(
                                    long chunkSize, Path filePath, int shards, double commonChunkFraction,
                                    int commonChunkSizeBits)
                throws IOException {
            checkArg(commonChunkFraction < 0.9 && commonChunkFraction >= 0);
            var raf = new RandomAccessFile(filePath.toFile(), "r");
            this.fileSize = raf.length();

            // Common pool
            long commonPoolStart = Math.min(roundToNearestHigherMultipleOf(
                    chunkSize, (long) (fileSize * (1 - commonChunkFraction))),
                    fileSize);
            this.commonPool = new AtomicLong(commonPoolStart);
            this.commonChunkSize = 1L << commonChunkSizeBits;

            // Distribute chunks to shards
            this.nextStarts = new long[shards << 4]; // thread idx -> 16*idx to avoid cache line conflict
            for (long i = 0,
                    currentStart = 0,
                    remainingChunks = (commonPoolStart + chunkSize - 1) / chunkSize; i < shards; i++) {
                long remainingShards = shards - i;
                long currentChunks = (remainingChunks + remainingShards - 1) / remainingShards;
                // Shard i handles: [currentStart, currentStart + currentChunks * chunkSize)
                int pos = (int) i << 4;
                nextStarts[pos] = currentStart;
                nextStarts[pos + 1] = currentStart + currentChunks * chunkSize;
                currentStart += currentChunks * chunkSize;
                remainingChunks -= currentChunks;
            }
            this.chunkSize = chunkSize;

            this.byteRanges = new ByteRange[shards << 4];
            for (int i = 0; i < shards; i++) {
                byteRanges[i << 4] = new ByteRange(raf);
            }
        }

        @Override
        public ByteRange take(int idx) {
            // Try for thread local range
            final int pos = idx << 4;
            long rangeStart = nextStarts[pos];
            final long chunkEnd = nextStarts[pos + 1];

            final boolean sizeExactly;
            final long rangeEnd;

            if (rangeStart < chunkEnd) {
                rangeEnd = rangeStart + chunkSize;
                sizeExactly = false;
                nextStarts[pos] = rangeEnd;
            }
            else {
                rangeStart = commonPool.getAndAdd(commonChunkSize);
                // If that's exhausted too, nothing remains!
                if (rangeStart >= fileSize) {
                    return null;
                }
                rangeEnd = rangeStart + commonChunkSize;
                sizeExactly = false;
            }

            ByteRange chunk = byteRanges[pos];
            chunk.setRange(rangeStart, rangeEnd, sizeExactly);
            return chunk;
        }
    }

    public static class ShardProcessor {

        private final LazyShardQueue shardQueue;
        private final ShardProcessorState state;
        private final int threadIdx;

        public ShardProcessor(LazyShardQueue shardQueue, int hashtableSizeBits, int threadIdx) {
            this.shardQueue = shardQueue;
            this.threadIdx = threadIdx;
            this.state = new ShardProcessorState(hashtableSizeBits);
        }

        public Result processShard() {
            ByteRange range;
            while ((range = shardQueue.take(threadIdx)) != null) {
                processRange(range);
            }
            return result();
        }

        private void processRange(ByteRange range) {
            MappedByteBuffer mmb = range.byteBuffer;
            int nextPos = range.startInBuf;
            int end = range.endInBuf;

            while (nextPos < end) {
                nextPos = state.processLine(mmb, nextPos);
            }
        }

        private Result result() {
            return state.result();
        }
    }

    public static class ShardProcessorState {

        private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

        private final byte[][] cityNames;
        private final int slotsMask;
        private final Stat[] stats;

        public ShardProcessorState(int slotsBits) {
            this.stats = new Stat[1 << slotsBits];
            this.cityNames = new byte[1 << slotsBits][];
            this.slotsMask = (1 << slotsBits) - 1;
        }

        public int processLine(MappedByteBuffer mmb, int nextPos) {
            int originalPos = nextPos;
            byte nextByte;
            int hash = 0;

            // Read City and Hash
            // while ((nextByte = mmb.get(nextPos++)) != ';') {
            // hash = (hash << 5) - hash + nextByte;
            // }

            while (true) {
                int x = mmb.getInt(nextPos);
                if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
                    x = Integer.reverseBytes(x);
                }

                byte a = (byte) (x >>> 24);
                if (a == ';') {
                    nextPos += 1;
                    break;
                }

                byte b = (byte) (x >>> 16);
                if (b == ';') {
                    nextPos += 2;
                    hash = hash * 31 + ((0xFF000000 & x));
                    break;
                }

                byte c = (byte) (x >>> 8);
                if (c == ';') {
                    nextPos += 3;
                    hash = hash * 31 + ((0xFFFF0000 & x));
                    break;
                }

                byte d = (byte) (x >>> 0);
                if (d == ';') {
                    nextPos += 4;
                    hash = hash * 31 + ((0xFFFFFF00 & x));
                    break;
                }

                hash = hash * 31 + x;
                nextPos += 4;
            }
            int cityLen = nextPos - 1 - originalPos;

            // Read temperature
            int temperature = 0;
            boolean negative = (nextByte = mmb.get(nextPos++)) == '-';
            if (!negative) {
                temperature = nextByte - '0';
            }

            while (true) {
                nextByte = mmb.get(nextPos++);
                if (nextByte != '.') {
                    temperature = temperature * 10 + (nextByte - '0');
                }
                else {
                    temperature = temperature * 10 + (mmb.get(nextPos++) - '0');
                    nextPos++;
                    break;
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

        // ***************** Representative timings ****************** //
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
