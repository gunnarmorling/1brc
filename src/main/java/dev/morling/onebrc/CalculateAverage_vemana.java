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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This submission focuses on exploiting the non-SIMD parallelism that is inherent in OOO
 * super-scalar CPUs and avoids using Unsafe, SWAR and other such fine techniques. The hope is to
 * remain readable for a majority of SWEs. At a high level, the approach relies on a few principles
 * listed herein.
 *
 * <p>[Exploit Parallelism] Distribute the work into Shards. Separate threads (one per core) process
 * Shards and follow it up by merging the results. parallelStream() is appealing but carries
 * potential run-time variance (i.e. std. deviation) penalties based on informal testing. Variance
 * is not ideal when trying to minimize the maximum worker latency.
 *
 * <p>[Understand that unmapping is serial and runs in exit()]. This is very much about exploiting
 * parallelism. After adding tracing (plain old printfs), it was clear that the JVM was taking 400ms
 * (out of 1500ms) just to exit. Turns out that the kernel tries to unmap all the mappings as part
 * of the exit() call. Even strace wouldn't report this because the unmapping is running as part of
 * the exit() call. perf stat barely hinted at it, but we had more insights by actually running a
 * couple of experiments: reduce touched pages --> JVM shutdown latency went down; manually run
 * unmap() call to free up the ByteBuffers --> parallel execution doesn't help at all. From this it
 * was conclusive that unmap() executes serially and the 400ms was being spent purely unmapping.
 * Now, the challenge is to both (1) unmap a MappedByteBuffer (no such methods exposed) from code
 * rather than via exit() syscall and (2) do it in parallel without causing lock contention. For 1,
 * use Reflection and (2) is an interesting math problem with a provably optimal solution.
 * Parallelism in munmap() is achieved by using a fast lock that prevents two threads from
 * simultaneously cleaning (i.e. munmap()) the ByteBuffer.
 *
 * <p>[Use ByteBuffers over MemorySegment] Each Shard is further divided in Chunks. This would've
 * been unnecessary except that Shards are too big to be backed by ByteBuffers. Besides,
 * MemorySegment appears slower than ByteBuffers. So, to use ByteBuffers, we have to use smaller
 * chunks.
 *
 * <p>[Straggler freedom] The optimization function here is to minimize the maximal worker thread
 * completion. Law of large number averages means that all the threads will end up with similar
 * amounts of work and similar completion times; but, however ever so often there could be a bad
 * sharding and more importantly, Cores are not created equal; some will be throttled more than
 * others. So, we have a shared {@code LazyShardQueue} that aims to distribute work to minimize the
 * latest completion time.
 *
 * <p>[Work Assignment with LazyShardQueue] The queue provides each thread with its next big-chunk
 * until X% of the work remains. Big-chunks belong to the thread and will not be provided to another
 * thread. Then, it switches to providing small-chunk sizes. Small-chunks comprise the last X% of
 * work and every thread can participate in completing the chunk. Even though the queue is shared
 * across threads, there's no communication across thread during the big-chunk phases. The queue is
 * effectively a per-thread queue while processing big-chunks. The small-chunk phase uses an
 * AtomicLong to coordinate chunk allocation across threads.
 *
 * <p>[Chunk processing] Chunk processing is typical. Process line by line. Find a hash function
 * (polynomial hash fns are slow, but will work fine), hash the city name, resolve conflicts using
 * linear probing and then accumulate the temperature into the appropriate hash slot. The key
 * element then is how fast can you identify the hash slot, read the temperature and update the new
 * temperature in the slot (i.e. min, max, count).
 *
 * <p>[Cache friendliness] 7502P and my machine (7950X) offer 4MB L3 cache/core. This means we can
 * hope to fit all our datastructures in L3 cache. Since SMT is turned on, the Runtime's available
 * processors will show twice the number of actual cores and so we get 2MB L3 cache/thread. To be
 * safe, we try to stay within 1.8 MB/thread and size our hashtable appropriately.
 *
 * <p>[Native ByteOrder is MUCH better] There was almost a 10% lift by reading ints from bytebuffers
 * using native byteorder . It so happens that both the eval machine (7502P) and my machine 7950X
 * use native LITTLE_ENDIAN order, which again apparently is because X86[-64] is little-endian. But,
 * by default, ByteBuffers use BIG_ENDIAN order, which appears to be a somewhat strange default from
 * Java.
 *
 * <p>[Allocation] Since MemorySegment seemed slower than ByteBuffers, backing Chunks by bytebuffers
 * was the logical option. Creating one ByteBuffer per chunk was no bueno because the system doesn't
 * like it (JVM runs out of mapped file handle quota). Other than that, allocation in the hot path
 * was avoided.
 *
 * <p>[General approach to fast hashing and temperature reading] Here, it helps to understand the
 * various bottlenecks in execution. One particular thing that I kept coming back to was to
 * understand the relative costs of instructions: See
 * https://www.agner.org/optimize/instruction_tables.pdf It is helpful to think of hardware as a
 * smart parallel execution machine that can do several operations in one cycle if only you can feed
 * it. So, the idea is to reduce data-dependency chains in the bottleneck path. The other major idea
 * is to just avoid unnecessary work. For example, copying the city name into a byte array just for
 * the purpose of looking it up was costing a noticeable amount. Instead, encoding it as
 * (bytebuffer, start, len) was helpful. Spotting unnecessary work was non-trivial.So, them pesky
 * range checks? see if you can avoid them. For example, sometimes you can eliminate a "nextPos <
 * endPos" in a tight loop by breaking it into two pieces: one piece where the check will not be
 * needed and a tail piece where it will be needed.
 *
 * <p>[Understand What Cores like]. Cores like to go straight and loop back. Despite good branch
 * prediction, performance sucks with mispredicted branches.
 *
 * <p>[JIT] Java performance requires understanding the JIT. It is helpful to understand what the
 * JIT likes though it is still somewhat of a mystery to me. In general, it inlines small methods
 * very well and after constant folding, it can optimize quite well across a reasonably deep call
 * chain. My experience with the JIT was that everything I tried to tune it made it worse except for
 * one parameter. I have a new-found respect for JIT - it likes and understands typical Java idioms.
 *
 * <p>[Tuning] Nothing was more insightful than actually playing with various tuning parameters. I
 * can have all the theories but the hardware and JIT are giant blackboxes. I used a bunch of tools
 * to optimize: (1) Command line parameters to tune big and small chunk sizes etc. This was also
 * very helpful in forming a mental model of the JIT. Sometimes, it would compile some methods and
 * sometimes it would just run them interpreted since the compilation threshold wouldn't be reached
 * for intermediate methods. (2) AsyncProfiler - this was the first line tool to understand cache
 * misses and cpu time to figure where to aim the next optimization effort. (3) JitWatch -
 * invaluable for forming a mental model and attempting to tune the JIT.
 *
 * <p>[Things that didn't work]. This is a looong list and the hit rate is quite low. In general,
 * removing unnecessary work had a high hit rate and my pet theories on how things work in hardware
 * had a low hit rate. Java Vector API lacked a good gather API to load from arbitrary memory
 * addresses; this prevented performing real SIMD on the entire dataset, one where you load 64 bytes
 * from 64 different line starting positions (just after a previous line end) and then step through
 * one byte at a time. This to me is the most natural SIMD approach to the 1BRC problem but I
 * couldn't use it. I tried other local uses of the vector API and it was always slower, and mostly
 * much slower. In other words, Java Vector API needs a problem for which it is suited (duh) but,
 * unless I am overlooking something, the API still lacks gather from arbitrary memory addresses.
 *
 * <p>[My general takeaways]. Write simple, idiomatic java code and get 70-80% of the max
 * performance of an optimally hand-tuned code. Focus any optimization efforts on being friendly to
 * the JIT *before* thinking about tuning for the hardware. There's a real cost to EXTREME
 * performance tuning: a loss of abstraction and maintainability, but being JIT friendly is probably
 * much more achievable without sacrificing abstraction.
 */
public class CalculateAverage_vemana {

    public static void main(String[] args) throws Exception {
        Tracing.recordAppStart();
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    Tracing.recordEvent("In Shutdown hook");
                                }));

        // First process in large chunks without coordination among threads
        // Use chunkSizeBits for the large-chunk size
        int chunkSizeBits = 20;

        // For the last commonChunkFraction fraction of total work, use smaller chunk sizes
        double commonChunkFraction = 0;

        // Use commonChunkSizeBits for the small-chunk size
        int commonChunkSizeBits = 18;

        // Size of the hashtable (attempt to fit in L3)
        int hashtableSizeBits = 14;

        int minReservedBytesAtFileTail = 9;

        String inputFile = "measurements.txt";

        for (String arg : args) {
            String key = arg.substring(0, arg.indexOf('='));
            String value = arg.substring(key.length() + 1);
            switch (key) {
                case "chunkSizeBits":
                    chunkSizeBits = Integer.parseInt(value);
                    break;
                case "commonChunkFraction":
                    commonChunkFraction = Double.parseDouble(value);
                    break;
                case "commonChunkSizeBits":
                    commonChunkSizeBits = Integer.parseInt(value);
                    break;
                case "hashtableSizeBits":
                    hashtableSizeBits = Integer.parseInt(value);
                    break;
                case "inputfile":
                    inputFile = value;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        // System.err.println(STR."""
        // Using the following parameters:
        // - chunkSizeBits = \{chunkSizeBits}
        // - commonChunkFraction = \{commonChunkFraction}
        // - commonChunkSizeBits = \{commonChunkSizeBits}
        // - hashtableSizeBits = \{hashtableSizeBits}
        // """);

        System.out.println(
                new Runner(
                        Path.of(inputFile),
                        chunkSizeBits,
                        commonChunkFraction,
                        commonChunkSizeBits,
                        hashtableSizeBits,
                        minReservedBytesAtFileTail)
                                .getSummaryStatistics());

        Tracing.recordEvent("After printing result");
    }

  public record AggregateResult(Map<String, Stat> tempStats) {

    @Override
    public String toString() {
      return this.tempStats().entrySet().stream()
          .sorted(Entry.comparingByKey())
          .map(entry -> "%s=%s".formatted(entry.getKey(), entry.getValue()))
          .collect(Collectors.joining(", ", "{", "}"));
    }
  }

    // Mutable to avoid allocation
    public static class ByteRange {

        private static final int BUF_SIZE = 1 << 30;

        private final long fileSize;
        private final long maxEndPos; // Treat as if the file ends here
        private final RandomAccessFile raf;
        private final List<MappedByteBuffer> unclosedBuffers = new ArrayList<>();

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
        public int endInBuf; // where the chunk ends inside the buffer
        public int startInBuf; // where the chunk starts inside the buffer
        // Private State
        private long bufferEnd; // byteBuffer's ending coordinate
        private long bufferStart; // byteBuffer's begin coordinate

        // Uninitialized; for mutability
        public ByteRange(RandomAccessFile raf, long maxEndPos) {
            this.raf = raf;
            this.maxEndPos = maxEndPos;
            try {
                this.fileSize = raf.length();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            bufferEnd = bufferStart = -1;
        }

        public void close(int shardIdx) {
            Tracing.recordWorkStart("cleaner", shardIdx);
            if (byteBuffer != null) {
                unclosedBuffers.add(byteBuffer);
            }
            for (MappedByteBuffer buf : unclosedBuffers) {
                close(buf);
            }
            unclosedBuffers.clear();
            bufferEnd = bufferStart = -1;
            byteBuffer = null;
            Tracing.recordWorkEnd("cleaner", shardIdx);
        }

        public void setRange(long rangeStart, long rangeEnd) {
            if (rangeEnd + 1024 > bufferEnd || rangeStart < bufferStart) {
                bufferStart = rangeStart;
                bufferEnd = Math.min(bufferStart + BUF_SIZE, fileSize);
                setByteBufferToRange(bufferStart, bufferEnd);
            }

            if (rangeStart > 0) {
                rangeStart = 1 + nextNewLine(rangeStart);
            }
            else {
                rangeStart = 0;
            }

            if (rangeEnd < maxEndPos) {
                rangeEnd = 1 + nextNewLine(rangeEnd);
            }
            else {
                rangeEnd = maxEndPos;
            }

            startInBuf = (int) (rangeStart - bufferStart);
            endInBuf = (int) (rangeEnd - bufferStart);
        }

    @Override
    public String toString() {
      return STR."""
        ByteRange {
          bufferStart = \{bufferStart}
          bufferEnd = \{bufferEnd}
          startInBuf = \{startInBuf}
          endInBuf = \{endInBuf}
        }
        """;
    }

        private void close(MappedByteBuffer buffer) {
            Method cleanerMethod = Reflection.findMethodNamed(buffer, "cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = Reflection.invoke(buffer, cleanerMethod);

            Method cleanMethod = Reflection.findMethodNamed(cleaner, "clean");
            cleanMethod.setAccessible(true);
            Reflection.invoke(cleaner, cleanMethod);
        }

        private long nextNewLine(long pos) {
            int nextPos = (int) (pos - bufferStart);
            while (byteBuffer.get(nextPos) != '\n') {
                nextPos++;
            }
            return nextPos + bufferStart;
        }

        private void setByteBufferToRange(long start, long end) {
            if (byteBuffer != null) {
                unclosedBuffers.add(byteBuffer);
            }
            try {
                byteBuffer = raf.getChannel().map(MapMode.READ_ONLY, start, end - start);
                byteBuffer.order(ByteOrder.nativeOrder());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class Checks {

        public static void checkArg(boolean condition) {
            if (!condition) {
                throw new IllegalArgumentException();
            }
        }

        private Checks() {
        }
    }

    public interface LazyShardQueue {

        void close(int shardIdx);

        Optional<ByteRange> fileTailEndWork(int idx);

        ByteRange take(int shardIdx);
    }

    static final class Reflection {

        static Method findMethodNamed(Object object, String name, Class... paramTypes) {
            try {
                return object.getClass().getMethod(name, paramTypes);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        static Object invoke(Object receiver, Method method, Object... params) {
            try {
                return method.invoke(receiver, params);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Runner {

        private final double commonChunkFraction;
        private final int commonChunkSizeBits;
        private final int hashtableSizeBits;
        private final Path inputFile;
        private final int minReservedBytesAtFileTail;
        private final int shardSizeBits;

        public Runner(
                      Path inputFile,
                      int chunkSizeBits,
                      double commonChunkFraction,
                      int commonChunkSizeBits,
                      int hashtableSizeBits,
                      int minReservedBytesAtFileTail) {
            this.inputFile = inputFile;
            this.shardSizeBits = chunkSizeBits;
            this.commonChunkFraction = commonChunkFraction;
            this.commonChunkSizeBits = commonChunkSizeBits;
            this.hashtableSizeBits = hashtableSizeBits;
            this.minReservedBytesAtFileTail = minReservedBytesAtFileTail;
        }

        AggregateResult getSummaryStatistics() throws Exception {
            int nThreads = Runtime.getRuntime().availableProcessors();
            LazyShardQueue shardQueue = new SerialLazyShardQueue(
                    1L << shardSizeBits,
                    inputFile,
                    nThreads,
                    commonChunkFraction,
                    commonChunkSizeBits,
                    minReservedBytesAtFileTail);

            List<Future<AggregateResult>> results = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(
                    nThreads,
                    runnable -> {
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        return thread;
                    });

            for (int i = 0; i < nThreads; i++) {
                final int shardIdx = i;
                final Callable<AggregateResult> callable = () -> {
                    Tracing.recordWorkStart("shard", shardIdx);
                    AggregateResult result = new ShardProcessor(shardQueue, hashtableSizeBits, shardIdx).processShard();
                    Tracing.recordWorkEnd("shard", shardIdx);
                    return result;
                };
                results.add(executorService.submit(callable));
            }
            Tracing.recordEvent("Basic push time");

            AggregateResult result = executorService.submit(() -> merge(results)).get();

            Tracing.recordEvent("Merge results received");

            // Note that munmap() is serial and not parallel
            executorService.submit(
                    () -> {
                        for (int i = 0; i < nThreads; i++) {
                            shardQueue.close(i);
                        }
                    });

            Tracing.recordEvent("Waiting for executor shutdown");

            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            Tracing.recordEvent("Executor terminated");
            Tracing.analyzeWorkThreads("cleaner", nThreads);
            Tracing.recordEvent("After cleaner finish printed");

            return result;
        }

        private AggregateResult merge(List<Future<AggregateResult>> results)
                throws ExecutionException, InterruptedException {
            Tracing.recordEvent("Merge start time");
            Map<String, Stat> output = null;
            boolean[] isDone = new boolean[results.size()];
            int remaining = results.size();
            // Let's be naughty and spin in a busy loop
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
                                output.compute(
                                        entry.getKey(),
                                        (key, value) -> value == null ? entry.getValue() : Stat.merge(value, entry.getValue()));
                            }
                        }
                    }
                }
            }
            Tracing.recordEvent("Merge end time");
            Tracing.analyzeWorkThreads("shard", results.size());
            return new AggregateResult(output);
        }
    }

    public static class SerialLazyShardQueue implements LazyShardQueue {

        private static long roundToNearestLowerMultipleOf(long divisor, long value) {
            return value / divisor * divisor;
        }

        private final ByteRange[] byteRanges;
        private final long chunkSize;
        private final long commonChunkSize;
        private final AtomicLong commonPool;
        private final long effectiveFileSize;
        private final long fileSize;
        private final long[] perThreadData;
        private final RandomAccessFile raf;
        private final SeqLock seqLock;

        public SerialLazyShardQueue(
                                    long chunkSize,
                                    Path filePath,
                                    int shards,
                                    double commonChunkFraction,
                                    int commonChunkSizeBits,
                                    int fileTailReservedBytes)
                throws IOException {
            Checks.checkArg(commonChunkFraction < 0.9 && commonChunkFraction >= 0);
            Checks.checkArg(fileTailReservedBytes >= 0);
            this.raf = new RandomAccessFile(filePath.toFile(), "r");
            this.fileSize = raf.length();
            fileTailReservedBytes = fileTailReservedBytes == 0
                    ? 0
                    : consumeToPreviousNewLineExclusive(raf, fileTailReservedBytes);
            this.effectiveFileSize = fileSize - fileTailReservedBytes;

            // Common pool
            long commonPoolStart = Math.min(
                    roundToNearestLowerMultipleOf(
                            chunkSize, (long) (effectiveFileSize * (1 - commonChunkFraction))),
                    effectiveFileSize);
            this.commonPool = new AtomicLong(commonPoolStart);
            this.commonChunkSize = 1L << commonChunkSizeBits;

            // Distribute chunks to shards
            this.perThreadData = new long[shards << 4]; // thread idx -> 16*idx to avoid cache line conflict
            for (long i = 0,
                    currentStart = 0,
                    remainingChunks = (commonPoolStart + chunkSize - 1) / chunkSize; i < shards; i++) {
                long remainingShards = shards - i;
                long currentChunks = (remainingChunks + remainingShards - 1) / remainingShards;
                // Shard i handles: [currentStart, currentStart + currentChunks * chunkSize)
                int pos = (int) i << 4;
                perThreadData[pos] = currentStart; // next chunk begin
                perThreadData[pos + 1] = currentStart + currentChunks * chunkSize; // shard end
                perThreadData[pos + 2] = currentChunks; // active chunks remaining
                // threshold below which need to shrink
                // 0.03 is a practical number but the optimal strategy is this:
                // Shard number N (1-based) should unmap as soon as it completes (R/(R+1))^N fraction of
                // its work, where R = relative speed of unmap compared to the computation.
                // For our problem, R ~ 75 because unmap unmaps 30GB/sec (but, it is serial) while
                // cores go through data at the rate of 400MB/sec.
                perThreadData[pos + 3] = (long) (currentChunks * (0.03 * (shards - i)));
                perThreadData[pos + 4] = 1;
                currentStart += currentChunks * chunkSize;
                remainingChunks -= currentChunks;
            }
            this.chunkSize = chunkSize;

            this.byteRanges = new ByteRange[shards << 4];
            for (int i = 0; i < shards; i++) {
                byteRanges[i << 4] = new ByteRange(raf, effectiveFileSize);
            }

            this.seqLock = new SeqLock();
        }

        @Override
        public void close(int shardIdx) {
            byteRanges[shardIdx << 4].close(shardIdx);
        }

        @Override
        public Optional<ByteRange> fileTailEndWork(int idx) {
            if (idx == 0 && effectiveFileSize < fileSize) {
                ByteRange chunk = new ByteRange(raf, fileSize);
                chunk.setRange(
                        effectiveFileSize == 0 ? 0 : effectiveFileSize - 1 /* will consume newline at eFS-1 */,
                        fileSize);
                return Optional.of(chunk);
            }
            return Optional.empty();
        }

        @Override
        public ByteRange take(int shardIdx) {
            // Try for thread local range
            final int pos = shardIdx << 4;
            long rangeStart = perThreadData[pos];
            final long chunkEnd = perThreadData[pos + 1];
            final long rangeEnd;

            if (rangeStart < chunkEnd) {
                rangeEnd = rangeStart + chunkSize;
                perThreadData[pos] = rangeEnd;
                perThreadData[pos + 2]--;
            }
            else {
                rangeStart = commonPool.getAndAdd(commonChunkSize);
                // If that's exhausted too, nothing remains!
                if (rangeStart >= effectiveFileSize) {
                    return null;
                }
                rangeEnd = rangeStart + commonChunkSize;
            }

            if (perThreadData[pos + 2] <= perThreadData[pos + 3] && perThreadData[pos + 4] > 0) {
                if (attemptClose(shardIdx)) {
                    perThreadData[pos + 4]--;
                }
            }

            ByteRange chunk = byteRanges[pos];
            chunk.setRange(rangeStart, rangeEnd);
            return chunk;
        }

        private boolean attemptClose(int shardIdx) {
            if (seqLock.acquire()) {
                byteRanges[shardIdx << 4].close(shardIdx);
                seqLock.release();
                return true;
            }
            return false;
        }

        private int consumeToPreviousNewLineExclusive(RandomAccessFile raf, int minReservedBytes) {
            try {
                long pos = Math.max(raf.length() - minReservedBytes - 1, -1);
                if (pos < 0) {
                    return (int) raf.length();
                }

                long start = Math.max(pos - 512, 0);
                ByteBuffer buf = raf.getChannel().map(MapMode.READ_ONLY, start, pos + 1 - start);
                while (pos >= 0 && buf.get((int) (pos - start)) != '\n') {
                    pos--;
                }
                pos++;
                return (int) (raf.length() - pos);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** A low-traffic non-blocking lock. */
    static class SeqLock {

        private final AtomicBoolean isOccupied = new AtomicBoolean(false);

        boolean acquire() {
            return !isOccupied.get() && isOccupied.compareAndSet(false, true);
        }

        void release() {
            isOccupied.set(false);
        }
    }

    public static class ShardProcessor {

        private final int shardIdx;
        private final LazyShardQueue shardQueue;
        private final ShardProcessorState state;

        public ShardProcessor(LazyShardQueue shardQueue, int hashtableSizeBits, int shardIdx) {
            this.shardQueue = shardQueue;
            this.shardIdx = shardIdx;
            this.state = new ShardProcessorState(hashtableSizeBits);
        }

        public AggregateResult processShard() {
            return processShardReal();
        }

        public AggregateResult processShardReal() {
            // First process the file tail work to give ourselves freedom to go past ranges in parsing
            shardQueue.fileTailEndWork(shardIdx).ifPresent(this::processRangeSlow);

            ByteRange range;
            while ((range = shardQueue.take(shardIdx)) != null) {
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

        private void processRangeSlow(ByteRange range) {
            int nextPos = range.startInBuf;
            while (nextPos < range.endInBuf) {
                nextPos = state.processLineSlow(range.byteBuffer, nextPos);
            }
        }

        private AggregateResult result() {
            return state.result();
        }
    }

    public static class ShardProcessorState {

        public static final long ONE_MASK = 0x0101010101010101L;
        private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
        private static final long SEMICOLON_MASK = 0x3b3b3b3b3b3b3b3bL;
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

            while (true) {
                int x = mmb.getInt(nextPos);
                if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
                    x = Integer.reverseBytes(x);
                }

                byte a = (byte) (x >>> 0);
                if (a == ';') {
                    nextPos += 1;
                    break;
                }

                byte b = (byte) (x >>> 8);
                if (b == ';') {
                    nextPos += 2;
                    hash = hash * 31 + (0xFF & x);
                    break;
                }

                byte c = (byte) (x >>> 16);
                if (c == ';') {
                    nextPos += 3;
                    hash = hash * 31 + (0xFFFF & x);
                    break;
                }

                byte d = (byte) (x >>> 24);
                if (d == ';') {
                    nextPos += 4;
                    hash = hash * 31 + (0xFFFFFF & x);
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
            else {
                temperature = mmb.get(nextPos++) - '0';
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

            linearProbe(
                    cityLen, hash & slotsMask, negative ? -temperature : temperature, mmb, originalPos);

            return nextPos;
        }

        /** A slow version which is used only for the tail part of the file. */
        public int processLineSlow(MappedByteBuffer mmb, int nextPos) {
            int originalPos = nextPos;
            byte nextByte;
            int hash = 0;

            outer: while (true) {
                int accumulated = 0;
                for (int i = 0; i < 4; i++) {
                    nextByte = mmb.get(nextPos++);
                    if (nextByte == ';') {
                        if (i > 0) {
                            hash = hash * 31 + accumulated;
                        }
                        break outer;
                    }
                    else {
                        accumulated |= ((int) nextByte << (8 * i));
                    }
                }
                hash = hash * 31 + accumulated;
            }
            int cityLen = nextPos - 1 - originalPos;

            int temperature = 0;
            boolean negative = mmb.get(nextPos) == '-';
            while ((nextByte = mmb.get(nextPos++)) != '\n') {
                if (nextByte != '-' && nextByte != '.') {
                    temperature = temperature * 10 + (nextByte - '0');
                }
            }

            linearProbe(
                    cityLen, hash & slotsMask, negative ? -temperature : temperature, mmb, originalPos);

            return nextPos;
        }

        public AggregateResult result() {
            int N = stats.length;
            Map<String, Stat> map = new LinkedHashMap<>(5_000);
            for (int i = 0; i < N; i++) {
                if (stats[i] != null) {
                    map.put(new String(cityNames[i]), stats[i]);
                }
            }
            return new AggregateResult(map);
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

        private boolean hasSemicolonByte(long value) {
            long a = value ^ SEMICOLON_MASK;
            return (((a - ONE_MASK) & ~a) & (0x8080808080808080L)) != 0;
        }

        private void linearProbe(int len, int hash, int temp, MappedByteBuffer mmb, int offsetInMmb) {
            for (int i = hash;; i = (i + 1) & slotsMask) {
                var curBytes = cityNames[i];
                if (curBytes == null) {
                    cityNames[i] = copyFrom(mmb, offsetInMmb, len);
                    stats[i] = Stat.firstReading(temp);
                    return;
                }
                else {
                    if (len == curBytes.length && equals(curBytes, mmb, offsetInMmb, len)) {
                        stats[i].mergeReading(temp);
                        return;
                    }
                }
            }
        }
    }

    /** Represents aggregate stats. */
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
            // Can this be improved furhter?
            // Assuming random values for curTemp,
            // min (&max) gets updated roughly log(N)/N fraction of the time (a small number)
            // In the worst case, there will be at-most one branch misprediction.
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

    static class Tracing {

        private static final long[] cleanerTimes = new long[1 << 6 << 1];
        private static final long[] threadTimes = new long[1 << 6 << 1];
        private static long startTime;

        static void analyzeWorkThreads(String id, int nThreads) {
            printTimingsAnalysis(id + " Stats", nThreads, timingsArray(id));
        }

        static void recordAppStart() {
            startTime = System.nanoTime();
        }

        static void recordEvent(String event) {
            printEvent(event, System.nanoTime());
        }

        static void recordWorkEnd(String id, int threadId) {
            timingsArray(id)[2 * threadId + 1] = System.nanoTime();
        }

        static void recordWorkStart(String id, int threadId) {
            timingsArray(id)[2 * threadId] = System.nanoTime();
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////

        private static void errPrint(String message) {
            System.err.println(message);
        }

    private static void printEvent(String message, long nanoTime) {
      errPrint(STR."\{message} = \{(nanoTime - startTime) / 1_000_000}ms");
    }

    private static void printTimingsAnalysis(String header, int nThreads, long[] timestamps) {
      long minDuration = Long.MAX_VALUE, maxDuration = Long.MIN_VALUE;
      long minBegin = Long.MAX_VALUE, maxCompletion = Long.MIN_VALUE;
      long maxBegin = Long.MIN_VALUE, minCompletion = Long.MAX_VALUE;

      long[] durationsMs = new long[nThreads];
      long[] completionsMs = new long[nThreads];
      long[] beginMs = new long[nThreads];
      for (int i = 0; i < nThreads; i++) {
        long durationNs = timestamps[2 * i + 1] - timestamps[2 * i];
        durationsMs[i] = durationNs / 1_000_000;
        completionsMs[i] = (timestamps[2 * i + 1] - startTime) / 1_000_000;
        beginMs[i] = (timestamps[2 * i] - startTime) / 1_000_000;

        minDuration = Math.min(minDuration, durationNs);
        maxDuration = Math.max(maxDuration, durationNs);

        minBegin = Math.min(minBegin, timestamps[2 * i]);
        maxBegin = Math.max(maxBegin, timestamps[2 * i]);

        maxCompletion = Math.max(maxCompletion, timestamps[2 * i + 1]);
        minCompletion = Math.min(minCompletion, timestamps[2 * i + 1]);
      }
      errPrint(
          STR."""
        -------------------------------------------------------------------------------------------
                                       \{header}
        -------------------------------------------------------------------------------------------
        Max duration                              = \{maxDuration / 1_000_000} ms
        Min duration                              = \{minDuration / 1_000_000} ms
        Timespan[max(end)-min(start)]             = \{(maxCompletion - minBegin) / 1_000_000} ms
        Completion Timespan[max(end)-min(end)]    = \{(maxCompletion - minCompletion) / 1_000_000} ms
        Begin Timespan[max(begin)-min(begin)]     = \{(maxBegin - minBegin) / 1_000_000} ms
        Durations                                 = \{toString(durationsMs)} in ms
        Begin Timestamps                          = \{toString(beginMs)} in ms
        Completion Timestamps                     = \{toString(completionsMs)} in ms
        """);
    }

        private static long[] timingsArray(String id) {
            return switch (id) {
                case "cleaner" -> cleanerTimes;
                case "shard" -> threadTimes;
                default -> throw new RuntimeException("");
            };
        }

        private static String toString(long[] array) {
            return Arrays.stream(array)
                    .mapToObj(x -> String.format("%6d", x))
                    .collect(Collectors.joining(", ", "[ ", " ]"));
        }
    }
}
