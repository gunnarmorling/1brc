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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import sun.misc.Unsafe;

/**
 * Unlike its sister submission {@code CalculateAverage_vemana}, this submission employs non
 * idiomatic methods such as SWAR and Unsafe.
 *
 * <p>For details on how this solution works, check the documentation on the sister submission.
 */
public class CalculateAverage_vemanaNonIdiomatic {

  public static void main(String[] args) throws Exception {
    String className = MethodHandles.lookup().lookupClass().getSimpleName();
    System.err.println(
        STR."""
        ------------------------------------------------
        Running \{className}
        -------------------------------------------------
        """);
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
    double commonChunkFraction = 0.03;

    // Use commonChunkSizeBits for the small-chunk size
    int commonChunkSizeBits = 18;

    // Size of the hashtable (attempt to fit in L2 of 512KB of eval machine)
    int hashtableSizeBits = className.toLowerCase().contains("nonidiomatic") ? 13 : 16;

    // Reserve some number of lines at the end to give us freedom in reading LONGs past ranges
    int minReservedBytesAtFileTail = 9;

    // Number of threads
    int nThreads = -1;

    String inputFile = "measurements.txt";

    // Parallelize unmap. Thread #n (n=1,2,..N) unmaps its bytebuffer when
    // munmapFraction * n work remains.
    double munmapFraction = 0.03;

    boolean fakeAdvance = false;

    for (String arg : args) {
      String key = arg.substring(0, arg.indexOf('=')).trim();
      String value = arg.substring(key.length() + 1).trim();
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
        case "inputFile":
          inputFile = value;
          break;
        case "munmapFraction":
          munmapFraction = Double.parseDouble(value);
          break;
        case "fakeAdvance":
          fakeAdvance = Boolean.parseBoolean(value);
          break;
        case "nThreads":
          nThreads = Integer.parseInt(value);
          break;
        default:
          throw new IllegalArgumentException("Unknown argument: " + arg);
      }
    }

    System.out.println(
        new Runner(
                Path.of(inputFile),
                nThreads,
                chunkSizeBits,
                commonChunkFraction,
                commonChunkSizeBits,
                hashtableSizeBits,
                minReservedBytesAtFileTail,
                munmapFraction,
                fakeAdvance)
            .getSummaryStatistics());

    Tracing.recordEvent("Final result printed");
  }

  public record AggregateResult(Map<String, Stat> tempStats) {

    @Override
    public String toString() {
      return this.tempStats().entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(entry -> "%s=%s".formatted(entry.getKey(), entry.getValue()))
          .collect(Collectors.joining(", ", "{", "}"));
    }
  }

    // Mutable to avoid allocation
    public static class ByteRange {

        private static final int BUF_SIZE = 1 << 28;

        private final long fileSize;
        private final long maxEndPos; // Treat as if the file ends here
        private final RandomAccessFile raf;
        private final int shardIdx;
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

        ///////////// The PUBLIC API

        public MappedByteBuffer byteBuffer;
        public long endAddress; // the virtual memory address corresponding to 'endInBuf'
        public int endInBuf; // where the chunk ends inside the buffer
        public long startAddress; // the virtual memory address corresponding to 'startInBuf'
        public int startInBuf; // where the chunk starts inside the buffer

        ///////////// Private State

        long bufferBaseAddr; // buffer's base virtual memory address
        long extentEnd; // byteBuffer's ending coordinate
        long extentStart; // byteBuffer's begin coordinate

        // Uninitialized; for mutability
        public ByteRange(RandomAccessFile raf, long maxEndPos, int shardIdx) {
            this.raf = raf;
            this.maxEndPos = maxEndPos;
            this.shardIdx = shardIdx;
            try {
                this.fileSize = raf.length();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            bufferCleanSlate();
        }

        public void close(String closerId) {
            Tracing.recordWorkStart(closerId, shardIdx);
            bufferCleanSlate();
            for (MappedByteBuffer buf : unclosedBuffers) {
                close(buf);
            }
            unclosedBuffers.clear();
            Tracing.recordWorkEnd(closerId, shardIdx);
        }

        public void setRange(long rangeStart, long rangeEnd) {
            if (rangeEnd + 1024 > extentEnd || rangeStart < extentStart) {
                setByteBufferExtent(rangeStart, Math.min(rangeStart + BUF_SIZE, fileSize));
            }

            if (rangeStart > 0) {
                rangeStart = 1 + nextNewLine(rangeStart);
            }
            else {
                rangeStart = 0;
            }

            if (rangeEnd < maxEndPos) {
                // rangeEnd = 1 + nextNewLine(rangeEnd); // not needed
                rangeEnd = 1 + rangeEnd;
            }
            else {
                rangeEnd = maxEndPos;
            }

            startInBuf = (int) (rangeStart - extentStart);
            endInBuf = (int) (rangeEnd - extentStart);
            startAddress = bufferBaseAddr + startInBuf;
            endAddress = bufferBaseAddr + endInBuf;
        }

    @Override
    public String toString() {
      return STR."""
        ByteRange {
          shard                 = \{shardIdx}
          extentStart           = \{extentStart}
          extentEnd             = \{extentEnd}
          startInBuf            = \{startInBuf}
          endInBuf              = \{endInBuf}
          startAddress          = \{startAddress}
          endAddress            = \{endAddress}
        }
        """;
    }

        private void bufferCleanSlate() {
            if (byteBuffer != null) {
                unclosedBuffers.add(byteBuffer);
                byteBuffer = null;
            }
            extentEnd = extentStart = bufferBaseAddr = startAddress = endAddress = -1;
        }

        private void close(MappedByteBuffer buffer) {
            Method cleanerMethod = Reflection.findMethodNamed(buffer, "cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = Reflection.invoke(buffer, cleanerMethod);

            Method cleanMethod = Reflection.findMethodNamed(cleaner, "clean");
            cleanMethod.setAccessible(true);
            Reflection.invoke(cleaner, cleanMethod);
        }

        private long getBaseAddr(MappedByteBuffer buffer) {
            Method addressMethod = Reflection.findMethodNamed(buffer, "address");
            addressMethod.setAccessible(true);
            return (long) Reflection.invoke(buffer, addressMethod);
        }

        private long nextNewLine(long pos) {
            int nextPos = (int) (pos - extentStart);
            while (byteBuffer.get(nextPos) != '\n') {
                nextPos++;
            }
            return nextPos + extentStart;
        }

        /**
         * Extent different from Range. Range is what needs to be processed. Extent is what the byte
         * buffer can read without failing.
         */
        private void setByteBufferExtent(long start, long end) {
            bufferCleanSlate();
            try {
                byteBuffer = raf.getChannel().map(MapMode.READ_ONLY, start, end - start);
                byteBuffer.order(ByteOrder.nativeOrder());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            extentStart = start;
            extentEnd = end;
            bufferBaseAddr = getBaseAddr(byteBuffer);
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

    /*
     * ENTRY SHAPE
     * Ensure alignment boundaries. 4 bytes on 4 byte, 2 bytes on 2 byte etc.
     * 32 bytes per entry.
     * 96 KB L1 cache. 2048 entries should fully fit
     * -------------------
     * str: 14 bytes [Defined by constant STR_FIELD_LEN]
     * hash: 2 bytes
     * cityNameOffset: 3 bytes // Index in city names array if len > STR_FIELD_LEN bytes
     * len: 1 byte // Length of string, in bytes
     * sum: 4 bytes
     * count: 4 bytes
     * max: 2 bytes
     * min: 2 bytes
     */
    static class EntryData {

        public static final int ENTRY_SIZE_BITS = 5;

        /////////// OFFSETS ///////////////
        private static final int OFFSET_STR = 0;
        private static final int STR_FIELD_LEN = 14;
        private static final int OFFSET_HASH = OFFSET_STR + STR_FIELD_LEN;
        private static final int OFFSET_CITY_NAME_EXTRA = OFFSET_HASH + 2;
        private static final int OFFSET_LEN = OFFSET_CITY_NAME_EXTRA + 3;
        private static final int OFFSET_SUM = OFFSET_LEN + 1;
        private static final int OFFSET_COUNT = OFFSET_SUM + 4;
        private static final int OFFSET_MAX = OFFSET_COUNT + 4;
        private static final int OFFSET_MIN = OFFSET_MAX + 2;

        public static int strFieldLen() {
            return STR_FIELD_LEN;
        }

        private final EntryMeta entryMeta;

        private long baseAddress;

        public EntryData(EntryMeta entryMeta) {
            this.entryMeta = entryMeta;
        }

        public long baseAddress() {
            return baseAddress;
        }

        public String cityNameString() {
            int len = len();
            byte[] zeBytes = new byte[len];

            for (int i = 0; i < Math.min(len, strFieldLen()); i++) {
                zeBytes[i] = Unsafely.readByte(baseAddress + i);
            }

            if (len > strFieldLen()) {
                int rem = len - strFieldLen();
                long ptr = entryMeta.cityNamesAddress(cityNamesOffset());
                for (int i = 0; i < rem; i++) {
                    zeBytes[strFieldLen() + i] = Unsafely.readByte(ptr + i);
                }
            }

            return new String(zeBytes);
        }

        public int cityNamesOffset() {
            return Unsafely.readInt(baseAddress + OFFSET_CITY_NAME_EXTRA) & 0xFFFFFF;
        }

        public int count() {
            return Unsafely.readInt(baseAddress + OFFSET_COUNT);
        }

        public short hash16() {
            return Unsafely.readShort(baseAddress + OFFSET_HASH);
        }

        public int index() {
            return (int) ((baseAddress() - entryMeta.baseAddress(0)) >> ENTRY_SIZE_BITS);
        }

        public void init(long srcAddr, int len, short hash16, short temperature) {
            // Copy the string
            Unsafely.copyMemory(srcAddr, strAddress(), Math.min(len, EntryData.strFieldLen()));
            if (len > EntryData.strFieldLen()) {
                int remaining = len - EntryData.strFieldLen();
                int cityNamesOffset = entryMeta.getAndIncrementCityNames(remaining);
                Unsafely.copyMemory(
                        srcAddr + EntryData.strFieldLen(),
                        entryMeta.cityNamesAddress(cityNamesOffset),
                        remaining);
                setCityNameOffset(cityNamesOffset, len);
            }
            else {
                setLen((byte) len);
            }

            // and then update the others
            setHash16(hash16);
            setSum(temperature);
            setCount(1);
            setMax(temperature);
            setMin(temperature);
        }

        public boolean isPresent() {
            return len() > 0;
        }

        public int len() {
            return Unsafely.readByte(baseAddress + OFFSET_LEN);
        }

        public short max() {
            return Unsafely.readShort(baseAddress + OFFSET_MAX);
        }

        public short min() {
            return Unsafely.readShort(baseAddress + OFFSET_MIN);
        }

        public void setBaseAddress(long baseAddress) {
            this.baseAddress = baseAddress;
        }

        public void setCityNameOffset(int cityNamesOffset, int len) {
            // The 24 here is 3 bytes for Cityname extra index + 1 byte for actual len
            // that writes 4 bytes in one shot. It is not an offset.
            Unsafely.setInt(baseAddress + OFFSET_CITY_NAME_EXTRA, cityNamesOffset | (len << 24));
        }

        public void setCount(int value) {
            Unsafely.setInt(baseAddress + OFFSET_COUNT, value);
        }

        public void setHash16(short value) {
            Unsafely.setShort(baseAddress + OFFSET_HASH, value);
        }

        public void setIndex(int index) {
            setBaseAddress(entryMeta.baseAddress(index));
        }

        public void setLen(byte value) {
            Unsafely.setByte(baseAddress + OFFSET_LEN, value);
        }

        public void setMax(short value) {
            Unsafely.setShort(baseAddress + OFFSET_MAX, value);
        }

        public void setMin(short value) {
            Unsafely.setShort(baseAddress + OFFSET_MIN, value);
        }

        public void setSum(int value) {
            Unsafely.setInt(baseAddress + OFFSET_SUM, value);
        }

        public Stat stat() {
            return new Stat(min(), max(), sum(), count());
        }

        public long strAddress() {
            return baseAddress + OFFSET_STR;
        }

        public int sum() {
            return Unsafely.readInt(baseAddress + OFFSET_SUM);
        }

    public String toString() {
      return STR."""
        min = \{min()}
        max = \{max()}
        count = \{count()}
        sum = \{sum()}
        """;
    }

        public void update(short temperature) {
            setMin((short) Math.min(min(), temperature));
            setMax((short) Math.max(max(), temperature));
            setCount(count() + 1);
            setSum(sum() + temperature);
        }

        public boolean updateOnMatch(
                                     EntryMeta entryMeta, long srcAddr, int len, short hash16, short temperature) {

            // Quick paths
            if (len() != len) {
                return false;
            }
            if (hash16() != hash16) {
                return false;
            }

            // Actual string comparison
            if (len <= STR_FIELD_LEN) {
                if (!Unsafely.matches(srcAddr, strAddress(), len)) {
                    return false;
                }
            }
            else {
                if (!Unsafely.matches(srcAddr, strAddress(), STR_FIELD_LEN)) {
                    return false;
                }
                if (!Unsafely.matches(
                        srcAddr + STR_FIELD_LEN,
                        entryMeta.cityNamesAddress(cityNamesOffset()),
                        len - STR_FIELD_LEN)) {
                    return false;
                }
            }
            update(temperature);
            return true;
        }
    }

    /** Metadata for the collection of entries */
    static class EntryMeta {

        static int toIntFromUnsignedShort(short x) {
            int ret = x;
            if (ret < 0) {
                ret += (1 << 16);
            }
            return ret;
        }

        private final long baseAddress;
        private final long cityNamesBaseAddress; // For city names that overflow Entry.STR_FIELD_LEN
        private final int hashMask;
        private final int n_entries;
        private final int n_entriesBits;
        private long cityNamesEndAddress; // [cityNamesBaseAddress, cityNamesEndAddress)

        EntryMeta(int n_entriesBits, EntryMeta oldEntryMeta) {
            this.n_entries = 1 << n_entriesBits;
            this.hashMask = (1 << n_entriesBits) - 1;
            this.n_entriesBits = n_entriesBits;
            this.baseAddress = Unsafely.allocateZeroedCacheLineAligned(this.n_entries << EntryData.ENTRY_SIZE_BITS);
            if (oldEntryMeta == null) {
                this.cityNamesBaseAddress = Unsafely.allocateZeroedCacheLineAligned(1 << 17);
                this.cityNamesEndAddress = cityNamesBaseAddress;
            }
            else {
                this.cityNamesBaseAddress = oldEntryMeta.cityNamesBaseAddress;
                this.cityNamesEndAddress = oldEntryMeta.cityNamesEndAddress;
            }
        }

        public long cityNamesAddress(int extraLenOffset) {
            return cityNamesBaseAddress + extraLenOffset;
        }

        public int indexFromHash16(short hash16) {
            return indexFromHash32(toIntFromUnsignedShort(hash16));
        }

        public int nEntriesBits() {
            return n_entriesBits;
        }

        // Base Address of nth entry
        long baseAddress(int n) {
            return baseAddress + ((long) n << EntryData.ENTRY_SIZE_BITS);
        }

        // Size of each entry
        int entrySizeInBytes() {
            return 1 << EntryData.ENTRY_SIZE_BITS;
        }

        int getAndIncrementCityNames(int len) {
            long ret = cityNamesEndAddress;
            cityNamesEndAddress += ((len + 7) >> 3) << 3; // use aligned 8 bytes
            return (int) (ret - cityNamesBaseAddress);
        }

        // Index of an entry with given hash32
        int indexFromHash32(int hash32) {
            return hash32 & hashMask;
        }

        // Number of entries
        int nEntries() {
            return n_entries;
        }

        int nextIndex(int index) {
            return (index + 1) & hashMask;
        }
    }

    static class Hashtable {

        // State
        int n_filledEntries;
        // A single Entry to avoid local allocation
        private EntryData entry;
        private EntryMeta entryMeta;
        // Invariants
        // hash16 = (short) hash32
        // index = hash16 & hashMask
        private int hashHits = 0, hashMisses = 0;

        Hashtable(int slotsBits) {
            entryMeta = new EntryMeta(slotsBits, null);
            this.entry = new EntryData(entryMeta);
        }

        public void addDataPoint(long srcAddr, int len, int hash32, short temperature) {
            // hashHits++;
            for (int index = entryMeta.indexFromHash32(hash32);; index = entryMeta.nextIndex(index)) {
                entry.setIndex(index);

                if (!entry.isPresent()) {
                    entry.init(srcAddr, len, (short) hash32, temperature);
                    onNewEntry();
                    return;
                }

                if (entry.updateOnMatch(entryMeta, srcAddr, len, (short) hash32, temperature)) {
                    return;
                }
                // hashMisses++;
            }
        }

    public AggregateResult result() {
      Map<String, Stat> map = new LinkedHashMap<>(5_000);
      for (int i = 0; i < entryMeta.nEntries(); i++) {
        entry.setIndex(i);
        if (entry.isPresent()) {
          map.put(entry.cityNameString(), entry.stat());
        }
      }
      System.err.println(
          STR."""
        HashHits = \{hashHits}
        HashMisses = \{hashMisses} (\{hashMisses * 100.0 / hashHits})
        """);
      return new AggregateResult(map);
    }

        private EntryData getNewEntry(EntryData oldEntry, EntryMeta newEntryMeta) {
            EntryData newEntry = new EntryData(newEntryMeta);
            for (int index = newEntryMeta.indexFromHash16(oldEntry.hash16());; index = newEntryMeta.nextIndex(index)) {
                newEntry.setIndex(index);
                if (!newEntry.isPresent()) {
                    return newEntry;
                }
            }
        }

        private void onNewEntry() {
            if (++n_filledEntries == 450) {
                reHash(16);
            }
        }

        private void reHash(int new_N_EntriesBits) {
            EntryMeta oldEntryMeta = this.entryMeta;
            EntryData oldEntry = new EntryData(oldEntryMeta);
            Checks.checkArg(new_N_EntriesBits <= 16);
            Checks.checkArg(new_N_EntriesBits > oldEntryMeta.nEntriesBits());
            EntryMeta newEntryMeta = new EntryMeta(new_N_EntriesBits, oldEntryMeta);
            for (int i = 0; i < oldEntryMeta.nEntries(); i++) {
                oldEntry.setIndex(i);
                if (oldEntry.isPresent()) {
                    Unsafely.copyMemory(
                            oldEntry.baseAddress(),
                            getNewEntry(oldEntry, newEntryMeta).baseAddress(),
                            oldEntryMeta.entrySizeInBytes());
                }
            }
            this.entryMeta = newEntryMeta;
            this.entry = new EntryData(this.entryMeta);
        }
    }

    public interface LazyShardQueue {

        void close(String closerId, int shardIdx);

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
        private final boolean fakeAdvance;
        private final int hashtableSizeBits;
        private final Path inputFile;
        private final int minReservedBytesAtFileTail;
        private final double munmapFraction;
        private final int nThreads;
        private final int shardSizeBits;

        public Runner(
                      Path inputFile,
                      int nThreads,
                      int chunkSizeBits,
                      double commonChunkFraction,
                      int commonChunkSizeBits,
                      int hashtableSizeBits,
                      int minReservedBytesAtFileTail,
                      double munmapFraction,
                      boolean fakeAdvance) {
            this.inputFile = inputFile;
            this.nThreads = nThreads;
            this.shardSizeBits = chunkSizeBits;
            this.commonChunkFraction = commonChunkFraction;
            this.commonChunkSizeBits = commonChunkSizeBits;
            this.hashtableSizeBits = hashtableSizeBits;
            this.minReservedBytesAtFileTail = minReservedBytesAtFileTail;
            this.munmapFraction = munmapFraction;
            this.fakeAdvance = fakeAdvance;
        }

        AggregateResult getSummaryStatistics() throws Exception {
            int nThreads = this.nThreads < 0 ? Runtime.getRuntime().availableProcessors() : this.nThreads;

            LazyShardQueue shardQueue = new SerialLazyShardQueue(
                    1L << shardSizeBits,
                    inputFile,
                    nThreads,
                    commonChunkFraction,
                    commonChunkSizeBits,
                    minReservedBytesAtFileTail,
                    munmapFraction,
                    fakeAdvance);

            ExecutorService executorService = Executors.newFixedThreadPool(
                    nThreads,
                    runnable -> {
                        Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        return thread;
                    });

            List<Future<AggregateResult>> results = new ArrayList<>();
            for (int i = 0; i < nThreads; i++) {
                final int shardIdx = i;
                final Callable<AggregateResult> callable = () -> {
                    Tracing.recordWorkStart("Shard", shardIdx);
                    AggregateResult result = new ShardProcessor(shardQueue, hashtableSizeBits, shardIdx).processShard();
                    Tracing.recordWorkEnd("Shard", shardIdx);
                    return result;
                };
                results.add(executorService.submit(callable));
            }
            Tracing.recordEvent("Basic push time");

            // This particular sequence of Futures is so that both merge and munmap() can work as shards
            // finish their computation without blocking on the entire set of shards to complete. In
            // particular, munmap() doesn't need to wait on merge.
            // First, submit a task to merge the results and then submit a task to cleanup bytebuffers
            // from completed shards.
            Future<AggregateResult> resultFutures = executorService.submit(() -> merge(results));
            // Note that munmap() is serial and not parallel and hence we use just one thread.
            executorService.submit(() -> closeByteBuffers(results, shardQueue));

            AggregateResult result = resultFutures.get();
            Tracing.recordEvent("Merge results received");

            Tracing.recordEvent("About to shutdown executor and wait");
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            Tracing.recordEvent("Executor terminated");

            Tracing.analyzeWorkThreads(nThreads);
            return result;
        }

        private void closeByteBuffers(
                                      List<Future<AggregateResult>> results, LazyShardQueue shardQueue) {
            int n = results.size();
            boolean[] isDone = new boolean[n];
            int remaining = results.size();
            while (remaining > 0) {
                for (int i = 0; i < n; i++) {
                    if (!isDone[i] && results.get(i).isDone()) {
                        remaining--;
                        isDone[i] = true;
                        shardQueue.close("Ending Cleaner", i);
                    }
                }
            }
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
        private final boolean fakeAdvance;
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
                                    int fileTailReservedBytes,
                                    double munmapFraction,
                                    boolean fakeAdvance)
                throws IOException {
            this.fakeAdvance = fakeAdvance;
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
                perThreadData[pos + 3] = (long) (currentChunks * (munmapFraction * (shards - i)));
                perThreadData[pos + 4] = 1; // true iff munmap() hasn't been triggered yet
                currentStart += currentChunks * chunkSize;
                remainingChunks -= currentChunks;
            }
            this.chunkSize = chunkSize;

            this.byteRanges = new ByteRange[shards << 4];
            for (int i = 0; i < shards; i++) {
                byteRanges[i << 4] = new ByteRange(raf, effectiveFileSize, i);
            }

            this.seqLock = new SeqLock();
        }

        @Override
        public void close(String closerId, int shardIdx) {
            byteRanges[shardIdx << 4].close(closerId);
        }

        @Override
        public Optional<ByteRange> fileTailEndWork(int idx) {
            if (idx == 0 && effectiveFileSize < fileSize) {
                ByteRange chunk = new ByteRange(raf, fileSize, 0);
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
            final long rangeStart;
            final long rangeEnd;

            if (perThreadData[pos + 2] >= 1) {
                rangeStart = perThreadData[pos];
                rangeEnd = rangeStart + chunkSize;
                // Don't do this in the if-check; it causes negative values that trigger intermediate
                // cleanup
                perThreadData[pos + 2]--;
                if (!fakeAdvance) {
                    perThreadData[pos] = rangeEnd;
                }
            }
            else {
                rangeStart = commonPool.getAndAdd(commonChunkSize);
                // If that's exhausted too, nothing remains!
                if (rangeStart >= effectiveFileSize) {
                    return null;
                }
                rangeEnd = rangeStart + commonChunkSize;
            }

            if (perThreadData[pos + 2] < perThreadData[pos + 3] && perThreadData[pos + 4] > 0) {
                if (attemptIntermediateClose(shardIdx)) {
                    perThreadData[pos + 4]--;
                }
            }

            ByteRange chunk = byteRanges[pos];
            chunk.setRange(rangeStart, rangeEnd);
            return chunk;
        }

        private boolean attemptIntermediateClose(int shardIdx) {
            if (seqLock.acquire()) {
                close("Intermediate Cleaner", shardIdx);
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
        private final FastShardProcessorState state;

        public ShardProcessor(LazyShardQueue shardQueue, int hashtableSizeBits, int shardIdx) {
            this.shardQueue = shardQueue;
            this.shardIdx = shardIdx;
            this.state = new FastShardProcessorState(hashtableSizeBits);
        }

        public AggregateResult processShard() {
            return processShardReal();
        }

        private void processRange(ByteRange range) {
            long nextPos = range.startAddress;
            while (nextPos < range.endAddress) {
                nextPos = state.processLine(nextPos);
            }
        }

        private void processRangeSlow(ByteRange range) {
            long nextPos = range.startAddress;
            while (nextPos < range.endAddress) {
                nextPos = state.processLineSlow(nextPos);
            }
        }

        private AggregateResult processShardReal() {
            // First process the file tail work to give ourselves freedom to go past ranges in parsing
            shardQueue.fileTailEndWork(shardIdx).ifPresent(this::processRangeSlow);

            ByteRange range;
            while ((range = shardQueue.take(shardIdx)) != null) {
                processRange(range);
            }
            return result();
        }

        private AggregateResult result() {
            return state.result();
        }
    }

    public static class FastShardProcessorState {

        private static final long LEADING_ONE_BIT_MASK = 0x8080808080808080L;
        private static final long ONE_MASK = 0x0101010101010101L;
        private static final long SEMICOLON_MASK = 0x3b3b3b3b3b3b3b3bL;
        private final Hashtable hashtable;
        private final Map<String, Stat> slowProcessStats = new HashMap<>();

        public FastShardProcessorState(int slotsBits) {
            this.hashtable = new Hashtable(slotsBits);
            Checks.checkArg(slotsBits <= 16); // since this.hashes is 'short'
        }

        public long processLine(long nextPos) {
            final long origPos = nextPos;

            // Trying to extract this into a function made it slower.. so, leaving it at inlining.
            // It's a pity since the extracted version was more elegant to read
            long firstLong;
            int hash = 0;
            // Don't run Long.numberOfTrailingZeros in hasSemiColon; it is not needed to establish
            // whether there's a semicolon; only needed for pin-pointing length of the tail.
            long s = hasSemicolon(firstLong = Unsafely.readLong(nextPos));
            final int trailingZeroes;
            if (s == 0) {
                hash = doHash(firstLong);
                do {
                    nextPos += 8;
                    s = hasSemicolon(Unsafely.readLong(nextPos));
                } while (s == 0);
                trailingZeroes = Long.numberOfTrailingZeros(s) + 1; // 8, 16, 24, .. # past ;
            }
            else {
                trailingZeroes = Long.numberOfTrailingZeros(s) + 1; // 8, 16, 24, .. # past ;
                hash = doHash(firstLong & maskOf(trailingZeroes - 8));
            }
            // Sometimes we do mix a tail of length 0..
            nextPos += (trailingZeroes >> 3);

            final int temp = readTemperature(nextPos);
            hashtable.addDataPoint(origPos, (int) (nextPos - 1 - origPos), hash, (short) (temp >> 3));
            return nextPos + (temp & 7);
        }

        /**
         * A slow version which is used only for the tail part of the file. Maintaining hashcode sync
         * between this and the fast version is a pain for experimentation. So, we'll simply use a naive
         * approach.
         */
        public long processLineSlow(long nextPos) {
            byte nextByte;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((nextByte = Unsafely.readByte(nextPos++)) != ';') {
                baos.write(nextByte);
            }

            int temperature = 0;
            boolean negative = Unsafely.readByte(nextPos) == '-';
            while ((nextByte = Unsafely.readByte(nextPos++)) != '\n') {
                if (nextByte != '-' && nextByte != '.') {
                    temperature = temperature * 10 + (nextByte - '0');
                }
            }
            if (negative) {
                temperature = -temperature;
            }

            updateStat(slowProcessStats, baos.toString(), Stat.firstReading(temperature));
            return nextPos;
        }

        public AggregateResult result() {
            AggregateResult result = hashtable.result();
            if (!slowProcessStats.isEmpty()) {
                // bah.. just mutate the arg of the record...
                for (Entry<String, Stat> entry : slowProcessStats.entrySet()) {
                    updateStat(result.tempStats(), entry.getKey(), entry.getValue());
                }
            }
            return result;
        }

        int readTemperature(long nextPos) {
            // This Dependency chain
            // read -> shift -> xor -> compare -> 2 in parallel [ shift -> read ] -> add -> shift
            // Chain latency: 2 reads + 2 add + 4 logical [assuming compare = add]
            // vs
            // Prior Dependency chain (slightly optimized by hand)
            // read -> compare to '-' -> read -> compare to '.' -> 3 in parallel [read -> imul] -> add
            // Chain latency: 3 reads + 3 add + 1 mul [assuming compare = add]
            long data = Unsafely.readLong(nextPos);
            long d = data ^ (data >> 4);
            if ((data & 0xFF) == '-') {
                return TemperatureLookup.firstNeg(d >> 8) + TemperatureLookup.secondNeg(d >> 24);
            }
            else {
                return TemperatureLookup.firstPos(d >> 0) + TemperatureLookup.secondPos(d >> 16);
            }
        }

        private int doHash(long value) {
            long hash = 31L * (int) value + (int) (value >> 32);
            return (int) (hash ^ (hash >> 17) ^ (hash >> 28));
        }

        private long hasSemicolon(long x) {
            long a = (x ^ SEMICOLON_MASK);
            return (a - ONE_MASK) & (~a) & LEADING_ONE_BIT_MASK;
        }

        private long maskOf(int bits) {
            return ~(-1L << bits);
        }

        private void updateStat(Map<String, Stat> map, String key, Stat curValue) {
            map.compute(key, (_, value) -> value == null ? curValue : Stat.merge(value, curValue));
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

        long count, sum;
        int min, max;

        public Stat(int min, int max, long sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        // Caution: Mutates
        public Stat mergeReading(int curTemp) {
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
            return this;
        }

        @Override
        public String toString() {
            return "%.1f/%.1f/%.1f".formatted(min / 10.0, sum / 10.0 / count, max / 10.0);
        }
    }

    /**
     * Lookup table for temperature parsing.
     *
     * <pre>
     * 0       0011-0000
     * 9       0011-1001
     * .       0010-1110
     * \n      0000-1010
     *
     * Notice that there's no overlap in the last 4 bits. This means, if we are given two successive
     * bytes X, Y all of which belong to the above characters, we can REVERSIBLY hash it to
     * a single byte by doing 8-bit-hash = (last 4 bits of X) concat (last 4 bits of Y).
     *
     * Such a hash requires a few more operations than ideal. A more practical hash is:
     * (X>>4) ^ Y ^ (Y >> 4). This means if you read 4 bytes after the '-',
     * L = X Y Z W, where each of X Y Z W is a byte, then,
     * L ^ (L >> 4) = D hash(X, Y) hash(Y, Z) hash(Z, W) where D = don't care. In other words, we
     * can SWAR the hash.
     *
     * This has potential for minor conflicts; for e.g. (3, NewLine) collides with (0, 9). But, we
     * don't have any collisions between two digits. That is (x, y) will never collide with (a, b)
     * where x, y, a, b are digits (proof left as an exercise, lol). Along with a couple of other
     * such no-conflict observations, it suffices for our purposes.
     *
     * If we just precompute some values like
     * - BigTable[hash(X,Y)] = 100*X + 10*Y
     * - SmallTable[hash(Z,W)] = 10*Z + W
     *
     * where potentially X, Y, Z, W can be '.' or '\n', (and the arithmetic adjusted), we can lookup
     * the temperature pieces from BigTable and SmallTable and add them together.
     * </pre>
     *
     * <p>This class is an implementation of the above idea. The lookup tables being 256 ints long
     * will always be resident in L1 cache. What remains then is to also add the information on how
     * much input is to be consumed; i.e. count the - and newlines too. That can be piggy backed on
     * top of the values.
     *
     * <p>FWIW, this lookup appears to have reduced the temperature reading overhead substantially on
     * a Ryzen 7950X machine. But, it wasn't done systematically; so, YMMV.
     */
    public static class TemperatureLookup {

        // Second is the smaller (units place)
        // First is the larger (100 & 10)

        // _NEG tables simply negate the value so that call-site can always simply add the values from
        // the first and second units. Call-sites adding-up First and Second units adds up the
        // amount of input to consume.

        // Here, 2 is the amount of bytes consumed. This informs how much the reading pointer
        // should move.
        // For pattern XY value = ((-100*X -10*Y) << 3) + 2 [2 = 1 for X, 1 for Y]
        // For pattern Y. value = ((-10*Y) << 3) + 2 [2 = 1 for Y, 1 for .]
        private static final int[] FIRST_NEG = make(true, true);

        // For pattern XY value = ((100*X + 10*Y) << 3) + 2
        // For pattern Y. value = ((10*Y) << 3) + 2
        private static final int[] FIRST_POS = make(true, false);

        // We count newline and any initial '-' as part of SECOND
        // For pattern .Z value = (-Z << 3) + 2 + 2 [1 each for . and Z, 1 for newline, 1 for minus]
        // For pattern Zn value = (-Z << 3) + 1 + 2 [1 for Z, 1 for newline, 1 for minus]
        private static final int[] SECOND_NEG = make(false, true);

        // For pattern .Z value = (Z << 3) + 2 + 1 [1 each for . and Z, 1 for newline]
        // For pattern Zn value = (Z << 3) + 1 + 1 [1 for Z, 1 for newline]
        private static final int[] SECOND_POS = make(false, false);

        public static int firstNeg(long b) {
            return FIRST_NEG[(int) (b & 255)];
        }

        public static int firstPos(long b) {
            return FIRST_POS[(int) (b & 255)];
        }

        public static int secondNeg(long b) {
            return SECOND_NEG[(int) (b & 255)];
        }

        public static int secondPos(long b) {
            return SECOND_POS[(int) (b & 255)];
        }

        private static byte[] allDigits() {
            byte[] out = new byte[10];
            for (byte a = '0'; a <= '9'; a++) {
                out[a - '0'] = a;
            }
            return out;
        }

        private static int hash(byte msb, byte lsb) {
            // If K = [D msb lsb], then (K ^ (K>>4)) & 255 == hash(msb, lsb). D = don't care
            return (msb << 4) ^ lsb ^ (lsb >> 4);
        }

        private static int[] make(boolean isFirst, boolean isNegative) {
            int[] ret = new int[256];
            boolean[] done = new boolean[256];

            // Conventions: X = 100s place, Y = 10s place, Z = 1s place, n = new line

            // All the cases to handle
            // X Y . Z
            // Y . Z n

            // In little-endian order it becomes (byte-wise), shown in place value notation
            // Z . Y X
            // n Z . Y
            // First = YX or .Y
            // Second = Z. or nZ

            // Pattern 'YX'
            for (byte x : allDigits()) {
                for (byte y : allDigits()) {
                    int index = hash(y, x);
                    // Shouldn't occur in Second
                    int value = isFirst ? (y - '0') * 10 + (x - '0') * 100 : 12345;
                    int delta = isFirst ? 2 : 12345;
                    update(index, isNegative ? -value : value, delta, ret, done);
                }
            }

            // Pattern 'Z.'
            for (byte z : allDigits()) {
                int index = hash(z, (byte) '.');
                // shouldn't occur in First
                int value = isFirst ? 12345 : (z - '0');
                int delta = isFirst ? 12345 : 2;
                update(index, isNegative ? -value : value, delta, ret, done);
            }

            // Pattern '.Y'
            for (byte y : allDigits()) {
                int index = hash((byte) '.', y);
                // Shouldn't occur in Second
                int value = isFirst ? 10 * (y - '0') : 12345;
                int delta = isFirst ? 2 : 12345;
                update(index, isNegative ? -value : value, delta, ret, done);
            }

            // Pattern 'nZ'
            for (byte z : allDigits()) {
                int index = hash((byte) '\n', z);
                // shouldn't occur in First
                int value = isFirst ? 12345 : (z - '0');
                int delta = isFirst ? 12345 : 1;
                update(index, isNegative ? -value : value, delta, ret, done);
            }

            if (!isFirst) {
                // Adjust the deltas to reflect how much input needs to be consumed
                // need to consume the newline and any - sign in front
                for (int i = 0; i < ret.length; i++) {
                    ret[i] += (isNegative ? 1 : 0) /* for - sign */ + 1 /* for new line */;
                }
            }
            return ret;
        }

        private static void update(int index, int value, int delta, int[] ret, boolean[] done) {
            index &= 255;
            Checks.checkArg(!done[index]); // just a sanity check that our hashing is indeed reversible
            ret[index] = (value << 3) | delta;
            done[index] = true;
        }
    }

    static class Tracing {

        private static final Map<String, ThreadTimingsArray> knownWorkThreadEvents;
        private static long startTime;

        static {
            // Maintain the ordering to be chronological in execution
            // Map.of(..) screws up ordering
            knownWorkThreadEvents = new LinkedHashMap<>();
            for (String id : List.of("Shard", "Intermediate Cleaner", "Ending Cleaner", "Buffer Creation")) {
                knownWorkThreadEvents.put(id, new ThreadTimingsArray(id, 1 << 10));
            }
        }

        static void analyzeWorkThreads(int nThreads) {
            for (ThreadTimingsArray array : knownWorkThreadEvents.values()) {
                errPrint(array.analyze(nThreads));
            }
        }

        static void recordAppStart() {
            startTime = System.nanoTime();
            printEvent("Start time", startTime);
        }

        static void recordEvent(String event) {
            printEvent(event, System.nanoTime());
        }

        static void recordWorkEnd(String id, int threadId) {
            knownWorkThreadEvents.get(id).recordEnd(threadId);
        }

        static void recordWorkStart(String id, int threadId) {
            knownWorkThreadEvents.get(id).recordStart(threadId);
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////

        private static void errPrint(String message) {
            System.err.println(message);
        }

    private static void printEvent(String message, long nanoTime) {
      errPrint(STR."\{message} = \{(nanoTime - startTime) / 1_000_000}ms");
    }

        public static class ThreadTimingsArray {

            private static String toString(long[] array) {
                return Arrays.stream(array)
                        .map(x -> x < 0 ? -1 : x)
                        .mapToObj(x -> String.format("%6d", x))
                        .collect(Collectors.joining(", ", "[ ", " ]"));
            }

            private final String id;
            private final long[] timestamps;
            private boolean hasData = false;

            public ThreadTimingsArray(String id, int maxSize) {
                this.timestamps = new long[maxSize];
                this.id = id;
            }

      public String analyze(int nThreads) {
        if (!hasData) {
          return "%s has no thread timings data".formatted(id);
        }
        Checks.checkArg(nThreads <= timestamps.length);
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

          minBegin = Math.min(minBegin, timestamps[2 * i] - startTime);
          maxBegin = Math.max(maxBegin, timestamps[2 * i] - startTime);

          maxCompletion = Math.max(maxCompletion, timestamps[2 * i + 1] - startTime);
          minCompletion = Math.min(minCompletion, timestamps[2 * i + 1] - startTime);
        }
        return STR."""
        -------------------------------------------------------------------------------------------
                                       \{id} Stats
        -------------------------------------------------------------------------------------------
        Max duration                              = \{maxDuration / 1_000_000} ms
        Min duration                              = \{minDuration / 1_000_000} ms
        Timespan[max(end)-min(start)]             = \{(maxCompletion - minBegin) / 1_000_000} ms [\{maxCompletion / 1_000_000} - \{minBegin / 1_000_000} ]
        Completion Timespan[max(end)-min(end)]    = \{(maxCompletion - minCompletion) / 1_000_000} ms
        Begin Timespan[max(begin)-min(begin)]     = \{(maxBegin - minBegin) / 1_000_000} ms
        Average Duration                          = \{Arrays.stream(durationsMs)
                                                            .average()
                                                            .getAsDouble()} ms
        Durations                                 = \{toString(durationsMs)} ms
        Begin Timestamps                          = \{toString(beginMs)} ms
        Completion Timestamps                     = \{toString(completionsMs)} ms
        """;
      }

            public void recordEnd(int idx) {
                timestamps[2 * idx + 1] = System.nanoTime();
                hasData = true;
            }

            public void recordStart(int idx) {
                timestamps[2 * idx] = System.nanoTime();
                hasData = true;
            }
        }
    }

    static class Unsafely {

        private static final Unsafe unsafe = getUnsafe();

        public static long allocateZeroedCacheLineAligned(int size) {
            long address = unsafe.allocateMemory(size + 63);
            unsafe.setMemory(address, size + 63, (byte) 0);
            return (address + 63) & ~63;
        }

        public static void copyMemory(long srcAddress, long destAddress, long byteCount) {
            unsafe.copyMemory(srcAddress, destAddress, byteCount);
        }

        public static boolean matches(long srcAddr, long destAddress, int len) {
            if (len < 8) {
                return (readLong(srcAddr) & ~(-1L << (len << 3))) == (readLong(destAddress) & ~(-1L << (len << 3)));
            }
            if (readLong(srcAddr) != readLong(destAddress)) {
                return false;
            }
            len -= 8;

            if (len < 8) {
                return (readLong(srcAddr + 8) & ~(-1L << (len << 3))) == (readLong(destAddress + 8) & ~(-1L << (len << 3)));
            }
            if (readLong(srcAddr + 8) != readLong(destAddress + 8)) {
                return false;
            }
            len -= 8;
            srcAddr += 16;
            destAddress += 16;

            int idx = 0;
            for (; idx < (len & ~7); idx += 8) {
                if (Unsafely.readLong(srcAddr + idx) != Unsafely.readLong(destAddress + idx)) {
                    return false;
                }
            }

            if (idx < (len & ~3)) {
                if (Unsafely.readInt(srcAddr + idx) != Unsafely.readInt(destAddress + idx)) {
                    return false;
                }
                idx += 4;
            }

            if (idx < (len & ~1)) {
                if (Unsafely.readShort(srcAddr + idx) != Unsafely.readShort(destAddress + idx)) {
                    return false;
                }
                idx += 2;
            }

            return idx >= len || Unsafely.readByte(srcAddr + idx) == Unsafely.readByte(destAddress + idx);
        }

        public static byte readByte(long address) {
            return unsafe.getByte(address);
        }

        public static int readInt(long address) {
            return unsafe.getInt(address);
        }

        public static long readLong(long address) {
            return unsafe.getLong(address);
        }

        public static short readShort(long address) {
            return unsafe.getShort(address);
        }

        public static void setByte(long address, byte len) {
            unsafe.putByte(address, len);
        }

        public static void setInt(long address, int value) {
            unsafe.putInt(address, value);
        }

        public static void setShort(long address, short len) {
            unsafe.putShort(address, len);
        }

        private static Unsafe getUnsafe() {
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                return (Unsafe) unsafeField.get(null);
            }
            catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
