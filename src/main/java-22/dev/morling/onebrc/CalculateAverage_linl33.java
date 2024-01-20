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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class CalculateAverage_linl33 {
    private static final String FILE_PATH_PROPERTY = "dev.morling.onebrc.CalculateAverage_linl33.measurementsPath";
    private static final int WEATHER_STATION_LENGTH_MAX = 100;
    private static final long WEATHER_STATION_DISTINCT_MAX = 10_000L;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    private static final MemorySegment ALL = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

    private static final Thread.Builder THREAD_BUILDER = Thread
            .ofPlatform()
            .name("1brc-CalculateAverage-", 0)
            .inheritInheritableThreadLocals(false);

    private static final Unsafe UNSAFE;

    static {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new UnsupportedOperationException("Error: BE JVMs are not supported");
        }
        if ((BYTE_SPECIES.vectorByteSize() & (BYTE_SPECIES.vectorByteSize() - 1)) != 0) {
            throw new UnsupportedOperationException(STR."Unsupported vectorByteSize \{BYTE_SPECIES.vectorByteSize()}");
        }

        try {
            var f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main() throws InterruptedException, IOException {
        final var filePath = Paths.get(System.getProperty(FILE_PATH_PROPERTY, "./measurements.txt"));

        try (final var channel = FileChannel.open(filePath)) {
            final var inputMapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.global());

            final var chunkBounds = calcChunkBounds(inputMapped.address(), inputMapped.byteSize());
            final var maps = new SparseMap[N_THREADS];

            try (final var threadPool = Executors.newFixedThreadPool(N_THREADS, THREAD_BUILDER.factory());
                    final var singleThreadExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory())) {
                final var rootTask = CompletableFuture.runAsync(new CalculateAverageTask(maps, chunkBounds, 0), threadPool);

                final var futures = IntStream
                        .range(1, N_THREADS)
                        .mapToObj(t -> CompletableFuture
                                .runAsync(new CalculateAverageTask(maps, chunkBounds, t), threadPool)
                                .runAfterBothAsync(rootTask, () -> maps[0].merge(maps[t]), singleThreadExecutor))
                        .toArray(CompletableFuture[]::new);

                CompletableFuture.allOf(futures).join();
            }

            printSorted(maps[0]);
        }
    }

    private static long[] calcChunkBounds(final long mappedAddr, final long fileSizeBytes) {
        final var chunkBounds = new long[N_THREADS + 1];
        chunkBounds[0] = mappedAddr;
        chunkBounds[chunkBounds.length - 1] = mappedAddr + fileSizeBytes;

        final var chunkSize = (fileSizeBytes / N_THREADS) & -CalculateAverageTask.BATCH_SIZE_BYTES;
        for (int i = 1; i < chunkBounds.length - 1; i++) {
            chunkBounds[i] = chunkBounds[i - 1] + chunkSize;
        }

        return chunkBounds;
    }

    private static void printSorted(final SparseMap temperatureMeasurements) {
        final var weatherStations = new AggregatedMeasurement[(int) temperatureMeasurements.size];
        final var nameBuffer = new byte[WEATHER_STATION_LENGTH_MAX];
        var offset = temperatureMeasurements.denseAddress;
        for (int i = 0; i < weatherStations.length; i++, offset += SparseMap.DATA_SCALE * Long.BYTES) {
            final var nameAddr = UNSAFE.getLong(offset);
            final var nameLength = UNSAFE.getInt(offset + Integer.BYTES * 7);
            MemorySegment.copy(ALL, ValueLayout.JAVA_BYTE, nameAddr, nameBuffer, 0, nameLength);
            final var nameStr = new String(nameBuffer, 0, nameLength, StandardCharsets.UTF_8);
            weatherStations[i] = new AggregatedMeasurement(nameStr, i);
        }

        Arrays.sort(weatherStations);

        System.out.print('{');
        for (int i = 0; i < weatherStations.length - 1; i++) {
            printAggMeasurement(weatherStations[i], temperatureMeasurements);
            System.out.print(',');
            System.out.print(' ');
        }
        printAggMeasurement(weatherStations[weatherStations.length - 1], temperatureMeasurements);
        System.out.println('}');
    }

    private static void printAggMeasurement(final AggregatedMeasurement aggMeasurement,
                                            final SparseMap temperatureMeasurements) {
        final var offset = temperatureMeasurements.denseAddress + SparseMap.DATA_SCALE * Long.BYTES * aggMeasurement.id();

        // name
        System.out.print(aggMeasurement.name());
        System.out.print('=');

        // min
        printAsDouble(offset + Integer.BYTES * 5);
        System.out.print('/');

        // mean
        final double total = UNSAFE.getLong(offset + Integer.BYTES * 2);
        final var count = UNSAFE.getInt(offset + Integer.BYTES * 4);
        System.out.print(round(total / count / 10d));
        System.out.print('/');

        // max
        printAsDouble(offset + Integer.BYTES * 6);
    }

    private static void printAsDouble(final long addr) {
        final var val = (double) UNSAFE.getInt(addr);
        System.out.print(val / 10d);
    }

    private static double round(final double d) {
        return Math.round(d * 10d) / 10d;
    }

    private static class CalculateAverageTask implements Runnable {
        public static final int BATCH_SIZE_BYTES = BYTE_SPECIES.vectorByteSize();

        private final SparseMap[] maps;
        private final long[] chunkBounds;
        private final long chunkStart;
        private final long chunkEnd;
        private final int t;

        private SparseMap map;

        public CalculateAverageTask(SparseMap[] maps, long[] chunkBounds, int t) {
            this.maps = maps;
            this.chunkBounds = chunkBounds;
            this.chunkStart = chunkBounds[t];
            this.chunkEnd = chunkBounds[t + 1];
            this.t = t;
        }

        @Override
        public void run() {
            this.maps[this.t] = new SparseMap();
            this.map = this.maps[this.t];

            var lineStart = this.chunkBounds[0];
            // walk back to find the previous '\n' and use it as lineStart
            for (long i = this.chunkStart - 1; i > this.chunkBounds[0]; i--) {
                if (UNSAFE.getByte(i) == (byte) '\n') {
                    lineStart = i + 1L;
                    break;
                }
            }

            final var vectorLimit = this.chunkStart + ((this.chunkEnd - this.chunkStart) & -BYTE_SPECIES.vectorByteSize());
            for (long i = this.chunkStart; i < vectorLimit; i += BYTE_SPECIES.vectorByteSize()) {
                var lfMask = ByteVector.fromMemorySegment(BYTE_SPECIES, ALL, i, ByteOrder.nativeOrder())
                        .eq((byte) '\n')
                        .toLong();

                final var lfCount = Long.bitCount(lfMask);
                for (int j = 0; j < lfCount; j++) {
                    final var lfPosRelative = Long.numberOfTrailingZeros(lfMask);
                    final var lfAddress = i + lfPosRelative;
                    processLine(lineStart, lfAddress);

                    lineStart = lfAddress + 1L;
                    // unset the lowest set bit, should compile to BLSR
                    lfMask &= lfMask - 1L;
                }
            }

            if (vectorLimit != this.chunkEnd) {
                processTrailingBytes(lineStart, vectorLimit, this.chunkEnd);
            }
        }

        private void processTrailingBytes(long lineStart,
                                          final long start,
                                          final long end) {
            for (long i = start; i < end; i++) {
                final var b = UNSAFE.getByte(i);
                if (b != (byte) '\n') {
                    continue;
                }

                processLine(lineStart, i);
                lineStart = i + 1;
            }
        }

        private void processLine(final long lineStart, final long lfAddress) {
            // read 5 bytes before '\n'
            // the temperature is formatted to 1 decimal place
            // therefore the shortest temperature value is 0.0
            // so there are always at least 5 bytes between the location name and '\n'
            final var trailing5Bytes = UNSAFE.getLong(lfAddress - 5);
            final int trailingDWordRaw = (int) (trailing5Bytes >>> 8);

            // select the low nibble for each byte, '0'-'9' -> 0-9, ';' -> 11, '-' -> 13
            final var trailingDWordLowNibble = trailingDWordRaw & 0x0f_0f_0f_0f;
            // parse the 2 digits around the decimal point (note that these 2 digits must be present)
            final var trailingDigitsParsed = (trailingDWordLowNibble * 0x00_0a_00_01) >>> 24;

            // this byte must be ('-' & 0xf), (';' & 0xf), or a valid digit (0-9)
            final var secondHighestByte = trailingDWordLowNibble & 0xf;

            var temperature = trailingDigitsParsed;
            var lineLength = lfAddress - lineStart - 4;

            if (secondHighestByte > 9) {
                if (secondHighestByte == ('-' & 0xf)) {
                    lineLength--;
                    temperature = -temperature;
                }
            }
            else {
                lineLength--;
                temperature += secondHighestByte * 100;

                final var isNegative = (trailing5Bytes & 0xffL) == '-';
                if (isNegative) {
                    lineLength--;
                    temperature = -temperature;
                }
            }

            this.map.putEntry(lineStart, (int) lineLength, temperature);
        }
    }

    /**
     * Open addressing, linear probing hash map backed by off-heap memory
     */
    private static class SparseMap {
        private static final int TRUNCATED_HASH_BITS = 26;
        // max # of unique keys
        private static final long DENSE_SIZE = WEATHER_STATION_DISTINCT_MAX;
        // max hash code (exclusive)
        private static final long SPARSE_SIZE = 1L << (TRUNCATED_HASH_BITS + 1);
        private static final long DATA_SCALE = 4;

        public final long sparseAddress;
        public final long denseAddress;
        public long size;

        public SparseMap() {
            var arena = new MallocArena(Arena.global());
            var callocArena = new CallocArena(Arena.global());

            this.size = 0L;

            final var sparse = callocArena.allocate(ValueLayout.JAVA_LONG, SPARSE_SIZE);
            this.sparseAddress = (sparse.address() + MallocArena.MAX_ALIGN) & -MallocArena.MAX_ALIGN;

            final var dense = arena.allocate(ValueLayout.JAVA_LONG, DENSE_SIZE * DATA_SCALE);
            this.denseAddress = (dense.address() + MallocArena.MAX_ALIGN) & -MallocArena.MAX_ALIGN;
        }

        public void putEntry(final long keyAddress, final int keyLength, final int value) {
            final var hash = hash(keyAddress, keyLength);
            this.putEntryInternal(hash, keyAddress, keyLength, value, 1, value, value);
        }

        private void putEntryInternal(final long hash,
                                      final long keyAddress,
                                      final int keyLength,
                                      final long temperature,
                                      final int count,
                                      final int temperatureMin,
                                      final int temperatureMax) {
            final var sparseOffset = this.sparseAddress + truncateHash(hash) * Long.BYTES;

            for (long n = 0, sparseLinearOffset = sparseOffset; n < WEATHER_STATION_DISTINCT_MAX; n++, sparseLinearOffset += Long.BYTES) {
                final var denseOffset = UNSAFE.getLong(sparseLinearOffset);
                if (denseOffset == 0L) {
                    this.add(sparseLinearOffset, keyAddress, keyLength, temperature, count, temperatureMin, temperatureMax);
                    this.size++;
                    return;
                }

                if (isCollision(keyAddress, keyLength, denseOffset)) {
                    continue;
                }

                final var currTotal = UNSAFE.getLong(denseOffset + Integer.BYTES * 2);
                UNSAFE.putLong(denseOffset + Integer.BYTES * 2, currTotal + temperature); // total

                final var currCount = UNSAFE.getInt(denseOffset + Integer.BYTES * 4);
                UNSAFE.putInt(denseOffset + Integer.BYTES * 4, currCount + count); // count

                final var currMin = UNSAFE.getInt(denseOffset + Integer.BYTES * 5);
                if (temperatureMin < currMin) {
                    UNSAFE.putInt(denseOffset + Integer.BYTES * 5, temperatureMin); // min
                }

                final var currMax = UNSAFE.getInt(denseOffset + Integer.BYTES * 6);
                if (temperatureMax > currMax) {
                    UNSAFE.putInt(denseOffset + Integer.BYTES * 6, temperatureMax); // max
                }

                return;
            }
        }

        public void merge(final SparseMap other) {
            final var otherSize = other.size;
            for (long i = 0, offset = other.denseAddress; i < otherSize; i++, offset += DATA_SCALE * Long.BYTES) {
                final var keyAddress = UNSAFE.getLong(offset);
                final var keyLength = UNSAFE.getInt(offset + Integer.BYTES * 7);
                final var hash = hash(keyAddress, keyLength);

                this.putEntryInternal(
                        hash,
                        keyAddress,
                        keyLength,
                        UNSAFE.getLong(offset + Integer.BYTES * 2),
                        UNSAFE.getInt(offset + Integer.BYTES * 4),
                        UNSAFE.getInt(offset + Integer.BYTES * 5),
                        UNSAFE.getInt(offset + Integer.BYTES * 6));
            }
        }

        private void add(final long sparseOffset,
                         final long keyAddress,
                         final int keyLength,
                         final long temperature,
                         final int count,
                         final int temperatureMin,
                         final int temperatureMax) {
            // new entry, initialize sparse and dense
            final var denseOffset = this.denseAddress + this.size * DATA_SCALE * Long.BYTES;
            UNSAFE.putLong(sparseOffset, denseOffset);

            UNSAFE.putLong(denseOffset, keyAddress);
            UNSAFE.putLong(denseOffset + Integer.BYTES * 2, temperature);
            UNSAFE.putInt(denseOffset + Integer.BYTES * 4, count);
            UNSAFE.putInt(denseOffset + Integer.BYTES * 5, temperatureMin);
            UNSAFE.putInt(denseOffset + Integer.BYTES * 6, temperatureMax);
            UNSAFE.putInt(denseOffset + Integer.BYTES * 7, keyLength);
        }

        private static boolean isCollision(final long keyAddress, final int keyLength, final long denseOffset) {
            // key length compare is unnecessary

            final var entryKeyAddress = UNSAFE.getLong(denseOffset);
            return mismatch(keyAddress, entryKeyAddress, keyLength);
        }

        private static boolean mismatch(final long leftAddr, final long rightAddr, final int length) {
            // key length compare is unnecessary
            // strings compared through delimiter byte ';'

            final var loopBound = length >= (BYTE_SPECIES.vectorByteSize() - 1) ? ((length + 1) & -BYTE_SPECIES.vectorByteSize()) : 0;
            for (long i = 0; i < loopBound; i += BYTE_SPECIES.vectorByteSize()) {
                final var l = ByteVector.fromMemorySegment(BYTE_SPECIES, ALL, leftAddr + i, ByteOrder.nativeOrder());
                final var r = ByteVector.fromMemorySegment(BYTE_SPECIES, ALL, rightAddr + i, ByteOrder.nativeOrder());
                if (!l.eq(r).allTrue()) {
                    return true;
                }
            }

            final var l = ByteVector.fromMemorySegment(BYTE_SPECIES, ALL, leftAddr + loopBound, ByteOrder.nativeOrder());
            final var r = ByteVector.fromMemorySegment(BYTE_SPECIES, ALL, rightAddr + loopBound, ByteOrder.nativeOrder());
            final var eqMask = l.eq(r).toLong();

            // LE compare to add 1 to length
            return Long.numberOfTrailingZeros(~eqMask) <= (length - loopBound);
            // to support platforms without TZCNT, the check can be replaced with
            // a comparison to lowestZero = ~eqMask & (eqMask + 1)
        }

        // Use the leading and trailing few bytes as hash
        // this performs better than computing a good hash
        private static long hash(final long keyAddress, final int keyLength) {
            final var leadingQWord = UNSAFE.getLong(keyAddress);
            // the constant is the 64 bit FNV-1 offset basis
            final var hash = -3750763034362895579L ^ leadingQWord;
            if (keyLength < Integer.BYTES) {
                // the key is at least 2 bytes (if you count the delimiter)
                return hash & 0xffffL;
            }
            else {
                final var trailingDWord = UNSAFE.getLong(keyAddress + keyLength - Integer.BYTES) & 0xffffffffL;
                // only the lower dword in hash is guaranteed to exist so shift left 32
                return (hash << Integer.SIZE) ^ trailingDWord;
            }
        }

        private static long truncateHash(final long hash) {
            return ((hash >>> TRUNCATED_HASH_BITS) ^ hash) & ((1L << TRUNCATED_HASH_BITS) - 1L);
        }
    }

    private static class MallocArena implements Arena {
        public static final long MAX_ALIGN = 1L << 21;

        protected static final Linker LINKER = Linker.nativeLinker();
        protected static final AddressLayout C_POINTER = (AddressLayout) LINKER.canonicalLayouts().get("void*");
        protected static final ValueLayout C_SIZE_T = (ValueLayout) LINKER.canonicalLayouts().get("size_t");
        private static final MethodHandle MALLOC = LINKER.downcallHandle(
                LINKER.defaultLookup().find("malloc").orElseThrow(),
                FunctionDescriptor.of(C_POINTER, C_SIZE_T),
                Linker.Option.critical(false));
        private static final MethodHandle FREE = LINKER.downcallHandle(
                LINKER.defaultLookup().find("free").orElseThrow(),
                FunctionDescriptor.ofVoid(C_POINTER),
                Linker.Option.critical(false));
        protected static final MethodHandle CALLOC = LINKER.downcallHandle(
                LINKER.defaultLookup().find("calloc").orElseThrow(),
                FunctionDescriptor.of(C_POINTER, C_SIZE_T, C_SIZE_T),
                Linker.Option.critical(false));

        private final Arena arena;

        public MallocArena(Arena arena) {
            this.arena = arena;
        }

        @Override
        public MemorySegment allocate(final long byteSize, final long byteAlignment) {
            return malloc(byteSize + MAX_ALIGN).reinterpret(this, MallocArena::free);
        }

        @Override
        public MemorySegment.Scope scope() {
            return arena.scope();
        }

        @Override
        public void close() {
            arena.close();
        }

        private static MemorySegment malloc(final long byteSize) {
            try {
                return ((MemorySegment) MALLOC.invokeExact(byteSize)).reinterpret(byteSize);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        protected static void free(final MemorySegment address) {
            try {
                FREE.invokeExact(address);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class CallocArena extends MallocArena {
        public CallocArena(Arena arena) {
            super(arena);
        }

        @Override
        public MemorySegment allocate(final long byteSize, final long byteAlignment) {
            return calloc(byteSize + MAX_ALIGN).reinterpret(this, MallocArena::free);
        }

        private static MemorySegment calloc(final long byteSize) {
            try {
                return ((MemorySegment) MallocArena.CALLOC.invokeExact(1L, byteSize)).reinterpret(byteSize);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    private record AggregatedMeasurement(String name, long id) implements Comparable<AggregatedMeasurement> {

    @Override
    public int compareTo(final AggregatedMeasurement other) {
        return name.compareTo(other.name);
    }
}}
