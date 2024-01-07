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
package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * This is similar to the final submission but uses quad pumped processing - four simultaneous calculations in the same
 * thread. It was slower than double pumped version.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        // "-XX:NodeCountInliningCutoff=32000",
        "-XX:LiveNodeCountInliningCutoff=80000",
        // "-XX:+LogVMOutput",
        // "-XX:+LogCompilation",
        // "-XX:+PrintCompilation",
        // "-XX:+PrintCompilation2",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
})
@Threads(1)
public class UnalignedQuadMicrobenchmark {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');

    private static final Vector<Byte> ASCII_MINUS = SPECIES.broadcast('-');

    private static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    private static final VectorMask<Byte>[] MEASUREMENT_LOOKUP_MASK = createMasks32();

    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    private static final int[] STOI_SIZE_LOOKUP = { 0, 0, 0, 0, 5, 5, 0, 0, 0, 6, 4 };

    public static final int TABLE_SIZE = 0x400000;
    public static final int TABLE_SIZE_MASK = 0x400000 - 1;

    private Arena arena;
    private MemorySegment inputData;

    private CalculateAverage_tkowalcz.StatisticsAggregate[] dataTable;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);
            dataTable = new CalculateAverage_tkowalcz.StatisticsAggregate[TABLE_SIZE];
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public void test() {
        long dataSize = inputData.byteSize();
        long quarterData = dataSize / 4;

        long collisions = 0;

        long offset1 = 0;
        long offset2 = findPastNewline(inputData, quarterData);
        long offset3 = findPastNewline(inputData, quarterData + quarterData);
        long offset4 = findPastNewline(inputData, quarterData + quarterData + quarterData);
        do {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset1 += firstDelimiter1 + 1;

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            int firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset2 += firstDelimiter2 + 1;

            Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            int firstDelimiter3 = byteVector3.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset3 += firstDelimiter3 + 1;

            Vector<Byte> byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            int firstDelimiter4 = byteVector4.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset4 += firstDelimiter4 + 1;

            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);

            long perfectHash64_1 = hashInput1.reinterpretAsInts().reduceLanesToLong(VectorOperators.ADD);
            int perfectHash32_1 = (int) ((perfectHash64_1 >> 32) ^ perfectHash64_1);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;

            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_1 = dataTable[index1];
            if (statisticsAggregate_1 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput1.reinterpretAsBytes().intoArray(city, 0, hashMask1);

                statisticsAggregate_1 = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask1.trueCount());
                dataTable[index1] = statisticsAggregate_1;
            }
            else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_1.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                    collisions++;
                }
            }

            VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[firstDelimiter2];
            Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);

            long perfectHash64_2 = hashInput2.reinterpretAsInts().reduceLanesToLong(VectorOperators.ADD);
            int perfectHash32_2 = (int) ((perfectHash64_2 >> 32) ^ perfectHash64_2);
            int index2 = perfectHash32_2 & TABLE_SIZE_MASK;

            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_2 = dataTable[index2];
            if (statisticsAggregate_2 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput2.reinterpretAsBytes().intoArray(city, 0, hashMask2);

                statisticsAggregate_2 = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask2.trueCount());
                dataTable[index2] = statisticsAggregate_2;
            }
            else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_2.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput2).allTrue()) {
                    collisions++;
                }
            }

            VectorMask<Byte> hashMask3 = CITY_LOOKUP_MASK[firstDelimiter3];
            Vector<Byte> hashInput3 = ZERO.blend(byteVector3, hashMask3);

            long perfectHash64_3 = hashInput3.reinterpretAsInts().reduceLanesToLong(VectorOperators.ADD);
            int perfectHash32_3 = (int) ((perfectHash64_3 >> 32) ^ perfectHash64_3);
            int index3 = perfectHash32_3 & TABLE_SIZE_MASK;

            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_3 = dataTable[index3];
            if (statisticsAggregate_3 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput3.reinterpretAsBytes().intoArray(city, 0, hashMask3);

                statisticsAggregate_3 = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask3.trueCount());
                dataTable[index3] = statisticsAggregate_3;
            }
            else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_3.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput3).allTrue()) {
                    collisions++;
                }
            }

            VectorMask<Byte> hashMask4 = CITY_LOOKUP_MASK[firstDelimiter4];
            Vector<Byte> hashInput4 = ZERO.blend(byteVector4, hashMask4);

            long perfectHash64_4 = hashInput4.reinterpretAsInts().reduceLanesToLong(VectorOperators.ADD);
            int perfectHash32_4 = (int) ((perfectHash64_4 >> 32) ^ perfectHash64_4);
            int index4 = perfectHash32_4 & TABLE_SIZE_MASK;

            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_4 = dataTable[index4];
            if (statisticsAggregate_4 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput4.reinterpretAsBytes().intoArray(city, 0, hashMask4);

                statisticsAggregate_4 = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask4.trueCount());
                dataTable[index4] = statisticsAggregate_4;
            }
            else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_4.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput4).allTrue()) {
                    collisions++;
                }
            }

            byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) (mask1.toLong() & 0x0F);

            long value = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_1.accept(value);
            offset1 += STOI_SIZE_LOOKUP[lookupIndex1];

            byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex2 = (int) (mask2.toLong() & 0x0F);

            value = byteVector2
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex2])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_2.accept(value);
            offset2 += STOI_SIZE_LOOKUP[lookupIndex2];

            byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            VectorMask<Byte> mask3 = byteVector3.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex3 = (int) (mask3.toLong() & 0x0F);

            value = byteVector3
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex3])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_3.accept(value);
            offset3 += STOI_SIZE_LOOKUP[lookupIndex3];

            byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            VectorMask<Byte> mask4 = byteVector4.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex4 = (int) (mask4.toLong() & 0x0F);

            value = byteVector4
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex4])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_4.accept(value);
            offset4 += STOI_SIZE_LOOKUP[lookupIndex4];
        } while (offset4 < inputData.byteSize());

        System.out.println("collisions = " + collisions);
    }

    private int parseTemperatureToLongAndReturnOffset(long offset, CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate) {
        Vector<Byte> vector = SPECIES.fromMemorySegment(inputData, offset, ByteOrder.nativeOrder());
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        statisticsAggregate.accept(value);
        return STOI_SIZE_LOOKUP[lookupIndex];
    }

    private CalculateAverage_tkowalcz.StatisticsAggregate getStatisticsAggregate(Vector<Byte> hashInput, VectorMask<Byte> hashMask, int index) {
        CalculateAverage_tkowalcz.StatisticsAggregate result = dataTable[index];

        if (result == null) {
            byte[] city = new byte[SPECIES.length()];
            hashInput.reinterpretAsBytes().intoArray(city, 0, hashMask);

            result = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask.trueCount());
            dataTable[index] = result;
        }

        return result;
    }

    private long findPastNewline(MemorySegment inputData, long position) {
        while (inputData.get(ValueLayout.JAVA_BYTE, position) != '\n') {
            position++;
        }
        return position + 1;
    }

    private static String toString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        return new String(array, StandardCharsets.UTF_8);
    }

    private static MemorySegment mmapDataFile(String fileName, Arena arena) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[33];
        result[0] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 33; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    public static VectorShuffle<Byte>[] createShuffleLeftShift32() {
        int[] indices = IntStream.range(0, 32).toArray();
        VectorShuffle<Byte>[] result = new VectorShuffle[33];

        for (int i = 0; i < 33; i++) {
            result[i] = VectorShuffle.fromValues(SPECIES, indices);
            int j = 0;
            for (; j < 32 - i; j++) {
                indices[j]++;
                if (indices[j] >= 32) {
                    indices[j] = 0;
                }
            }

            for (; j < 32; j++) {
                indices[j] = 0;
            }
        }

        return result;
    }

    public static VectorShuffle<Byte>[] createShuffleRightShift32() {
        int[] indices = IntStream.range(0, 32).toArray();
        VectorShuffle<Byte>[] result = new VectorShuffle[33];

        for (int i = 0; i < 33; i++) {
            result[i] = VectorShuffle.fromValues(SPECIES, indices);
            for (int j = 0; j < 32; j++) {
                indices[j]--;
                if (indices[j] <= 0) {
                    indices[j] = 0;
                }
            }
        }

        return result;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;
        // Class<? extends Profiler> profilerClass = JavaFlightRecorderProfiler.class;
        Class<? extends Profiler> profilerClass = GCProfiler.class;

        Options opt = new OptionsBuilder()
                .include(UnalignedQuadMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                // .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
