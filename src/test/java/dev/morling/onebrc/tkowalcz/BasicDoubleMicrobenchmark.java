package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+LogVMOutput",
        "-XX:CompileCommand=print,*.selectWhereVector2",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-XX:MaxDirectMemorySize=10737418240"
})
@Threads(1)
public class BasicDoubleMicrobenchmark {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    private static final VectorShuffle<Byte>[] LEFT_SHIFT_LOOKUP = createShuffleLeftShift32();

    private static final VectorShuffle<Byte>[] RIGHT_SHIFT_LOOKUP = createShuffleRightShift32();

    private Arena arena;
    private MemorySegment inputData;
    // private MemorySegment ids;
    // private MemorySegment values;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);

            // ids = arena.allocate(1024L * 1024L * 1024L * 4, 32);
            // values = arena.allocate(1024L * 1024L * 1024L * 4, 32);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void tearDown() {
        arena.close();
    }

    // Colombo;16.0\Villahermosa;15.5\L jubljana;15.9\Valencia;11.8\Port
    // .......T.................T......
    // ............T.................T.
    //
    // Colombo;16.0\Villahermosa;15.5\L jubljana;15.9\Valencia;11.8\Port
    // Villahermosa;15.5\Ljubljana;15.9 \Valencia;11.8\Port Sudan;28.8\A
    // Ljubljana;15.9\Valencia;11.8\Por t Sudan;28.8\Assab;26.9\Muscat;2
    //
    // Colombo;16.0\Villahermosa;15.5\L jubljana;15.9\Valencia;11.8\Port
    // Villahermosa;15.5\Ljubljana;15.9 \Valencia;11.8\Port t Sudan;28.8\Assab;26.9\Muscat;2
    // Ljubljana;15.9\Valencia;11.8\Por t Sudan;28.8\Assab;26.9\Muscat;2

    @Benchmark
    public long test() {
        long dataSize = inputData.byteSize();
        // long half = findHalf(inputData, 0, dataSize);
        // long half = findHalf(inputData, 0, dataSize);
        long newlinesCount1 = 0;
        long quarterData = dataSize / 4;
        long offset1 = 0;
        long offset2 = findPastNewline(inputData, quarterData);
        long offset3 = findPastNewline(inputData, quarterData + quarterData);
        long offset4 = findPastNewline(inputData, quarterData + quarterData + quarterData);
        long idsIndex = 0;
        long consumed1 = 0;
        long consumed2 = offset2;
        long consumed3 = offset3;
        long consumed4 = offset4;
        do {
            Vector<Byte> current1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            Vector<Byte> next1 = SPECIES.fromMemorySegment(inputData, offset1 + SPECIES.length(), ByteOrder.nativeOrder());

            Vector<Byte> current2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            Vector<Byte> next2 = SPECIES.fromMemorySegment(inputData, offset2 + SPECIES.length(), ByteOrder.nativeOrder());

            Vector<Byte> current3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            Vector<Byte> next3 = SPECIES.fromMemorySegment(inputData, offset3 + SPECIES.length(), ByteOrder.nativeOrder());

            Vector<Byte> current4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            Vector<Byte> next4 = SPECIES.fromMemorySegment(inputData, offset4 + SPECIES.length(), ByteOrder.nativeOrder());

            int index1 = (int) (consumed1 & 31);
            int index2 = (int) (consumed2 & 31);
            int index3 = (int) (consumed3 & 31);
            int index4 = (int) (consumed4 & 31);

            Vector<Byte> cr1 = current1.rearrange(LEFT_SHIFT_LOOKUP[index1]);
            Vector<Byte> nr1 = next1.rearrange(RIGHT_SHIFT_LOOKUP[32 - index1]);
            current1 = nr1.blend(cr1, CITY_LOOKUP_MASK[32 - index1]);

            Vector<Byte> cr2 = current2.rearrange(LEFT_SHIFT_LOOKUP[index2]);
            Vector<Byte> nr2 = next2.rearrange(RIGHT_SHIFT_LOOKUP[32 - index2]);
            current2 = nr2.blend(cr2, CITY_LOOKUP_MASK[32 - index2]);

            Vector<Byte> cr3 = current3.rearrange(LEFT_SHIFT_LOOKUP[index3]);
            Vector<Byte> nr3 = next3.rearrange(RIGHT_SHIFT_LOOKUP[32 - index3]);
            current3 = nr3.blend(cr3, CITY_LOOKUP_MASK[32 - index3]);

            Vector<Byte> cr4 = current4.rearrange(LEFT_SHIFT_LOOKUP[index4]);
            Vector<Byte> nr4 = next4.rearrange(RIGHT_SHIFT_LOOKUP[32 - index4]);
            current4 = nr4.blend(cr4, CITY_LOOKUP_MASK[32 - index4]);

            // if (consumed1 % 1000_0000 == 0) {
            // System.out.println("current1 = " + toString(current1).replaceAll("\\n", "#"));
            // System.out.println("next1 = " + toString(next1).replaceAll("\\n", "#"));
            // }

            VectorMask<Byte> currentDelimiters1 = current1.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            VectorMask<Byte> currentDelimiters2 = current2.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            VectorMask<Byte> currentDelimiters3 = current3.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            VectorMask<Byte> currentDelimiters4 = current4.compare(VectorOperators.EQ, DELIMITER_VECTOR);

            VectorMask<Byte> currentNewlines1 = current1.compare(VectorOperators.EQ, NEWLINE_VECTOR);
            VectorMask<Byte> currentNewlines2 = current2.compare(VectorOperators.EQ, NEWLINE_VECTOR);
            VectorMask<Byte> currentNewlines3 = current3.compare(VectorOperators.EQ, NEWLINE_VECTOR);
            VectorMask<Byte> currentNewlines4 = current4.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            // calculate hash
            {
                VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[currentDelimiters1.firstTrue()];
                Vector<Byte> hashInput1 = ZERO.blend(current1, hashMask1);

                long perfectHash641 = hashInput1.reinterpretAsLongs().reduceLanesToLong(VectorOperators.ADD);
                int perfectHash321 = (int) ((perfectHash641 >> 32) ^ perfectHash641);

                VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[currentDelimiters2.firstTrue()];
                Vector<Byte> hashInput2 = ZERO.blend(current2, hashMask2);

                long perfectHash642 = hashInput2.reinterpretAsLongs().reduceLanesToLong(VectorOperators.ADD);
                int perfectHash322 = (int) ((perfectHash642 >> 32) ^ perfectHash642);

                VectorMask<Byte> hashMask3 = CITY_LOOKUP_MASK[currentDelimiters3.firstTrue()];
                Vector<Byte> hashInput3 = ZERO.blend(current3, hashMask2);

                long perfectHash643 = hashInput3.reinterpretAsLongs().reduceLanesToLong(VectorOperators.ADD);
                int perfectHash323 = (int) ((perfectHash643 >> 32) ^ perfectHash643);

                VectorMask<Byte> hashMask4 = CITY_LOOKUP_MASK[currentDelimiters4.firstTrue()];
                Vector<Byte> hashInput4 = ZERO.blend(current4, hashMask4);

                long perfectHash644 = hashInput4.reinterpretAsLongs().reduceLanesToLong(VectorOperators.ADD);
                int perfectHash324 = (int) ((perfectHash644 >> 32) ^ perfectHash644);

                // System.out.println(toString(hashInput));
                newlinesCount1++;
                // ids.set(ValueLayout.JAVA_INT, idsIndex, perfectHash32);
                // idsIndex += 4;
            }

            // Slow path
            // if (!currentNewlines1.anyTrue()) {
            // consumed1 += 32;
            // while (inputData.get(ValueLayout.JAVA_BYTE, consumed1) != '\n') {
            // consumed1++;
            // }
            //
            // idsIndex++;
            // consumed1++;

            // System.out.println("current1 = " + toString(current1).replaceAll("\\n", "#"));
            // System.out.println("next1 = " + toString(next1).replaceAll("\\n", "#"));
            //
            // System.out.println("consumed1 = " + consumed1);
            // throw new RuntimeException();
            // } else {
            consumed1 += currentNewlines1.firstTrue() + 1;
            consumed2 += currentNewlines2.firstTrue() + 1;
            consumed3 += currentNewlines3.firstTrue() + 1;
            consumed4 += currentNewlines4.firstTrue() + 1;
            // }

            offset1 = consumed1 & -32;
            offset2 = consumed2 & -32;
            offset3 = consumed3 & -32;
            offset4 = consumed4 & -32;
            // int slotsNeeded = SPECIES.length() - firstNewlinePosition;
            // Vector<Byte> cr1 = current1.rearrange(LEFT_SHIFT_LOOKUP[firstNewlinePosition + 1]);
            // Vector<Byte> nr1 = next1.rearrange(RIGHT_SHIFT_LOOKUP[slotsNeeded]);
            //
            // current1 = nr1.blend(cr1, CITY_LOOKUP_MASK[slotsNeeded]);
            //
            // offset1 += newlinePosition + 1;
        } while (consumed4 < inputData.byteSize() - 100);

        // System.out.println("newlinesCount1 = " + newlinesCount1);
        // System.out.println("exceptions count = " + idsIndex);
        return newlinesCount1;
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
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(BasicDoubleMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                // .threads(3)
                .build();

        new Runner(opt).run();
    }
}
