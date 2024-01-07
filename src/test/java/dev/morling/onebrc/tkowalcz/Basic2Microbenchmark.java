package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
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
public class Basic2Microbenchmark {

    private static final String FILE = "measurements.txt";

    public static final int TABLE_SIZE = 0x40000;
    public static final int TABLE_SIZE_MASK = 0x40000 - 1;

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    private static final VectorShuffle<Byte>[] LEFT_SHIFT_LOOKUP = createShuffleLeftShift32();

    private static final VectorShuffle<Byte>[] RIGHT_SHIFT_LOOKUP = createShuffleRightShift32();

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
    public long test() {
        int offset = 0;
        int consumed = 0;
        do {
            Vector<Byte> current = SPECIES.fromMemorySegment(inputData, offset, ByteOrder.nativeOrder());
            Vector<Byte> next = SPECIES.fromMemorySegment(inputData, offset + SPECIES.length(), ByteOrder.nativeOrder());

            int index = consumed & (SPECIES.length() - 1);
            Vector<Byte> cr = current.rearrange(LEFT_SHIFT_LOOKUP[index]);
            Vector<Byte> nr = next.rearrange(RIGHT_SHIFT_LOOKUP[32 - index]);
            current = nr.blend(cr, CITY_LOOKUP_MASK[32 - index]);

            VectorMask<Byte> currentDelimiters = current.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            VectorMask<Byte> currentNewlines = current.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            // calculate hash
            {
                VectorMask<Byte> hashMask = CITY_LOOKUP_MASK[currentDelimiters.firstTrue()];
                Vector<Byte> hashInput = ZERO.blend(current, hashMask);

                long perfectHash64 = hashInput.reinterpretAsLongs().reduceLanesToLong(VectorOperators.MUL);
                int perfectHash32 = (int) ((perfectHash64 >> 32) ^ perfectHash64);

                int mapIndex = perfectHash32 & TABLE_SIZE_MASK;
                CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate = dataTable[mapIndex];
                if (statisticsAggregate == null) {
                    byte[] city = new byte[SPECIES.length()];
                    hashInput.reinterpretAsBytes().intoArray(city, 0, hashMask);

                    statisticsAggregate = new CalculateAverage_tkowalcz.StatisticsAggregate(city, hashMask.trueCount());
                    dataTable[mapIndex] = statisticsAggregate;
                }
                else {
                    ByteVector city = ByteVector.fromArray(SPECIES, statisticsAggregate.getCity(), 0);
                    if (!city.compare(VectorOperators.EQ, hashInput).allTrue()) {
                        // System.out.println("collision");
                    }
                }
            }

            // Slow path
            if (!currentNewlines.anyTrue()) {
                consumed += 32;
                while (inputData.get(ValueLayout.JAVA_BYTE, consumed) != '\n') {
                    consumed++;
                }

                consumed++;
            }
            else {
                consumed += currentNewlines.firstTrue() + 1;
            }

            offset = consumed & -SPECIES.length();
        } while (consumed < inputData.byteSize() - 1000);

        return consumed;
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
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(Basic2Microbenchmark.class.getSimpleName())
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
