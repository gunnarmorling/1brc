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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
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
public class BasicMicrobenchmark {

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
        long newlinesCount = 0;
        long offset = 0;
        long idsIndex = 0;
        long consumed = 0;
        do {
            Vector<Byte> current = SPECIES.fromMemorySegment(inputData, offset, ByteOrder.nativeOrder());
            Vector<Byte> next = SPECIES.fromMemorySegment(inputData, offset + SPECIES.length(), ByteOrder.nativeOrder());

            int index = (int) (consumed & 31);
            Vector<Byte> cr = current.rearrange(LEFT_SHIFT_LOOKUP[index]);
            Vector<Byte> nr = next.rearrange(RIGHT_SHIFT_LOOKUP[32 - index]);
            current = nr.blend(cr, CITY_LOOKUP_MASK[32 - index]);

            // if (consumed % 1000_0000 == 0) {
            // System.out.println("current = " + toString(current).replaceAll("\\n", "#"));
            // System.out.println("next = " + toString(next).replaceAll("\\n", "#"));
            // }

            // VectorMask<Byte> currentDelimiters = current.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            VectorMask<Byte> currentNewlines = current.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            // calculate hash
            {
                // VectorMask<Byte> hashMask = CITY_LOOKUP_MASK[currentDelimiters.firstTrue()];
                // Vector<Byte> hashInput = ZERO.blend(current, hashMask);
                //
                // long perfectHash64 = hashInput.reinterpretAsLongs().reduceLanesToLong(VectorOperators.ADD);
                // int perfectHash32 = (int) ((perfectHash64 >> 32) ^ perfectHash64);

                // System.out.println(toString(hashInput));
                newlinesCount++;
                // ids.set(ValueLayout.JAVA_INT, idsIndex, perfectHash32);
                // idsIndex += 4;
            }

            // Slow path
            // if (!currentNewlines.anyTrue()) {
            // consumed += 32;
            // while (inputData.get(ValueLayout.JAVA_BYTE, consumed) != '\n') {
            // consumed++;
            // }
            //
            // idsIndex++;
            // consumed++;

            // System.out.println("current = " + toString(current).replaceAll("\\n", "#"));
            // System.out.println("next = " + toString(next).replaceAll("\\n", "#"));
            //
            // System.out.println("consumed = " + consumed);
            // throw new RuntimeException();
            // } else {
            consumed += currentNewlines.firstTrue() + 1;
            // }

            offset = consumed & -32;
            // int slotsNeeded = SPECIES.length() - firstNewlinePosition;
            // Vector<Byte> cr = current.rearrange(LEFT_SHIFT_LOOKUP[firstNewlinePosition + 1]);
            // Vector<Byte> nr = next.rearrange(RIGHT_SHIFT_LOOKUP[slotsNeeded]);
            //
            // current = nr.blend(cr, CITY_LOOKUP_MASK[slotsNeeded]);
            //
            // offset += newlinePosition + 1;
        } while (consumed < inputData.byteSize() - 1000);

        System.out.println("newlinesCount = " + newlinesCount);
        System.out.println("exceptions count = " + idsIndex);
        return newlinesCount;
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
                .include(BasicMicrobenchmark.class.getSimpleName())
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
