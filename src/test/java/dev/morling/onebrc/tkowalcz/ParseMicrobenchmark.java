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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 0, jvmArgsPrepend = {
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
public class ParseMicrobenchmark {

    private static final Vector<Byte> ASCII_ZERO = ByteVector.SPECIES_256.broadcast('0');

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

    private byte[] value;
    private int position;

    @Setup
    public void setup() {
        value = "Warszawa;-12.3\nKraków;0.4\nSuwałki;-4.3".getBytes(StandardCharsets.UTF_8);
        position = 9;
    }

    @Benchmark
    public long parseScalar() {
        long result = 0;
        if (value[position] == '-') {
            position++;
        }

        byte v1 = (byte) (value[position] - '0');
        byte v2 = (byte) (value[position + 1] - '0');
        byte v3 = (byte) (value[position + 2] - '0');
        byte v4 = (byte) (value[position + 3] - '0');

        boolean negative = value[position] == '-';
        if (v3 == '.') {

        }
        if (v1 == '-') {

        }
        else {
            v1 -= '0';
        }

        // byte v2 = value[position + 1];
        // if (v2 == '.') {
        // return
        // }
        // byte v3 = value[position + 2]; This is always '.'
        // byte v4 = value[position + 3];

        return v4 + v2 * 10 + v1 * 100;
    }

    @Benchmark
    public float parseFloat() {
        int integerPart = 0;
        int fractionalPart = 0;
        float negative = 1.0f;

        int i = position;
        if (value[i] == '-') {
            negative = -1.0f;
            i++;
        }

        for (; i < value.length; i++) {
            if (value[i] == '.') {
                break;
            }

            integerPart *= 10;
            integerPart += value[i] - '0';
        }

        i++;
        fractionalPart = value[i] - '0';
        return negative / fractionalPart + integerPart;
    }

    @Benchmark
    public long parseVectorised() {
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256, value, 0);

        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x00_00_00_00_00_00_00_0FL);

        return vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(ParseMicrobenchmark.class.getSimpleName())
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
