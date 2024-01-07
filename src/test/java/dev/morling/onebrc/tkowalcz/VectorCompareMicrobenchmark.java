package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
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
public class VectorCompareMicrobenchmark {

    private byte[][] cities1;
    private byte[][] cities2;

    @Setup
    public void setup() {
        cities1 = new byte[4][];

        cities1[0] = new byte[32];
        ThreadLocalRandom.current().nextBytes(cities1[0]);
        cities1[1] = new byte[32];
        ThreadLocalRandom.current().nextBytes(cities1[1]);
        cities1[2] = new byte[32];
        ThreadLocalRandom.current().nextBytes(cities1[2]);
        cities1[3] = new byte[32];
        ThreadLocalRandom.current().nextBytes(cities1[3]);

        cities2 = Arrays.copyOf(cities1, cities1.length);
    }

    @Benchmark
    public boolean compareLanewise() {
        ByteVector v1 = ByteVector.fromArray(ByteVector.SPECIES_256, cities1[0], 0);
        ByteVector v2 = ByteVector.fromArray(ByteVector.SPECIES_256, cities2[0], 0);

        // vmovdqu ymm0,YMMWORD PTR [r12+r10*8+0x10];
        // vmovdqu ymm2,YMMWORD PTR [r12+r11*8+0x10];
        // vpcmpeqb ymm0,ymm0,ymm2
        // vptest ymm0,ymm1
        return v1.compare(VectorOperators.EQ, v2).allTrue();
    }

    @Benchmark
    public boolean compareLanewise4() {
        ByteVector v11 = ByteVector.fromArray(ByteVector.SPECIES_256, cities1[0], 0);
        ByteVector v12 = ByteVector.fromArray(ByteVector.SPECIES_256, cities1[1], 0);
        ByteVector v13 = ByteVector.fromArray(ByteVector.SPECIES_256, cities1[2], 0);
        ByteVector v14 = ByteVector.fromArray(ByteVector.SPECIES_256, cities1[3], 0);

        ByteVector v21 = ByteVector.fromArray(ByteVector.SPECIES_256, cities2[0], 0);
        ByteVector v22 = ByteVector.fromArray(ByteVector.SPECIES_256, cities2[1], 0);
        ByteVector v23 = ByteVector.fromArray(ByteVector.SPECIES_256, cities2[2], 0);
        ByteVector v24 = ByteVector.fromArray(ByteVector.SPECIES_256, cities2[3], 0);

        return v11.compare(VectorOperators.EQ, v21).allTrue() &
                v12.compare(VectorOperators.EQ, v22).allTrue() &
                v13.compare(VectorOperators.EQ, v23).allTrue() &
                v14.compare(VectorOperators.EQ, v24).allTrue();
    }

    @Benchmark
    public boolean compareSubtract() {
        ByteVector v1 = ByteVector.fromArray(ByteVector.SPECIES_256, cities1[0], 0);
        ByteVector v2 = ByteVector.fromArray(ByteVector.SPECIES_256, cities2[0], 0);

        return v1.sub(v2).reduceLanesToLong(VectorOperators.OR) == 0;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(VectorCompareMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
