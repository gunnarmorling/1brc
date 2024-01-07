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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
})
@Threads(1)
public class AllTrueMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');
    private int value1;
    private int value2;

    @Setup
    public void setup() {
        value1 = ThreadLocalRandom.current().nextInt();
        value2 = ThreadLocalRandom.current().nextInt();
    }

    @Benchmark
    // @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public long testTrue() {
        ByteVector byteVector1 = ByteVector.broadcast(ByteVector.SPECIES_256, (byte) value1);
        ByteVector byteVector2 = ByteVector.broadcast(ByteVector.SPECIES_256, (byte) value2);
        return byteVector1.compare(VectorOperators.EQ, byteVector2).firstTrue();
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(AllTrueMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
