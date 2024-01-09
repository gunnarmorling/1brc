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
public class VectorisedHashMicrobenchmark {

    public static final int TABLE_SIZE = 0x20000;
    public static final int TABLE_SIZE_MASK = 0x20000 - 1;
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    private static final Vector<Byte> CITY = SPECIES.fromArray("Las Palmas de Gran Canaria      ".getBytes(StandardCharsets.UTF_8), 0);

    @Benchmark
    public int computeHash() {
        VectorMask<Byte> hashMask = CITY_LOOKUP_MASK[26];
        Vector<Byte> hashInput = ZERO.blend(CITY, hashMask);

        long perfectHash64 = hashInput.reinterpretAsLongs().reduceLanesToLong(VectorOperators.XOR);

        int perfectHash32 = (int) ((perfectHash64 >> 32) ^ perfectHash64);
        return perfectHash32 & TABLE_SIZE_MASK;
    }

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[32];
        result[0] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 32; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(VectorisedHashMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
