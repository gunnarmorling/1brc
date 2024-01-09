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
@Fork(value = 0, jvmArgsPrepend = {
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
                .build();

        new Runner(opt).run();
    }
}
