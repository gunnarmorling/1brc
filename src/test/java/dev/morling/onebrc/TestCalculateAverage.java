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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

interface Runner {
    void run() throws Exception;
}

public class TestCalculateAverage {

    @ParameterizedTest
    @ValueSource(classes = {
            CalculateAverage.class,

            //
            // Failing implementations commented out
            //
            CalculateAverage_bjhara.class,
            // CalculateAverage_criccomini.class,
            // CalculateAverage_ddimtirov.class,
            // CalculateAverage_ebarlas.class,
            CalculateAverage_filiphr.class,
            // CalculateAverage_itaske.class,
            CalculateAverage_khmarbaise.class,
            CalculateAverage_kuduwa_keshavram.class,
            // CalculateAverage_naive.class,
            // CalculateAverage_padreati.class,
            // CalculateAverage_palmr.class,
            CalculateAverage_richardstartin.class,
            // CalculateAverage_royvanrijn.class,
            // CalculateAverage_seijikun.class,
            // CalculateAverage_spullara.class,
            CalculateAverage_truelive.class,
    })
    void TestSamples(Class<?> clazz) throws Exception {
        var main = clazz.getMethod("main", String[].class);
        runSamples(clazz.getSimpleName(), () -> {
            main.invoke(null, (Object) (new String[]{}));
        });
    }

    void runSamples(String implName, Runner impl) throws Exception {
        var samplesDir = Paths.get("src/test/resources/samples");
        try (Stream<Path> stream = Files.list(samplesDir)) {
            stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.endsWith(".txt"))
                    .map(name -> samplesDir.resolve(name))
                    .forEach(sample -> {
                        var expectedOutput = getExpectedOutput(sample);
                        var resultOutput = run(impl, sample);

                        assertEquals(expectedOutput, resultOutput, implName + " produced wrong output for " + sample);
                    });
        }
    }

    private String run(Runner impl, Path sample) {
        var originalOut = System.out;

        var capture = new ByteArrayOutputStream();
        try {
            var link = Paths.get("./measurements.txt");
            if (Files.exists(link, LinkOption.NOFOLLOW_LINKS)) {
                Files.delete(link);
            }
            Files.createSymbolicLink(link, sample.toAbsolutePath());

            System.setOut(new PrintStream(capture));

            impl.run();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to run", e);
        }
        finally {
            System.setOut(originalOut);
        }
        return capture.toString();
    }

    private String getExpectedOutput(Path sample) {
        try {
            var prefix = trimSuffix(sample.getFileName().toString(), ".txt");
            var out = sample.getParent().resolve(prefix + ".out");

            return new String(Files.readAllBytes(out), "UTF-8");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to red expected output", e);
        }
    }

    private String trimSuffix(String s, String suffix) {
        if (s.endsWith(suffix)) {
            return s.substring(0, s.length() - suffix.length());
        }
        return s;
    }
}
