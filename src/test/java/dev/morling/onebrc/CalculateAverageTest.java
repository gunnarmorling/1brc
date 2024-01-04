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

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.platform.commons.util.ReflectionUtils;
import org.radughiorma.ConsoleOutputRecorder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CalculateAverageTest {
    /**
     * This variable holds the default arguments for different classes.
     * It is a map where the key is the class and the value is an array of strings representing the default arguments.
     * You only need to provide a mapping here if your class requires additional arguments beside the measurements file
     *
     */
    private static Map<Class, String[]> DEFAULT_ARGS = new HashMap<>() {
        {
            put(CalculateAverage_ebarlas.class, new String[]{ "16" });
        }
    };
    @RegisterExtension
    ConsoleOutputRecorder consoleOutputRecorder = new ConsoleOutputRecorder();

    @ParameterizedTest
    @MethodSource("contendantsProvider")
    void results(Class calculateAverateClass, Object args, String expectedResultPath) throws NoSuchMethodException, IOException {
        ReflectionUtils.invokeMethod(calculateAverateClass.getMethod("main", String[].class), null, args);

        String expectedResult = Files.readString(Paths.get(expectedResultPath), StandardCharsets.UTF_8);
        assertThat(consoleOutputRecorder.getCapturedOutput()).containsIgnoringWhitespaces(expectedResult);

    }

    static Stream<Arguments> contendantsProvider() {
        List<Class<?>> classes = ReflectionSupport.findAllClassesInPackage(
                "dev.morling.onebrc",
                aClass -> !aClass.isMemberClass() && !aClass.isAnonymousClass() && !aClass.isLocalClass(),
                aClassName -> "dev.morling.onebrc.CalculateAverage".equals(aClassName) || aClassName.startsWith("dev.morling.onebrc.CalculateAverage_")
        );
        Stream<Arguments> calculateAverage = Stream.of(1, 2, 10)
                .flatMap(sampleSize -> classes
                        .stream()
                        .map(aClass -> {
                            Object[] arguments = Stream.concat(
                                            Stream.of(STR."target/test-classes/samples/measurements-\{sampleSize}.txt"),
                                            Arrays.stream(DEFAULT_ARGS.getOrDefault(aClass, new String[]{})))
                                    .toArray(String[]::new);
                            return Arguments.of(
                                    aClass,
                                    arguments,
                                    STR."target/test-classes/samples/measurements-\{sampleSize}.out");
                        })
                );
        return calculateAverage;
    }

}