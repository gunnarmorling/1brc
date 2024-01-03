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

import static org.assertj.core.api.Assertions.assertThat;

import dev.morling.onebrc.CalculateAverage_michaljonko.ParsingCache;
import java.util.stream.Stream;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ParsingCacheTest {

    private final ParsingCache cache = new ParsingCache();

    private static Stream<Arguments> params() {
        return Stream.of(
                Arguments.of("10.1", 10.1d),
                Arguments.of("-10.1", -10.1d),
                Arguments.of("123.456", 123.456d),
                Arguments.of("-123.456", -123.456d));
    }

    @ParameterizedTest
    @MethodSource("params")
    void parsing(String raw, double expected) {
        assertThat(cache.parseIfAbsent(raw))
                .isCloseTo(expected, Offset.offset(0.0001d));
    }
}