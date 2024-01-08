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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class CalculateElsteveograndeTest {
    @Test
    public void testParseFloatDot1() {
        assertEquals(
                1.0f,
                Util.parseFloatDot1("1.0".getBytes(StandardCharsets.UTF_8)));
        assertEquals(
                31.0f,
                Util.parseFloatDot1("31.0".getBytes(StandardCharsets.UTF_8)));
        assertEquals(
                31.4f,
                Util.parseFloatDot1("31.4".getBytes(StandardCharsets.UTF_8)));
        assertEquals(
                -31.4f,
                Util.parseFloatDot1("-31.4".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testStationStats() {
        final var s = new Station("Foo");

        s.update(0.0f);
        assertEquals("0.0/0.0/0.0", s.toString());

        s.update(-10.0f);
        assertEquals("-10.0/-5.0/0.0", s.toString());

        s.update(30.0f);
        assertEquals("-10.0/6.7/30.0", s.toString()); // check rounding (6.666...)
    }
}