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

    @Test
    public void testArrayMapGeneral() {
        final var m = new ArrayMap<Character>();
        assertTrue(m.isEmpty());
        assertNull(m.put(1, 'a'));
        assertNull(m.put(2, 'b'));
        assertNull(m.put(3, 'c'));
        assertFalse(m.isEmpty());
        assertEquals(3, m.size());
        assertEquals('c', m.put(3, 'C'));
        assertEquals(3, m.size());
    }

    @Test
    public void testArrayMapInternal() {
        final var m = new ArrayMap<Character>();
        assertNull(m.put(0x10001111, 'a'));
        assertNull(m.put(0x20002222, 'b'));
        assertNull(m.put(0x20003333, 'c'));
        assertNull(m.entries[0]);
        assertNotNull(m.entries[0x1000]);
        assertNotNull(m.entries[0x2000]);
        assertEquals('a', m.entries[0x1000][0x1111]);
        assertEquals('b', m.entries[0x2000][0x2222]);
        assertEquals('c', m.entries[0x2000][0x3333]);
    }
}