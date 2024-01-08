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