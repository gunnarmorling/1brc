package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

class IntegerParsingTest {

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

    @Test
    void shouldParseNegativeNumberWithTwoIntegralDigits() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "-12.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(-123L);
    }

    @Test
    void shouldParsePositiveNumberWithTwoIntegralDigits() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "12.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(123L);
    }

    @Test
    void shouldParseNegativeNumberWithOneIntegralDigit() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "-2.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(-23L);
    }

    @Test
    void shouldParsePositiveNumberWithOneIntegralDigit() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "2.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(23L);
    }

    static Stream<Arguments> measurements() {
        return Stream.of(
                of("34.9\nSydney;34.9\nSydney;9.8\nBrazzaville;24.2", 349),
                of("9.8\nBrazzaville;34.9\nSydney;9.8\nBrazzaville;24.2", 98),
                of("24.2\nMilan;34.9\nSydney;9.8\nBrazzaville;24.2", 242),
                of("15.1\nPetropavlovsk-Kamchatsky;34.9\nSydney;9.8\nBrazzaville;24.2", 151),
                of("-5.9\nErbil;34.9\nSydney;9.8\nBrazzaville;24.2", -59),
                of("11.2\nBangui;34.9\nSydney;9.8\nBrazzaville;24.2", 112),
                of("22.9\nLyon;34.9\nSydney;9.8\nBrazzaville;24.2", 229),
                of("2.1\nLuanda;34.9\nSydney;9.8\nBrazzaville;24.2", 21),
                of("32.3\nJayapura;34.9\nSydney;9.8\nBrazzaville;24.2", 323));
    }

    @ParameterizedTest
    @MethodSource("measurements")
    void shouldParseMany(String input, long expected) {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                input.getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(expected);
    }
}
