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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CalculateAverage_tkowalczTest {

    private final String stringData = """
            İzmir;23.6
            Dushanbe;6.0
            Rostov-on-Don;6.8
            Split;19.6
            Marrakesh;25.3
            Belgrade;15.6
            Kansas City;9.2
            Lubumbashi;19.1
            Colombo;22.3
            Vladivostok;2.6
            Toliara;34.9
            Sydney;9.8
            Brazzaville;24.2
            Milan;15.1
            Petropavlovsk-Kamchatsky;-5.9
            Erbil;11.2
            Bangui;22.9
            Lyon;2.1
            Luanda;32.3
            Jayapura;19.0
            Muscat;34.8
            Napier;14.6
            Baltimore;20.0
            Changsha;18.1
            Alexandria;17.2
            Sana'a;29.3
            Cairo;14.0
            Tamale;34.3
            Hanoi;18.1
            Conakry;32.3
            Antananarivo;23.7
            San Jose;8.8
            Nairobi;21.4
            Toliara;34.9
            """;

    @Test
    void shouldDivideALongNewlinesWithRemainderInLastSlice() {
        // Given
        MemorySegment inputData = MemorySegment.ofArray(stringData.getBytes(StandardCharsets.UTF_8));

        // When
        List<MemorySegment> actual = CalculateAverage_tkowalcz.divideAlongNewlines(inputData, 8);

        // Then
        List<String> actualList = actual.stream().map(
                memorySegment -> new String(memorySegment.toArray(ValueLayout.JAVA_BYTE))).toList();

        assertThat(actualList).containsExactly(
                """
                        İzmir;23.6
                        Dushanbe;6.0
                        Rostov-on-Don;6.8
                        Split;19.6
                        Marrakesh;25.3
                        """,
                """
                        Belgrade;15.6
                        Kansas City;9.2
                        Lubumbashi;19.1
                        Colombo;22.3
                        """,
                """
                        Vladivostok;2.6
                        Toliara;34.9
                        Sydney;9.8
                        Brazzaville;24.2
                        Milan;15.1
                        """,
                """
                        Petropavlovsk-Kamchatsky;-5.9
                        Erbil;11.2
                        Bangui;22.9
                        Lyon;2.1
                        """,
                """
                        Luanda;32.3
                        Jayapura;19.0
                        Muscat;34.8
                        Napier;14.6
                        Baltimore;20.0
                        """,
                """
                        Changsha;18.1
                        Alexandria;17.2
                        Sana'a;29.3
                        Cairo;14.0
                        Tamale;34.3
                        """,
                """
                        Hanoi;18.1
                        Conakry;32.3
                        Antananarivo;23.7
                        San Jose;8.8
                        Nairobi;21.4
                        """,
                """
                        Toliara;34.9
                        """);
    }

    @Test
    void shouldDivideALongNewlinesWithRemainderInLastSlice_NoNewlineAtEndOfFile() {
        // Given
        String localStringData = stringData.substring(0, stringData.length() - 1);
        MemorySegment inputData = MemorySegment.ofArray(localStringData.getBytes(StandardCharsets.UTF_8));

        // When
        List<MemorySegment> actual = CalculateAverage_tkowalcz.divideAlongNewlines(inputData, 8);

        // Then
        List<String> actualList = actual.stream().map(
                memorySegment -> new String(memorySegment.toArray(ValueLayout.JAVA_BYTE))).toList();

        assertThat(actualList).containsExactly(
                """
                        İzmir;23.6
                        Dushanbe;6.0
                        Rostov-on-Don;6.8
                        Split;19.6
                        Marrakesh;25.3
                        """,
                """
                        Belgrade;15.6
                        Kansas City;9.2
                        Lubumbashi;19.1
                        Colombo;22.3
                        """,
                """
                        Vladivostok;2.6
                        Toliara;34.9
                        Sydney;9.8
                        Brazzaville;24.2
                        Milan;15.1
                        """,
                """
                        Petropavlovsk-Kamchatsky;-5.9
                        Erbil;11.2
                        Bangui;22.9
                        Lyon;2.1
                        """,
                """
                        Luanda;32.3
                        Jayapura;19.0
                        Muscat;34.8
                        Napier;14.6
                        Baltimore;20.0
                        """,
                """
                        Changsha;18.1
                        Alexandria;17.2
                        Sana'a;29.3
                        Cairo;14.0
                        Tamale;34.3
                        """,
                """
                        Hanoi;18.1
                        Conakry;32.3
                        Antananarivo;23.7
                        San Jose;8.8
                        Nairobi;21.4
                        """,
                """
                        Toliara;34.9""");
    }

    @Test
    void shouldFindLastNodeInChain() {
        // Given
        CalculateAverage_tkowalcz.StatisticsAggregate node1 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Antananarivo                    ".getBytes(StandardCharsets.UTF_8),
                12);

        CalculateAverage_tkowalcz.StatisticsAggregate node2 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Conakry                         ".getBytes(StandardCharsets.UTF_8),
                7);

        CalculateAverage_tkowalcz.StatisticsAggregate node3 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Napier                          ".getBytes(StandardCharsets.UTF_8),
                6);

        node1.attachLast(node2).attachLast(node3);

        ByteVector hashInput = ByteVector.fromArray(ByteVector.SPECIES_256, node3.getCity(), 0);
        VectorMask<Byte> hashMask = CalculateAverage_tkowalcz.CITY_LOOKUP_MASK[6];

        // When
        CalculateAverage_tkowalcz.StatisticsAggregate actual = CalculateAverage_tkowalcz.findCityInChain(node1, hashInput, hashMask);

        // Then
        assertThat(actual).isSameAs(node3);
    }

    @Test
    void shouldFindNodeInTheMiddleOfTheChain() {
        // Given
        CalculateAverage_tkowalcz.StatisticsAggregate node1 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Antananarivo                    ".getBytes(StandardCharsets.UTF_8),
                12);

        CalculateAverage_tkowalcz.StatisticsAggregate node2 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Conakry                         ".getBytes(StandardCharsets.UTF_8),
                7);

        CalculateAverage_tkowalcz.StatisticsAggregate node3 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Napier                          ".getBytes(StandardCharsets.UTF_8),
                6);

        node1.attachLast(node2).attachLast(node3);

        ByteVector hashInput = ByteVector.fromArray(ByteVector.SPECIES_256, node2.getCity(), 0);
        VectorMask<Byte> hashMask = CalculateAverage_tkowalcz.CITY_LOOKUP_MASK[7];

        // When
        CalculateAverage_tkowalcz.StatisticsAggregate actual = CalculateAverage_tkowalcz.findCityInChain(node1, hashInput, hashMask);

        // Then
        assertThat(actual).isSameAs(node2);
    }

    @Test
    void shouldCreateNewNodeIfNotFound() {
        // Given
        CalculateAverage_tkowalcz.StatisticsAggregate node1 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Antananarivo                    ".getBytes(StandardCharsets.UTF_8),
                12);

        CalculateAverage_tkowalcz.StatisticsAggregate node2 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Conakry                         ".getBytes(StandardCharsets.UTF_8),
                7);

        CalculateAverage_tkowalcz.StatisticsAggregate node3 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                "Napier                          ".getBytes(StandardCharsets.UTF_8),
                6);

        node1.attachLast(node2).attachLast(node3);

        byte[] newCity = "Kansas City                          ".getBytes(StandardCharsets.UTF_8);

        ByteVector hashInput = ByteVector.fromArray(ByteVector.SPECIES_256, newCity, 0);
        VectorMask<Byte> hashMask = CalculateAverage_tkowalcz.CITY_LOOKUP_MASK[11];

        // When
        CalculateAverage_tkowalcz.StatisticsAggregate actual = CalculateAverage_tkowalcz.findCityInChain(node1, hashInput, hashMask);

        // Then
        assertThat(actual)
                .isNotSameAs(node1)
                .isNotSameAs(node2)
                .isNotSameAs(node3);

        assertThat(actual.cityAsString()).isEqualTo("Kansas City");
        assertThat(node3.getNext()).isSameAs(actual);
    }
}
