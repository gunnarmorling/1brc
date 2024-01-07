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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Testing efficiency of various hashes
 */
public class HashCheck {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_256;

    private static final ByteVector HASH_COEFFICIENTS_1B = ByteVector.fromArray(SPECIES,
            new byte[]{ 7, 11, 13, 17, 19, 23, 29, 31,
                    37, 41, 43, 47, 53, 59, 61, 67,
                    71, 73, 79, 83, 89, 97, 101, 103,
                    107, 109, 113, 127, (byte) 131, (byte) 137, (byte) 139, (byte) 149 },
            0);

    static final ShortVector HASH_COEFFICIENTS_1 = ShortVector.fromArray(SHORT_SPECIES,
            new short[]{ 13, 19, 29, 37, 43, 53, 61, 71, 79, 89, 101, 107, 113, 131, 139, 151 },
            0);

    static final ShortVector HASH_COEFFICIENTS_2 = ShortVector.fromArray(SHORT_SPECIES,
            new short[]{ 17, 23, 31, 41, 47, 59, 67, 73, 83, 97, 103, 109, 127, 137, 149, 157 },
            0);

    private static final VectorMask<Byte> EXPAND_TO_SHORT_MASK_1 = VectorMask.fromLong(SPECIES, 0x55_55_55_55_55_55_55_55L);
    private static final VectorMask<Byte> EXPAND_TO_SHORT_MASK_2 = EXPAND_TO_SHORT_MASK_1.not();

    static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromValues(
            SPECIES,
            1, 0, 3, 2, 5, 4, 7, 6,
            9, 8, 11, 10, 13, 12, 15, 14,
            17, 16, 19, 18, 21, 20, 23, 22,
            25, 24, 27, 26, 29, 28, 31, 30);

    public static void main(String[] args) throws IOException {
        Map<Integer, List<CityHashPair>> collect = Files.lines(Path.of(FILE))
                .map(line -> line.split(";")[0])
                .map(HashCheck::hash3)
                .distinct()
                .collect(Collectors.groupingBy(CityHashPair::hash));

        int[] histogramBuckets = new int[40];
        collect.forEach((ignored, pair) -> {
            int collisions = pair.size();
            histogramBuckets[collisions]++;
        });

        System.out.println(Arrays.toString(histogramBuckets));

        collect.values().stream()
                .filter(list -> list.size() > 1)
                .forEach(System.out::println);
    }

    private static CityHashPair hash(String city) {
        byte[] name = Arrays.copyOf(city.getBytes(StandardCharsets.UTF_8), SPECIES.length());
        ByteVector byteVector = ByteVector.fromArray(SPECIES, name, 0);

        ShortVector s1 = byteVector.blend(ZERO, EXPAND_TO_SHORT_MASK_1).rearrange(SHUFFLE).reinterpretAsShorts();
        ShortVector s2 = byteVector.blend(ZERO, EXPAND_TO_SHORT_MASK_2).reinterpretAsShorts();

        long hash = s1.mul(HASH_COEFFICIENTS_1).reduceLanesToLong(VectorOperators.ADD) +
                s2.mul(HASH_COEFFICIENTS_2).reduceLanesToLong(VectorOperators.ADD);

        return new CityHashPair(city, Math.abs((int) ((hash >> 32) ^ hash) % 100000), hash);
    }

    private static CityHashPair hash2(String city) {
        byte[] name = Arrays.copyOf(city.getBytes(StandardCharsets.UTF_8), SPECIES.length());
        ByteVector byteVector = ByteVector.fromArray(SPECIES, name, 0);

        ShortVector s1 = byteVector.reinterpretAsShorts();
        ShortVector s2 = byteVector.rearrange(SHUFFLE).reinterpretAsShorts();

        long hash = s1.mul(HASH_COEFFICIENTS_1).reduceLanesToLong(VectorOperators.ADD) +
                s2.mul(HASH_COEFFICIENTS_2).reduceLanesToLong(VectorOperators.ADD);

        return new CityHashPair(city, Math.abs((int) ((hash >> 32) ^ hash) % 100000), hash);
    }

    private static CityHashPair hash3(String city) {
        byte[] name = Arrays.copyOf(city.getBytes(StandardCharsets.UTF_8), SPECIES.length());
        ByteVector byteVector = ByteVector.fromArray(SPECIES, name, 0);

        long hash = byteVector.reinterpretAsInts().reduceLanesToLong(VectorOperators.ADD);

        int h1 = (int) (hash ^ (hash >>> 32));
        short s1 = (short) (h1 ^ (h1 >>> 16));
        return new CityHashPair(city, s1 & (0x400000 - 1), hash);
    }

}

record CityHashPair(String city, int hash, long orignal) {
}
