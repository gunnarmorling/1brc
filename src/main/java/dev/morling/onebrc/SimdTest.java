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
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.TreeMap;

public class SimdTest {
    private static final VectorSpecies<Byte> SPECIES = VectorSpecies.ofPreferred(byte.class);
    private static final double[] DOUBLES = new double[]{ 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };
    private static final int[] DIGIT_LOOKUP = new int[]{
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, 0, 1,
            2, 3, 4, 5, 6, 7, 8, 9, -1, -1 };

    public static void main(String[] args) throws Exception {
        String method = System.getProperty("testMethod", "vectorized");
        try (RandomAccessFile file = new RandomAccessFile("./measurements.txt", "r")) {
            MemorySegment memorySegment = file.getChannel().map(
                    FileChannel.MapMode.READ_ONLY, 0, 872_415_232, Arena.global());
            memorySegment.load();
            ByteBuffer bb = memorySegment.asByteBuffer();
            byte[] page = new byte[bb.capacity()];
            bb.get(page);
            for (int i = 0; i < 30; i++) {
                CalculateAverage_godofwharf_all.FastHashMap fastHashMap = new CalculateAverage_godofwharf_all.FastHashMap(10010);
                long time = System.nanoTime();
                if (method.equals("vectorized")) {
                    SearchResult searchResult = findNewLinesVectorized(page, page.length);
                    int j = 0;
                    int prevOffset = 0;
                    while (j < searchResult.len) {
                        int curOffset = searchResult.offsets[j];
                        if (curOffset <= 6) {
                            throw new IllegalStateException("Error in currOffset. Value = %d".formatted(curOffset));
                        }
                        byte ch1 = page[curOffset - 4];
                        byte ch2 = page[curOffset - 5];
                        int temperatureLen = 5;
                        if (ch1 == ';') {
                            temperatureLen = 3;
                        }
                        else if (ch2 == ';') {
                            temperatureLen = 4;
                        }
                        byte[] temperature = new byte[5];
                        byte[] station = new byte[100];

                        // ex: abc;1.3\ndefghi;22.4\n
                        if (page[curOffset] != '\n') {
                            throw new IllegalStateException("curOffset is pointing to %d but this offset doesn't contain NL char. Instead it contains %s"
                                    .formatted(curOffset, page[curOffset]));
                        }
                        int lineLength = curOffset - prevOffset;
                        int stationLen = lineLength - temperatureLen - 1;
                        System.arraycopy(page, curOffset - temperatureLen, temperature, 0, temperatureLen);
                        System.arraycopy(page, prevOffset, station, 0, stationLen);
                        int hashCode = Arrays.hashCode(station);
                        CalculateAverage_godofwharf_all.Measurement m = new CalculateAverage_godofwharf_all.Measurement(
                                station, stationLen, temperature, temperatureLen, false, hashCode, -1);
                        fastHashMap.put(m.stateKey(), new CalculateAverage_godofwharf_all.MeasurementAggregator(
                                m.value(),
                                m.value(),
                                m.value(),
                                1));
                        prevOffset = curOffset + 1;
                        j++;
                    }
                    TreeMap<String, CalculateAverage_godofwharf_all.MeasurementAggregator> sortedMap = new TreeMap<>();
                    fastHashMap.forEach((k, v) -> sortedMap.put(k.toString(), v));
                    // System.out.println(sortedMap);
                    System.out.printf("Vectorized loop took %d ns. Result size = %d%n", System.nanoTime() - time, sortedMap.size());
                }
                else {
                    SearchResult searchResult = findNewLines(page, page.length);
                    System.out.printf("Normal loop took %d ns. Result size = %d%n", System.nanoTime() - time, searchResult.len);
                }
            }
        }
    }

    private static SearchResult findNewLinesVectorized(final byte[] page,
                                                       final int pageLen) {
        SearchResult ret = new SearchResult(new int[pageLen / 10], 0);
        int loopLength = SPECIES.length();
        int loopBound = SPECIES.loopBound(pageLen);
        int i = 0;
        int j = 0;
        Vector<Byte> newLineVec = SPECIES.broadcast('\n');
        int[] positions = new int[64];
        while (j < loopBound) {
            Vector<Byte> vec = ByteVector.fromArray(SPECIES, page, j);
            long res = vec.eq(newLineVec).toLong();
            int k = 0;
            int bitCount = Long.bitCount(res);
            while (res > 0) {
                int idx = Long.numberOfTrailingZeros(res);
                positions[k++] = j + idx;
                res &= (res - 1);
                idx = Long.numberOfTrailingZeros(res);
                positions[k++] = j + idx;
                res &= (res - 1);
                idx = Long.numberOfTrailingZeros(res);
                positions[k++] = j + idx;
                res &= (res - 1);
                idx = Long.numberOfTrailingZeros(res);
                positions[k++] = j + idx;
                res &= (res - 1);
            }
            System.arraycopy(positions, 0, ret.offsets, i, bitCount);
            j += loopLength;
            i += bitCount;
        }

        // tail loop
        while (j < pageLen) {
            byte b = page[j];
            if (b == '\n') {
                ret.offsets[i++] = j;
            }
            j++;
        }
        ret.len = i;
        return ret;
    }

    private static SearchResult findNewLines(final byte[] page,
                                             final int pageLen) {
        SearchResult ret = new SearchResult(new int[pageLen / 10], 0);
        int i = 0;
        int k = 0;
        for (; i < pageLen; i++) {
            byte b = page[i];
            if (b == '\n') {
                ret.offsets[k++] = i;
            }
        }
        ret.len = k;
        return ret;
    }

    public static class SearchResult {
        private int[] offsets;
        private int len;

        public SearchResult(final int[] offsets,
                            final int len) {
            this.offsets = offsets;
            this.len = len;
        }
    }

    private static double parseDouble2(final byte[] b, final int len) {
        try {
            char ch0 = (char) b[0];
            char ch1 = (char) b[1];
            char ch2 = (char) b[2];
            char ch3 = len > 3 ? (char) b[3] : ' ';
            char ch4 = len > 4 ? (char) b[4] : ' ';
            if (len == 3) {
                int decimal = toDigit(ch0);
                double fractional = DOUBLES[toDigit(ch2)];
                return decimal + fractional;
            }
            else if (len == 4) {
                // -1.2 or 11.2
                int decimal = (ch0 == '-' ? toDigit(ch1) : (fastMul10(toDigit(ch0)) + toDigit(ch1)));
                double fractional = DOUBLES[toDigit(ch3)];
                if (ch0 == '-') {
                    return Math.negateExact(decimal) - fractional;
                }
                else {
                    return decimal + fractional;
                }
            }
            else {
                int decimal = fastMul10(toDigit(ch1)) + toDigit(ch2);
                double fractional = DOUBLES[toDigit(ch4)];
                return Math.negateExact(decimal) - fractional;
            }
        }
        catch (ArrayIndexOutOfBoundsException e) {
            System.out.printf("Array index out of bounds for string: %s%n", new String(b, 0, len));
            throw new RuntimeException(e);
        }
        catch (StringIndexOutOfBoundsException e) {
            System.out.printf("String index out of bounds for string: %s%n", new String(b, 0, len));
            throw new RuntimeException(e);
        }
    }

    private static int fastMul10(final int i) {
        return (i << 1) + (i << 3);
    }

    private static int toDigit(final char c) {
        return DIGIT_LOOKUP[c];
    }

}
