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

//import jdk.incubator.vector.ByteVector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Samuel Yvon's entry.
 * <p>
 * Explanation behind my reasoning:
 * - I want to make it as fast as possible without it being an unreadable mess; I want to avoid bit fiddling UTF-8
 * and use the provided facilities
 * - I use the fact that we know the number of stations to optimize HashMap creation (75% rule)
 * - I stole branch-less compare from royvanrijn
 * - I assume valid ASCII encoding for the number part, which allows me to parse it manually
 * (should hold for valid UTF-8)
 * - I have not done Java in forever. Especially what the heck it's become. I've looked at the other submissions and
 * the given sample to get inspiration. I did not even know about this Stream API thing.
 * </p>
 *
 * <p>
 * Future ideas:
 * - Probably can Vector-Apirize the number parsing (but it's three to four numbers, is it worth?)
 * </p>
 *
 * <p>
 * Observations:
 * - [2024-01-09] The branch-less code from royvarijn does not have a huge impact
 * </p>
 *
 * <p>
 * Changelogs:
 * 2024-01-09: Naive multi-threaded, no floats, manual line parsing
 * </p>
 */
public class CalculateAverage_SamuelYvon {

    private static final String FILE = "./measurements.txt";

    private static final int MAX_STATIONS = 10000;

    private static final byte SEMICOL = 0x3B;

    private static final byte DOT = '.';

    private static final byte MINUS = '-';

    private static final byte ZERO = '0';

    private static final String SLASH_S = "/";

    private static final byte NEWLINE = '\n';

    // The minimum line length in bytes (over-egg.)
    private static final int MIN_LINE_LENGTH_BYTES = 200;

    private static final int DJB2_INIT = 5381;

    /**
     * Branchless min (unprecise for large numbers, but good enough)
     *
     * @author royvanrijn
     */
    private static int branchlessMax(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    /**
     * Branchless min (unprecise for large numbers, but good enough)
     *
     * @author royvanrijn
     */
    private static int branchlessMin(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
    }

    /**
     * A magic key that contains references to the String and where it's located in memory
     */
    private static final class StationName {
        private final int hash;

        private final byte[] value;

        private StationName(int hash, MappedByteBuffer backing, int pos, int len) {
            this.hash = hash;
            this.value = new byte[len];
            backing.get(pos, this.value);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            // Should NEVER be true
            // if (!(obj instanceof StationName)) {
            // return false;
            // }
            StationName other = (StationName) obj;

            if (this.value.length != other.value.length) {
                return false;
            }

            // Byte for byte compare. This actually is a bug! I'm assuming the input
            // is UTF-8 normalized, which in real life would probably not be the case.
            // TODO: SIMD?
            return Arrays.equals(this.value, other.value);
        }

        @Override
        public String toString() {
            return new String(this.value, StandardCharsets.UTF_8);
        }
    }

    private static class StationMeasureAgg {
        private int min;
        private int max;
        private long sum;
        private long count;

        private final StationName station;

        private String memoizedName;

        public StationMeasureAgg(StationName name) {
            // Actual numbers are between -99.9 and 99.9, but we *10 to avoid float
            this.station = name;
            min = 1000;
            max = -1000;
            sum = 0;
            count = 0;
        }

        @Override
        public int hashCode() {
            return this.station.hash;
        }

        /**
         * Get the city name, but also memoized it to avoid building it multiple times
         *
         * @return the city name
         */
        public String city() {
            if (null == this.memoizedName) {
                this.memoizedName = station.toString();
            }
            return this.memoizedName;
        }

        public StationMeasureAgg mergeWith(StationMeasureAgg other) {
            min = branchlessMin(min, other.min);
            max = branchlessMax(max, other.max);

            sum += other.sum;
            count += other.count;

            return this;
        }

        public void accumulate(int number) {
            min = branchlessMin(min, number);
            max = branchlessMax(max, number);

            sum += number;
            count++;
        }

        @Override
        public String toString() {
            double min = Math.round((double) this.min) / 10.0;
            double max = Math.round((double) this.max) / 10.0;
            double mean = Math.round((((double) this.sum / this.count))) / 10.0;
            return min + SLASH_S + mean + SLASH_S + max;
        }

        public StationName station() {
            return this.station;
        }
    }

    private static HashMap<StationName, StationMeasureAgg> parseChunk(MappedByteBuffer chunk) {
        HashMap<StationName, StationMeasureAgg> m = HashMap.newHashMap(MAX_STATIONS);

        int i = 0;
        while (i < chunk.limit()) {
            int j = i;

            int hash = DJB2_INIT;

            // Implement a version of djb2 while we read until the semi, we read anyways
            for (; j < chunk.limit(); ++j) {
                byte b = chunk.get(j);

                if (b == SEMICOL) {
                    break;
                }

                // How will this behave in java? Why do I get no control OF SIGNEDNESS FFS
                // Can I assume int is 32 bits? What is this language! In Java 1.7 it could be 16 bits?
                // Apparently not anymore??
                hash = (((hash << 5) + hash) + b);
            }

            StationName name = new StationName(hash, chunk, i, j - i);

            // Skip the `;`
            j++;

            // Parse the int ourselves, avoids a 'String::replace' to remove the
            // digit.
            int temp = 0;
            boolean neg = chunk.get(j) == MINUS;
            for (j = j + (neg ? 1 : 0); j < chunk.limit(); ++j) {
                temp *= 10;
                byte c = chunk.get(j);
                if (c != DOT) {
                    temp += (char) (c - ZERO);
                }
                else {
                    j++;
                    break;
                }
            }

            // The decimal point
            temp += (char) (chunk.get(j) - ZERO);

            i = j + 1;

            while (chunk.get(i++) != NEWLINE)
                ;

            if (neg)
                temp = -temp;

            m.computeIfAbsent(name, StationMeasureAgg::new).accumulate(temp);
        }

        return m;
    }

    private static int approximateChunks() {
        // https://stackoverflow.com/a/4759606
        // I don't remember Java :D
        return Runtime.getRuntime().availableProcessors();
    }

    private static List<MappedByteBuffer> getFileChunks() throws IOException {
        int approxChunkCount = approximateChunks();

        List<MappedByteBuffer> fileChunks = new ArrayList<>(approxChunkCount * 2);

        // Compute chunks offsets and lengths
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {
            long totalOffset = 0;
            long fLength = file.length();
            long approximateLength = Long.max(fLength / approxChunkCount, MIN_LINE_LENGTH_BYTES);

            while (totalOffset < fLength) {
                long offset = totalOffset;
                int length = (int) approximateLength;

                boolean eof = offset + length >= fLength;
                if (eof) {
                    length = (int) (fLength - offset);
                }

                MappedByteBuffer out = file.getChannel().map(FileChannel.MapMode.READ_ONLY, totalOffset, length);

                while (out.get(length - 1) != NEWLINE) {
                    length--;
                }

                out.position(0);
                out.limit(length);

                fileChunks.add(out);

                totalOffset += length;
            }
        }

        return fileChunks;
    }

    public static void main(String[] args) throws IOException {
        var fileChunks = getFileChunks();

        // Map per core, giving the non-overlapping memory slices
        final Map<StationName, StationMeasureAgg> allMeasures = fileChunks.parallelStream().map(CalculateAverage_SamuelYvon::parseChunk).flatMap(x -> x.values().stream())
                .collect(Collectors.toMap(StationMeasureAgg::station, x -> x, StationMeasureAgg::mergeWith, HashMap::new));

        // Give a capacity that should never be gone past
        StringBuilder sb = new StringBuilder(allMeasures.size() * (100 + 4 + 20));
        sb.append('{');

        allMeasures.values().stream().sorted(Comparator.comparing(StationMeasureAgg::city)).forEach(x -> {
            sb.append(x.city());
            sb.append('=');
            sb.append(x);
            sb.append(", ");
        });

        sb.delete(sb.length() - 2, sb.length()); // delete trailing comma and space

        sb.append('}');

        System.out.println(sb);
    }
}
