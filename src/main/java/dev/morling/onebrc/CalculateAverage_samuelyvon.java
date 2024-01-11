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
public class CalculateAverage_samuelyvon {

    private static final String FILE = "./measurements.txt";

    private static final int MAX_STATIONS = 10000;

    private static final byte SEMICOL = 0x3B;

    private static final byte MINUS = '-';

    private static final byte ZERO = '0';

    private static final byte NEWLINE = '\n';

    // The minimum line length in bytes (over-egg.)
    private static final int MIN_LINE_LENGTH_BYTES = 200;

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

    private static class StationMeasureAgg {
        private int min;
        private int max;
        private long sum;
        private long count;

        private final String city;

        public StationMeasureAgg(String city) {
            // Actual numbers are between -99.9 and 99.9, but we *10 to avoid float
            this.city = city;
            min = 1000;
            max = -1000;
            sum = 0;
            count = 0;
        }

        public String city() {
            return this.city;
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
            return min + "/" + mean + "/" + max;
        }
    }

    private static HashMap<String, StationMeasureAgg> parseChunk(MappedByteBuffer chunk) {
        HashMap<String, StationMeasureAgg> m = HashMap.newHashMap(MAX_STATIONS);

        int i = 0;
        while (i < chunk.limit()) {
            int j = i;
            for (; j < chunk.limit(); ++j) {
                // TODO: Could compute a hash here, store a byte array, and decode UTF-8 at the
                // latest moment.
                if (chunk.get(j) == SEMICOL) {
                    break;
                }
            }

            byte[] backingNameArray = new byte[j - i];
            chunk.get(i, backingNameArray);
            String name = new String(backingNameArray, StandardCharsets.UTF_8);

            // Skip the `;`
            j++;

            // Parse the int ourselves, avoids a 'String::replace' to remove the
            // digit.
            int temp = 0;
            boolean neg = chunk.get(j) == MINUS;
            for (j = j + (neg ? 1 : 0); j < chunk.limit(); ++j) {
                temp *= 10;
                byte c = chunk.get(j);
                if (c != '.') {
                    temp += (char) (c - ZERO);
                } else {
                    j++;
                    break;
                }
            }

            // The decimal point
            temp += (char) (chunk.get(j) - ZERO);

            i = j + 1;

            while (chunk.get(i++) != '\n')
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
        final Map<String, StationMeasureAgg> sortedMeasures = fileChunks.parallelStream().map(CalculateAverage_samuelyvon::parseChunk)
                .flatMap(x -> x.values().stream()).collect(Collectors.toMap(StationMeasureAgg::city, x -> x, StationMeasureAgg::mergeWith, TreeMap::new));

        System.out.println(sortedMeasures);
    }
}
