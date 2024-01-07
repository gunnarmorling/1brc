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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

public class CalculateAverage_semotpan {

    private static final Path FILE = Path.of("./measurements.txt");

    public static void main(String[] args) throws IOException, InterruptedException {
        var spliterator = new SegmentSpliterator(FILE);
        var summaryStations = IntStream.range(0, spliterator.segments)
                .parallel()
                .mapToObj(segment -> {
                    var buff = spliterator.buffSegment(segment);
                    return new SegmentReader(buff);
                })
                .flatMap(segmentReader -> segmentReader.values().stream())
                .collect(Collectors.groupingByConcurrent(StationSummary::station));

        // Complexity: O(stations * segments)
        var output = new TreeMap<String, StationSummary>();
        summaryStations.forEach((station, summaries) -> {
            var summary = summaries.stream()
                    .reduce(new StationSummary(), (identity, acc) -> {
                        identity.min = Integer.min(identity.min, acc.min);
                        identity.max = Integer.max(identity.max, acc.max);
                        identity.sum += acc.sum;
                        identity.count += acc.count;
                        return identity;
                    });
            output.put(station, summary);
        });

        System.out.println(output);
    }

    private static class StationSummary {

        public String station;
        public int min, max, sum, count;

        StationSummary() {
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
        }

        StationSummary(String station, int temperature) {
            this.station = station;
            this.min = this.max = this.sum = temperature;
            this.count = 1;
        }

        String station() {
            return station;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append(round((double) min * 0.1)).append('/')
                    .append(round((double) sum * 0.1 / count))
                    .append('/').append(round((double) max * 0.1))
                    .toString();
        }
    }

    static class SegmentSpliterator {

        private int segments;
        private final ByteBuffer[] buffSegments;

        SegmentSpliterator(Path file) throws IOException {
            this.segments = Runtime.getRuntime().availableProcessors();
            var fileSize = Files.size(file);
            var segmentSize = fileSize / segments;

            if (segmentSize <= (1 << 8)) { // small segment: 1 is enough
                segments = 1;
            }

            buffSegments = new ByteBuffer[segments];

            var pos = 0L;
            for (var s = 0; s < segments - 1; s++) {
                try (var channel = (FileChannel) Files.newByteChannel(FILE, READ)) {
                    var buff = channel.map(READ_ONLY, pos, segmentSize);
                    pos = normalize(buff, (int) segmentSize - 1, pos);
                    buffSegments[s] = buff;
                }
            }

            // handle last segment
            try (var channel = (FileChannel) Files.newByteChannel(FILE, READ)) {
                var buff = channel.map(READ_ONLY, pos, fileSize - pos);
                buffSegments[segments - 1] = buff;
            }
        }

        private long normalize(ByteBuffer buff, int relativePos, long pos) {
            while (buff.get(relativePos) != '\n') {
                relativePos--;
            }

            buff.limit(relativePos + 1);
            return pos + (relativePos + 1);
        }

        ByteBuffer buffSegment(int index) {
            return buffSegments[index];
        }
    }

    static class SegmentReader {

        private final Map<String, StationSummary> accumulator;
        private final byte[] keyBuff = new byte[256];

        SegmentReader(ByteBuffer byteBuffer) {
            accumulator = new HashMap<>();
            read(byteBuffer);
        }

        private void read(ByteBuffer buff) {
            byte b;
            for (int pos = 0, limit = buff.limit(); buff.hasRemaining(); buff.position(pos)) {
                // parse station (key)
                int offset = 0;
                while ((b = buff.get(pos++)) != ';') {
                    keyBuff[offset++] = b;
                }

                // parse temperature (value) as int (1.1 * 10 = 11)
                int temp = 0;
                int negative = 1;
                while (pos != limit && (b = buff.get(pos++)) != '\n') {
                    switch (b) {
                        case '-':
                            negative = -1;
                        case '.':
                            continue;
                        default:
                            temp = 10 * temp + (b - '0');
                    }
                }
                temp *= negative;

                // compute station
                var station = new String(keyBuff, 0, offset);
                var finalTemp = temp;
                accumulator.compute(station, (key, summary) -> {
                    if (summary == null) {
                        return new StationSummary(station, finalTemp);
                    }
                    summary.min = Integer.min(summary.min, finalTemp);
                    summary.max = Integer.max(summary.max, finalTemp);
                    summary.sum += finalTemp;
                    summary.count += 1;
                    return summary;
                });
            }
        }

        Collection<StationSummary> values() {
            return accumulator.values();
        }
    }
}
