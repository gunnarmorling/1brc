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

import org.radughiorma.Arguments;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

// gunnar morling - 2:10
// roy van rijn -   1:01
//                  0:37

public class CalculateAverage_ddimtirov {
    private static final int HASH_NO_CLASH_MODULUS = 49999;
    private static final int OFFSET_MIN = 0;
    private static final int OFFSET_MAX = 1;
    private static final int OFFSET_COUNT = 2;

    @SuppressWarnings("RedundantSuppression")
    public static void main(String[] args) throws IOException {
        var path = Arguments.measurmentsPath(args);
        var start = Instant.now();
        var desiredSegmentsCount = Runtime.getRuntime().availableProcessors();

        var fileSegments = FileSegment.forFile(path, desiredSegmentsCount);

        var trackers = fileSegments.stream().parallel().map(fileSegment -> {
            try (var fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
                var tracker = new Tracker();
                var memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, fileSegment.start(), fileSegment.size(), Arena.ofConfined());
                tracker.processSegment(memorySegment);
                return tracker;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        var result = summarizeTrackers(trackers);
        System.out.println(result);

        // noinspection ConstantValue
        if (start != null)
            System.err.println(Duration.between(start, Instant.now()));
        // assert Files.readAllLines(Path.of("measurements_result.txt")).getFirst().equals(result);
    }

    record FileSegment(long start, long size) {
        public static List<FileSegment> forFile(Path file, int desiredSegmentsCount) throws IOException {
            try (var raf = new RandomAccessFile(file.toFile(), "r")) {
                var segments = new ArrayList<FileSegment>();
                var fileSize = raf.length();
                var segmentSize = fileSize / desiredSegmentsCount;
                for (int segmentIdx = 0; segmentIdx < desiredSegmentsCount; segmentIdx++) {
                    var segStart = segmentIdx * segmentSize;
                    var segEnd = (segmentIdx == desiredSegmentsCount - 1) ? fileSize : segStart + segmentSize;
                    segStart = findSegmentBoundary(raf, segmentIdx, 0, segStart, segEnd);
                    segEnd = findSegmentBoundary(raf, segmentIdx, desiredSegmentsCount - 1, segEnd, fileSize);

                    var segSize = segEnd - segStart;

                    segments.add(new FileSegment(segStart, segSize));
                }
                return segments;
            }
        }

        private static long findSegmentBoundary(RandomAccessFile raf, int i, int skipForSegment, long location, long fileSize) throws IOException {
            if (i == skipForSegment) return location;

            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n') break;
            }
            return location;
        }
    }

    private static String summarizeTrackers(List<Tracker> trackers) {
        var result = new TreeMap<String, String>();
        for (var i = 0; i < HASH_NO_CLASH_MODULUS; i++) {
            String name = null;

            var min = Integer.MAX_VALUE;
            var max = Integer.MIN_VALUE;
            var sum = 0L;
            var count = 0L;
            for (Tracker tracker : trackers) {
                if (tracker.names[i] == null)
                    continue;
                if (name == null)
                    name = tracker.names[i];

                var minn = tracker.minMaxCount[i * 3];
                var maxx = tracker.minMaxCount[i * 3 + 1];
                if (minn < min)
                    min = minn;
                if (maxx > max)
                    max = maxx;
                count += tracker.minMaxCount[i * 3 + 2];
                sum += tracker.sums[i];
            }
            if (name == null)
                continue;

            var mean = Math.round((double) sum / count) / 10.0;
            result.put(name, (min / 10.0) + "/" + mean + "/" + (max / 10.0));
        }
        return result.toString();
    }

    static class Tracker {
        private final int[] minMaxCount = new int[HASH_NO_CLASH_MODULUS * 3];
        private final long[] sums = new long[HASH_NO_CLASH_MODULUS];
        private final String[] names = new String[HASH_NO_CLASH_MODULUS];

        private void processSegment(MemorySegment memory) {
            int position = 0;
            long limit = memory.byteSize();
            while (position < limit) {
                int pos = position;
                byte b;

                int nameLength = 0, nameHash = 0;
                while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
                    nameHash = nameHash * 31 + b;
                    nameLength++;
                }

                int temperature = 0, sign = 1;
                outer: while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != '\n') {
                    switch (b) {
                        case '\r':
                            pos++;
                            break outer;
                        case '.':
                            break;
                        case '-':
                            sign = -1;
                            break;
                        default:
                            var digit = b - '0';
                            assert digit >= 0 && digit <= 9;
                            temperature = 10 * temperature + digit;
                    }
                }

                processLine(nameHash, memory, position, nameLength, temperature * sign);
                position = pos;
            }
        }

        public void processLine(int nameHash, MemorySegment buffer, int nameOffset, int nameLength, int temperature) {
            var i = Math.abs(nameHash) % HASH_NO_CLASH_MODULUS;

            if (names[i] == null) {
                names[i] = parseName(buffer, nameOffset, nameLength);
            }
            else {
                assert parseName(buffer, nameOffset, nameLength).equals(names[i]) : parseName(buffer, nameOffset, nameLength) + "!=" + names[i];
            }

            sums[i] += temperature;

            int mmcIndex = i * 3;
            var min = minMaxCount[mmcIndex + OFFSET_MIN];
            var max = minMaxCount[mmcIndex + OFFSET_MAX];
            if (temperature < min)
                minMaxCount[mmcIndex + OFFSET_MIN] = temperature;
            if (temperature > max)
                minMaxCount[mmcIndex + OFFSET_MAX] = temperature;

            minMaxCount[mmcIndex + OFFSET_COUNT]++;
        }

        private String parseName(MemorySegment memory, int nameOffset, int nameLength) {
            byte[] array = memory.asSlice(nameOffset, nameLength).toArray(ValueLayout.JAVA_BYTE);
            return new String(array, StandardCharsets.UTF_8);
        }
    }
}
