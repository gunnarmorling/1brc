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


import java.io.*;
import java.nio.MappedByteBuffer;
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
    private static final String FILE = "./measurements.txt";

    private static final int HASH_NO_CLASH_MODULUS = 49999;
    private static final int OFFSET_MIN = 0;
    private static final int OFFSET_MAX = 1;
    private static final int OFFSET_COUNT = 2;

    @SuppressWarnings("RedundantSuppression")
    public static void main(String[] args) throws IOException {
        var path = Path.of(FILE);
        var start = Instant.now();
        var desiredSegmentsCount = Runtime.getRuntime().availableProcessors();

        var segments = FileSegment.forFile(path, desiredSegmentsCount);

        var trackers = segments.stream().parallel().map(segment -> {
            try (var fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
                var tracker = new Tracker();
                var segmentBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segment.size());
                tracker.processSegment(segmentBuffer, segment.end());
                return tracker;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        var result = summarizeTrackers(trackers);
        System.out.println(result);

        //noinspection ConstantValue
        if (start!=null) System.err.println(Duration.between(start, Instant.now()));
        assert Files.readAllLines(Path.of("expected_result.txt")).getFirst().equals(result);
    }


    record FileSegment(long start, long end) {
        public long size() { return end() - start(); }

        public static List<FileSegment> forFile(Path file, int desiredSegmentsCount) throws IOException {
            try (var raf = new RandomAccessFile(file.toFile(), "r")) {
                List<FileSegment> segments = new ArrayList<>();
                long fileSize = raf.length();
                long segmentSize = fileSize / desiredSegmentsCount;
                for (int segmentIdx = 0; segmentIdx < desiredSegmentsCount; segmentIdx++) {
                    long segStart = segmentIdx * segmentSize;
                    long segEnd = (segmentIdx == desiredSegmentsCount - 1) ? fileSize : segStart + segmentSize;
                    segStart = findSegmentBoundary(raf, segmentIdx, 0, segStart, segEnd);
                    segEnd = findSegmentBoundary(raf, segmentIdx, desiredSegmentsCount - 1, segEnd, fileSize);

                    segments.add(new FileSegment(segStart, segEnd));
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
        for (int i = 0; i < HASH_NO_CLASH_MODULUS; i++) {
            String name = null;

            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            long sum = 0;
            long count = 0;
            for (Tracker tracker : trackers) {
                if (tracker.names[i]==null) continue;
                if (name==null) name = tracker.names[i];

                var minn = tracker.minMaxCount[i*3];
                var maxx = tracker.minMaxCount[i*3+1];
                if (minn<min) min = minn;
                if (maxx>max) max = maxx;
                count += tracker.minMaxCount[i*3+2];
                sum += tracker.sums[i];
            }
            if (name==null) continue;

            var mean = Math.round((double) sum / count) / 10.0;
            result.put(name, (min/10.0) + "/" + mean + "/" + (max/10.0));
        }
        return result.toString();
    }

    static class Tracker {
        private final int[] minMaxCount = new int[HASH_NO_CLASH_MODULUS * 3];
        private final long[] sums = new long[HASH_NO_CLASH_MODULUS];
        private final String[] names = new String[HASH_NO_CLASH_MODULUS];
        private final byte[] nameThreadLocal = new byte[64];

        private void processSegment(MappedByteBuffer segmentBuffer, long segmentEnd) {
            int startLine;
            int limit = segmentBuffer.limit();
            while ((startLine = segmentBuffer.position()) < limit) {
                int pos = startLine;
                byte b;

                int nameLength = 0, nameHash = 0;
                while (pos != segmentEnd && (b = segmentBuffer.get(pos++)) != ';') {
                    nameHash = nameHash*31 + b;
                    nameLength++;
                }

                int temperature = 0, sign = 1;
                outer:
                while (pos != segmentEnd && (b = segmentBuffer.get(pos++)) != '\n') {
                    switch (b) {
                        case '\r' :
                            pos++;
                            break outer;
                        case '.'  :
                            break;
                        case '-'  :
                            sign = -1;
                            break;
                        default   :
                            var digit = b - '0';
                            assert digit >= 0 && digit <= 9;
                            temperature = 10 * temperature + digit;
                    }
                }

                processLine(nameHash, segmentBuffer, startLine, nameLength, temperature * sign);
                segmentBuffer.position(pos);
            }
        }

        public void processLine(int nameHash, MappedByteBuffer buffer, int nameOffset, int nameLength, int temperature) {
            var i = Math.abs(nameHash) % HASH_NO_CLASH_MODULUS;

            if (names[i]==null) {
                names[i] = parseName(buffer, nameOffset, nameLength);
            } else {
                assert parseName(buffer, nameOffset, nameLength).equals(names[i]) : parseName(buffer, nameOffset, nameLength) + "!=" + names[i];
            }

            sums[i] += temperature;

            int mmcIndex = i * 3;
            var min = minMaxCount[mmcIndex + OFFSET_MIN];
            var max = minMaxCount[mmcIndex + OFFSET_MAX];
            if (temperature < min) minMaxCount[mmcIndex + OFFSET_MIN] = temperature;
            if (temperature > max) minMaxCount[mmcIndex + OFFSET_MAX] = temperature;

            minMaxCount[mmcIndex + OFFSET_COUNT]++;
        }

        private String parseName(MappedByteBuffer buffer, int nameOffset, int nameLength) {
            buffer.get(nameOffset, nameThreadLocal, 0, nameLength);
            return new String(nameThreadLocal, 0, nameLength, StandardCharsets.UTF_8);
        }
    }
}
