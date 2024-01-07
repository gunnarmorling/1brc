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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class CalculateAverage_ddimtirov {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_STATIONS = 100_000;
    private static final int MAX_STATION_NAME_LENGTH = 100;
    private static final int MAX_UTF8_CODEPOINT_SIZE = 3;

    private static final int OFFSET_MIN = 0;
    private static final int OFFSET_MAX = 1;
    private static final int OFFSET_COUNT = 2;

    private static final boolean assertions = CalculateAverage_ddimtirov.class.desiredAssertionStatus();
    private static final Map<String, LongAdder> hashCollisionOccurrences = new ConcurrentHashMap<>();

    @SuppressWarnings("RedundantSuppression")
    public static void main(String[] args) throws IOException, InterruptedException {
        var path = Path.of(args.length>0 ? args[0] : FILE);
        Instant start = null;// Instant.now();

        var desiredSegmentsCount = Runtime.getRuntime().availableProcessors();
        var fileSegments = FileSegment.forFile(path, desiredSegmentsCount);

        var loaders = new ThreadGroup("Loaders");
        var trackers = Collections.synchronizedList(new ArrayList<Tracker>());
        var threads = fileSegments.stream().map(fileSegment -> Thread // manually start thread per segment
                .ofPlatform()
                .group(loaders)
                .name(STR."Segment \{fileSegment}")
                .start(() -> {
                    try (var fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
                        var tracker = new Tracker();
                        var memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, fileSegment.start(), fileSegment.size(), Arena.ofConfined());
                        tracker.processSegment(memorySegment);
                        trackers.add(tracker);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
        ).toList();

        for (Thread thread : threads) thread.join();
        assert trackers.size() == threads.size();
        assert trackers.size() <= desiredSegmentsCount;

        var result = summarizeTrackers(trackers.toArray(Tracker[]::new));
        System.out.println(result);

        // noinspection ConstantValue
        if (start != null) {
            System.err.println(Duration.between(start, Instant.now()));
            if (assertions) System.err.printf("hash clashes: %s%n", hashCollisionOccurrences);
        }
        assert Files.readAllLines(Path.of("measurements.out")).getFirst().equals(result);
    }

    record FileSegment(int index, long start, long size) {
        @Override
        public String toString() {
            return STR."#\{index} [\{start}..\{start + size}] \{size} bytes";
        }

        public static List<FileSegment> forFile(Path file, int desiredSegmentsCount) throws IOException {
            try (var raf = new RandomAccessFile(file.toFile(), "r")) {
                var segments = new ArrayList<FileSegment>();
                var fileSize = raf.length();
                var segmentSize = Math.max(1024 * 1024, fileSize / desiredSegmentsCount);

                var i = 1;
                var prevEnd = 0L;
                while (prevEnd < fileSize-1) {
                    var start = prevEnd;
                    var end = findNewLineAfter(raf, prevEnd + segmentSize, fileSize);
                    segments.add(new FileSegment(i, start, end - start));
                    prevEnd = end;
                }
                return segments;
            }
        }

        private static long findNewLineAfter(RandomAccessFile raf, long location, long fileSize) throws IOException {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                int c = raf.read();
                if (c == '\r' || c == '\n') break;
            }
            return Math.min(location, fileSize - 1);
        }
    }

    static class Accumulator {
        public final String name;
        public int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, count;
        public long sum;

        public Accumulator(String name) {
            this.name = name;
        }

        public void accumulate(int min, int max, int count, long sum) {
            if (this.min > min)
                this.min = min;
            if (this.max < max)
                this.max = max;
            this.count += count;
            this.sum += sum;
        }

        @Override
        public String toString() {
            var mean = Math.round((double) sum / count) / 10.0;
            return (min / 10.0) + "/" + mean + "/" + (max / 10.0);
        }
    }

    private static String summarizeTrackers(Tracker[] trackers) {
        var result = new TreeMap<String, Accumulator>();

        for (var i = 0; i < Tracker.SIZE; i++) {
            Accumulator acc = null;

            for (Tracker tracker : trackers) {
                var name = tracker.names[i];
                if (name == null) {
                    continue;
                }
                else if (acc == null || !name.equals(acc.name)) {
                    acc = result.computeIfAbsent(name, Accumulator::new);
                }
                acc.accumulate(
                        tracker.minMaxCount[i * 3 + OFFSET_MIN],
                        tracker.minMaxCount[i * 3 + OFFSET_MAX],
                        tracker.minMaxCount[i * 3 + OFFSET_COUNT],
                        tracker.sums[i]);
            }
        }
        return result.toString();
    }

    static class Tracker {
        public static final int SIZE = MAX_STATIONS * 10;

        private static final int CORRECTION_0_TO_9 = '0' * 10 + '0';
        private static final int CORRECTION_10_TO_99 = '0' * 100 + '0' * 10 + '0';

        private final byte[] tempNameBytes = new byte[MAX_STATION_NAME_LENGTH * MAX_UTF8_CODEPOINT_SIZE];
        private final int[] minMaxCount = new int[SIZE * 3];
        private final long[] sums = new long[SIZE];
        private final String[] names = new String[SIZE];
        private final byte[][] nameBytes = new byte[SIZE][];

        private void processSegment(MemorySegment memory) {
            long limit = memory.byteSize();

            var pos = 0;

            // skip newlines so the chunk limit check can work correctly
            while (pos < limit) {
                byte c = memory.get(ValueLayout.JAVA_BYTE, pos);
                if (c != '\r' && c != '\n')
                    break;
                pos++;
            }

            while (pos < limit) {
                byte b;

                int nameLength = 0, nameHash = 0;
                while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
                    tempNameBytes[nameLength++] = b;
                    nameHash = nameHash * 31 + b;
                }

                int sign;
                if (memory.get(ValueLayout.JAVA_BYTE, pos) == '-') {
                    sign = -1;
                    pos++;
                }
                else {
                    sign = 1;
                }

                int temperature; // between [-99.9 and 99.9], mapped to fixed point int (scaled by 10)
                if (memory.get(ValueLayout.JAVA_BYTE, pos + 1) == '.') { // between -9.99 and 9.99
                    assert memory.get(ValueLayout.JAVA_BYTE, pos + 1) == '.';
                    temperature = memory.get(ValueLayout.JAVA_BYTE, pos) * 10 +
                            memory.get(ValueLayout.JAVA_BYTE, pos + 2) - CORRECTION_0_TO_9;
                    pos += 3; // #.# - 3 chars
                }
                else { // between [-99.9 and -9.99] OR [9.99 and 99.9]
                    assert memory.get(ValueLayout.JAVA_BYTE, pos + 2) == '.';
                    temperature = memory.get(ValueLayout.JAVA_BYTE, pos) * 100 +
                            memory.get(ValueLayout.JAVA_BYTE, pos + 1) * 10 +
                            memory.get(ValueLayout.JAVA_BYTE, pos + 3) - CORRECTION_10_TO_99;
                    pos += 4; // ##.# - 4 chars
                }

                processLine(nameHash, tempNameBytes, nameLength, temperature * sign);

                // skip newlines so the chunk limit check can work correctly
                while (pos < limit) {
                    byte c = memory.get(ValueLayout.JAVA_BYTE, pos);
                    if (c != '\r' && c != '\n')
                        break;
                    pos++;
                }
            }
        }

        public void processLine(int nameHash, byte[] nameBytesBuffer, int nameLength, int temperature) {
            var i = Math.abs(nameHash) % SIZE;

            while (true) {
                if (names[i] == null) {
                    byte[] trimmedBytes = Arrays.copyOf(nameBytesBuffer, nameLength);
                    names[i] = new String(trimmedBytes, StandardCharsets.UTF_8);
                    nameBytes[i] = trimmedBytes;
                    minMaxCount[i*3 + OFFSET_MIN] = Integer.MAX_VALUE;
                    minMaxCount[i*3 + OFFSET_MAX] = Integer.MIN_VALUE;
                    break;
                }
                else if (nameBytes[i].length==nameLength && Arrays.equals(nameBytes[i], 0, nameLength, nameBytesBuffer, 0, nameLength)) {
                    break;
                }
                if (assertions) {
                    var key = new String(nameBytesBuffer, 0, nameLength, StandardCharsets.UTF_8);
                    hashCollisionOccurrences.computeIfAbsent(key, _ -> new LongAdder()).increment();
                }
                i = (i + 1) % SIZE;
            }
            if (assertions) {
                var key = new String(nameBytesBuffer, 0, nameLength, StandardCharsets.UTF_8);
                if (hashCollisionOccurrences.containsKey(key)) {
                    hashCollisionOccurrences.computeIfAbsent(STR."\{key}[\{i}]", _ -> new LongAdder()).increment();
                }
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

    }
}
