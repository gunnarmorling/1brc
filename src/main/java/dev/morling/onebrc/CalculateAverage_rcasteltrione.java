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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static java.util.stream.Collectors.toMap;

//baseline: 266s

public class CalculateAverage_rcasteltrione {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./backup/measurements.txt";

    public static void main(String[] args) throws IOException, InterruptedException {
        Path path = Paths.get(FILE);
        Instant start = Instant.now();

        var segList = FileSegment.forFile(path, Runtime.getRuntime().availableProcessors());
        var results = new ByteArrayToMeasurementMap[segList.size()];
        var threads = new Thread[segList.size()];
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            for (int i = 0; i < segList.size(); i++) {
                int finalI = i;
                FileSegment fileSegment = segList.get(finalI);
                var t = Thread.ofPlatform().start(() -> results[finalI] = processSegment(channel, fileSegment));
                threads[i] = t;
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }

        Map<String, Measurement> aggregatedMap = Arrays.stream(results)
                .flatMap(m -> m.entries().stream())
                .collect(toMap(
                        ByteArrayToMeasurementMap.Entry::key,
                        ByteArrayToMeasurementMap.Entry::value,
                        Measurement::merge,
                        TreeMap::new));

        System.out.println(aggregatedMap);
        // System.out.println(Duration.between(start, Instant.now()).toMillis());
    }

    private static ByteArrayToMeasurementMap processSegment(FileChannel channel, FileSegment seg) {
        try {
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, seg.start(), seg.size());
            byte b;
            var result = new ByteArrayToMeasurementMap();
            var lineBuffer = new byte[1 << 13];
            var segmentPosition = mbb.position();
            var limit = mbb.limit();
            var lastLineOffset = 0;

            while (segmentPosition < mbb.limit()) {

                int remaining = limit - segmentPosition;
                int chunk = Math.min(remaining, lineBuffer.length);
                mbb.get(segmentPosition, lineBuffer, 0, chunk);
                for (int i = chunk - 1; i >= 0; i--) {
                    if (lineBuffer[i] == '\n') {
                        lastLineOffset = i;
                        break;
                    }
                }
                for (int lineBufferOffset = 0; lineBufferOffset < lastLineOffset;) {
                    int nameHash = 0;
                    int nameLength = 0;
                    int nameStart = lineBufferOffset;
                    while ((b = lineBuffer[lineBufferOffset++]) != ';') {
                        nameHash = 31 * nameHash + b;
                        nameLength++;
                    }

                    int temp;
                    int negative = 1;
                    // var s = new String(Arrays.copyOfRange(lineBuffer, nameStart, lineOffset - 1), StandardCharsets.UTF_8);
                    if (lineBuffer[lineBufferOffset] == '-') {
                        lineBufferOffset++;
                        negative = -1;
                    }

                    // Temperature value: non-null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
                    if (lineBuffer[lineBufferOffset + 1] == '.') {
                        temp = (lineBuffer[lineBufferOffset] - '0') * 10 + (lineBuffer[lineBufferOffset + 2] - '0');
                        lineBufferOffset += 3;
                    }
                    else {
                        temp = (lineBuffer[lineBufferOffset] - '0') * 100
                                + (lineBuffer[lineBufferOffset + 1] - '0') * 10
                                + (lineBuffer[lineBufferOffset + 3] - '0');
                        lineBufferOffset += 4;
                    }
                    if (lineBuffer[lineBufferOffset] == '\r') {
                        lineBufferOffset++;
                    }
                    lineBufferOffset++;

                    temp *= negative;
                    result.mergeOrCreate(lineBuffer, nameStart, nameLength, nameHash, temp);
                    // segmentPosition += lineOffset;
                    // i += lineoffset;
                }

                segmentPosition += lastLineOffset + 1;

            }

            return result;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record FileSegment(long start, long size) {
        public static List<FileSegment> forFile(Path file, int desiredSegmentsCount) throws IOException {
            try (var raf = new RandomAccessFile(file.toFile(), "r")) {
                var segments = new ArrayList<FileSegment>();
                var fileSize = raf.length();
                if (fileSize < 1000000) {
                    return Collections.singletonList(new FileSegment(0, fileSize));
                }
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

    static class Measurement {
        int min, max, n;
        long sum;

        private Measurement(int min, int max, long sum, int n) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.n = n;
        }

        public Measurement(int temp) {
            this(temp, temp, temp, 1);
        }

        final Measurement merge(Measurement other) {
            this.min = Math.min(other.min, this.min);
            this.max = Math.max(other.max, this.max);
            this.sum += other.sum;
            this.n += other.n;
            return this;
        }

        @Override
        public String toString() {
            return STR."\{round(min)}/\{round(((double) sum / n))}/\{round(max)}";
        }

        double round(double v) {
            return Math.round(v) / 10.0;
        }
    }

    static class ByteArrayToMeasurementMap {

        public static final int DEFAULT_CAPACITY = 1024;
        public static final float LOAD_FACTOR = 0.75f;
        MeasurementSlot[] slots = new MeasurementSlot[DEFAULT_CAPACITY];
        int threshold = (int) (DEFAULT_CAPACITY * LOAD_FACTOR);
        int size = 0;

        private record MeasurementSlot(int hash, byte[] key, String city, Measurement measurement) {
        }

        public final void mergeOrCreate(byte[] line, int nameStart, int nameLength, int hash, int temperature) {
            int hashMask = slots.length - 1;

            for (int idx = hash & hashMask;; idx = (idx + 1) & hashMask) {
                MeasurementSlot slot = slots[idx];
                if (slot == null) {
                    size++;
                    if (size > threshold) {
                        idx = resize(hash);
                    }
                    byte[] nameBuffer = new byte[nameLength];
                    System.arraycopy(line, nameStart, nameBuffer, 0, nameLength);
                    slots[idx] = new MeasurementSlot(
                            hash,
                            nameBuffer,
                            new String(nameBuffer, StandardCharsets.UTF_8),
                            new Measurement(temperature));
                    return;
                }

                if (slot.hash == hash && arrayEquals(slot.key, line, nameStart, nameLength)) {
                    Measurement value = slots[idx].measurement;
                    value.min = Math.min(value.min, temperature);
                    value.max = Math.max(value.max, temperature);
                    value.sum += temperature;
                    value.n++;
                    return;
                }
            }
        }

        private int resize(int hash) {
            var oldSlots = slots;
            var newSlots = new MeasurementSlot[oldSlots.length << 1];
            var mask = newSlots.length - 1;
            for (MeasurementSlot oldSlot : oldSlots) {
                if (oldSlot == null) {
                    continue;
                }
                int idx = oldSlot.hash & mask;
                while (newSlots[idx] != null) {
                    idx = (idx + 1) & mask;
                }
                newSlots[idx] = oldSlot;
            }

            slots = newSlots;
            threshold = (int) (newSlots.length * LOAD_FACTOR);
            int hashMask = slots.length - 1;
            int idx;
            for (idx = hash & hashMask; slots[idx] != null; idx = (idx + 1) & hashMask) {
            }
            return idx;
        }

        private boolean arrayEquals(byte[] storedKey, byte[] line, int nameStart, int nameLength) {
            if (storedKey.length != nameLength) {
                return false;
            }

            for (int i = 0; i < storedKey.length; i++) {
                if (storedKey[i] != line[nameStart + i]) {
                    return false;
                }
            }
            return true;
        }

        private static int hashCode(int h) {
            h ^= (h >>> 20) ^ (h >>> 12);
            h ^= (h >>> 7) ^ (h >>> 4);
            h += h << 7;
            return h;
        }

        public final List<Entry> entries() {
            var result = new ArrayList<Entry>(slots.length);
            for (MeasurementSlot slot : slots) {
                if (slot != null) {
                    result.add(new Entry(slot.city, slot.measurement));
                }
            }
            return result;
        }

        public record Entry(String key, Measurement value) {
        }

    }

}
