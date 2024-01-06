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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CalculateAverage_yemreinci {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var filename = args.length == 0 ? FILE : args[0];
        var file = new File(filename);
        long start = System.currentTimeMillis();

        var totalLines = new AtomicInteger();
        var results = getFileSegments(file).stream().map(segment -> {
            var resultMap = new ByteArrayToResultMap();
            long segmentEnd = segment.end();
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(filename), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());
                var buffer = new byte[64];
                int lines = 0;
                int startLine;
                int limit = bb.limit();
                while ((startLine = bb.position()) < limit) {
                    int currentPosition = startLine;
                    byte b;
                    int offset = 0;
                    int hash = 0;
                    while ((b = bb.get(currentPosition++)) != ';') {
                        buffer[offset++] = b;
                        hash = (hash << 5) - hash + b;
                    }

                    boolean negative = false;
                    if ((b = bb.get(currentPosition)) == '-') {
                        negative = true;
                        currentPosition++;
                    }

                    double temp;
                    if ((b = bb.get(currentPosition + 1)) == '.') { // temperature is in either XX.X or X.X form
                        temp = (bb.get(currentPosition) - '0') + (bb.get(currentPosition + 2) - '0') / 10.0;
                        currentPosition += 4;
                    }
                    else {
                        temp = (bb.get(currentPosition) - '0') * 10 + (b - '0') + (bb.get(currentPosition + 3) - '0') / 10.0;
                        currentPosition += 5;
                    }

                    double finalTemp = (negative ? -temp : temp);
                    resultMap.putOrMerge(hash, buffer, 0, offset,
                            () -> new Result(temp),
                            measurement -> merge(measurement, finalTemp, finalTemp, finalTemp, 1));
                    lines++;
                    bb.position(currentPosition);
                }
                totalLines.addAndGet(lines);
                return resultMap;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel().toList();

        var resultMap = results.stream()
                .flatMap(partition -> partition.getAll().stream())
                .collect(Collectors.toMap(e -> new String(e.key()), Entry::value, CalculateAverage_yemreinci::merge, TreeMap::new));

        System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println("Lines processed: " + totalLines);
        System.out.println(resultMap);
    }

    private static List<FileSegment> getFileSegments(File file) throws IOException {
        int numberOfSegments = Runtime.getRuntime().availableProcessors();
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfSegments;
        List<FileSegment> segments = new ArrayList<>();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < numberOfSegments; i++) {
                long segStart = i * segmentSize;
                long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
                segStart = findSegment(i, 0, randomAccessFile, segStart, segEnd);
                segEnd = findSegment(i, numberOfSegments - 1, randomAccessFile, segEnd, fileSize);

                segments.add(new FileSegment(segStart, segEnd));
            }
        }
        return segments;
    }

    private static Result merge(Result v, Result value) {
        return merge(v, value.min, value.max, value.sum, value.count);
    }

    private static Result merge(Result v, double value, double value1, double value2, long value3) {
        v.min = Math.min(v.min, value);
        v.max = Math.max(v.max, value1);
        v.sum += value2;
        v.count += value3;
        return v;
    }

    private static long findSegment(int i, int skipSegment, RandomAccessFile raf, long location, long fileSize) throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    break;
            }
        }
        return location;
    }

    static class Result {
        double min, max, sum;
        long count;

        Result(double value) {
            min = max = sum = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        double round(double v) {
            return Math.round(v * 10.0) / 10.0;
        }

    }

    static record Pair(int slot, Result slotValue) {
    }

    static record Entry(byte[] key, Result value) {
    }

    static record FileSegment(long start, long end) {
    }

    static class ByteArrayToResultMap {
        public static final int MAPSIZE = 1024 * 128;
        Result[] slots = new Result[MAPSIZE];
        byte[][] keys = new byte[MAPSIZE][];

        private Pair getPair(int hash, byte[] key, int offset, int size) {
            int slot = hash & (slots.length - 1);
            var slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            return new Pair(slot, slotValue);
        }

        public void putOrMerge(int hash, byte[] key, int offset, int size, Supplier<Result> supplier, Consumer<Result> merge) {
            Pair result = getPair(hash, key, offset, size);
            Result value = result.slotValue();
            if (value == null) {
                int slot = result.slot();
                slots[slot] = supplier.get();
                byte[] bytes = new byte[size];
                System.arraycopy(key, offset, bytes, 0, size);
                keys[slot] = bytes;
            }
            else {
                merge.accept(value);
            }
        }

        // Get all pairs
        public List<Entry> getAll() {
            List<Entry> result = new ArrayList<>();
            for (int i = 0; i < slots.length; i++) {
                Result slotValue = slots[i];
                if (slotValue != null) {
                    result.add(new Entry(keys[i], slotValue));
                }
            }
            return result;
        }
    }
}
