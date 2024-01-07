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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_deemkeen {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        File file = new File(FILE);
        long fileSize = file.length();
        int numberOfSegments = 400;
        long segmentSize = fileSize / numberOfSegments;

        if (segmentSize < 100) {
            numberOfSegments = 1;
            segmentSize = fileSize;
        }

        List<SegmentPair> segments = new ArrayList<>();

        try (
                var randomAccessFile = new RandomAccessFile(file, "r");
                var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
            for (int i = 0; i < numberOfSegments; i++) {
                long segStart = i * segmentSize;
                long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;

                if (i != 0) {
                    randomAccessFile.seek(segStart);
                    while (segStart < segEnd) {
                        segStart++;
                        if (randomAccessFile.read() == '\n')
                            break;
                    }
                }

                if (i != numberOfSegments - 1) {
                    randomAccessFile.seek(segEnd);
                    while (segEnd < fileSize) {
                        segEnd++;
                        if (randomAccessFile.read() == '\n')
                            break;
                    }
                }

                segments.add(new SegmentPair(new FileSegment(segStart, segEnd), new ByteArrayToResultMap()));
            }

            try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
                var partitions = Collections.synchronizedList(new ArrayList<ByteArrayToResultMap>());
                for (var segment : segments) {
                    var segmentResultMap = segment.value;
                    es.execute(() -> {
                        MappedByteBuffer bb;
                        try {
                            bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.key.start, segment.key.end - segment.key.start);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        byte[] buffer = new byte[100];
                        int startLine;
                        while ((startLine = bb.position()) < bb.limit()) {
                            int currentPosition = startLine;
                            byte b;
                            int offset = 0;
                            int hash = 0;
                            while (currentPosition != segment.key.end && (b = bb.get(currentPosition++)) != ';') {
                                buffer[offset++] = b;
                                hash = 31 * hash + b;
                            }
                            int temp;
                            int negative = 1;
                            // Inspired by @yemreinci to unroll this even further
                            if (bb.get(currentPosition) == '-') {
                                negative = -1;
                                currentPosition++;
                            }
                            if (bb.get(currentPosition + 1) == '.') {
                                temp = negative * ((bb.get(currentPosition) - '0') * 10 + (bb.get(currentPosition + 2) - '0'));
                                currentPosition += 3;
                            }
                            else {
                                temp = negative
                                        * ((bb.get(currentPosition) - '0') * 100 + ((bb.get(currentPosition + 1) - '0') * 10 + (bb.get(currentPosition + 3) - '0')));
                                currentPosition += 4;
                            }
                            if (bb.get(currentPosition) == '\r') {
                                currentPosition++;
                            }
                            currentPosition++;
                            segmentResultMap.putOrMerge(buffer, 0, offset, temp / 10.0, hash);
                            bb.position(currentPosition);
                        }

                    });

                    partitions.add(segmentResultMap);

                }

                try {
                    es.shutdown();
                    while (!es.awaitTermination(24L, TimeUnit.HOURS)) {
                        System.out.println("Still waiting for termination..");
                    }
                }
                catch (InterruptedException e) {
                    // do nothing
                }

                TreeMap<String, Result> resultMap = new TreeMap<>();
                for (ByteArrayToResultMap partition : partitions) {
                    for (Entry e : partition.getAll()) {
                        resultMap.merge(new String(e.key()), e.value(), CalculateAverage_deemkeen::merge);
                    }
                }

                System.out.println(resultMap);
            }
        }
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

    record Pair(int slot, Result slotValue) {
    }

    record SegmentPair(FileSegment key, ByteArrayToResultMap value) {
    }

    record Entry(byte[] key, Result value) {
    }

    record FileSegment(long start, long end) {
    }

    static class Result {
        double min;
        double max;
        double sum;
        long count;

        Result(double value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return round(min) +
                    "/" + round(sum / count) +
                    "/" + round(max);
        }

        double round(double v) {
            return Math.round(v * 10.0) / 10.0;
        }

    }

    static class ByteArrayToResultMap {
        public static final int MAPSIZE = 1024 * 128;
        Result[] slots = new Result[MAPSIZE];
        byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(byte[] key, int offset, int size, double temp, int hash) {
            int slot = hash & (slots.length - 1);
            var slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            Result value = slotValue;
            if (value == null) {
                slots[slot] = new Result(temp);
                byte[] bytes = new byte[size];
                System.arraycopy(key, offset, bytes, 0, size);
                keys[slot] = bytes;
            }
            else {
                value.min = Math.min(value.min, temp);
                value.max = Math.max(value.max, temp);
                value.sum += temp;
                value.count += 1;
            }
        }

        private int hashCode(byte[] a, int fromIndex, int length) {
            int result = 0;
            int end = fromIndex + length;
            for (int i = fromIndex; i < end; i++) {
                result = 31 * result + a[i];
            }
            return result;
        }

        private Pair getPair(byte[] key, int offset, int size) {
            int hash = hashCode(key, offset, size);
            int slot = hash & (slots.length - 1);
            Result slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            return new Pair(slot, slotValue);
        }

        public Result get(byte[] key, int offset, int size) {
            return getPair(key, offset, size).slotValue();
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