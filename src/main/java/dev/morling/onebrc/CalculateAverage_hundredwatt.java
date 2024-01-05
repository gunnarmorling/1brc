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
import java.util.stream.Collectors;

public class CalculateAverage_hundredwatt {
    private static final String FILE = "./measurements.txt";

    /*
     * My results on this computer:
     *
     * CalculateAverage: 2m37.788s
     * CalculateAverage_royvanrijn: 0m29.639s
     * CalculateAverage_spullara: 0m2.013s
     *
     */

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        var filename = args.length == 0 ? FILE : args[0];
        var file = new File(filename);

        var resultsMap = getFileSegments(file).stream().map(segment -> {
            var resultMap = new ByteArrayToResultMap();
            long segmentEnd = segment.end();
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(filename), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());
                // Up to 100 characters for a city name
                var buffer = new byte[100];
                int startLine;
                int limit = bb.limit();
                while ((startLine = bb.position()) < limit) {
                    byte b;
                    int hash, offset, currentPosition;

                    // first character
                    buffer[0] = bb.get(startLine);
                    hash = buffer[0]; // 31 * 0 + b == b
                    offset = 1;
                    currentPosition = startLine + 1;

                    // rest of city name characters until ';'
                    while (currentPosition != segmentEnd && (b = bb.get(currentPosition++)) != ';') {
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
                        temp = negative * ((bb.get(currentPosition) - '0') * 100 + ((bb.get(currentPosition + 1) - '0') * 10 + (bb.get(currentPosition + 3) - '0')));
                        currentPosition += 4;
                    }
                    if (bb.get(currentPosition) == '\r') {
                        currentPosition++;
                    }
                    currentPosition++;
                    resultMap.putOrMerge(buffer, 0, offset, temp / 10.0, hash);
                    bb.position(currentPosition);
                }
                return resultMap;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel().flatMap(partition -> partition.getAll().stream())
                .collect(Collectors.toMap(e -> new String(e.key()), Entry::value, CalculateAverage_hundredwatt::merge, TreeMap::new));

        System.out.println(resultsMap);
    }

    private static List<FileSegment> getFileSegments(File file) throws IOException {
        int numberOfSegments = Runtime.getRuntime().availableProcessors();
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfSegments;
        List<FileSegment> segments = new ArrayList<>(numberOfSegments);
        // Pointless to split small files
        if (segmentSize < 1_000_000) {
            segments.add(new FileSegment(0, fileSize));
            return segments;
        }
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
}

// use classes from _spullara:
// * Result
// * Entry
// * FileSegment
// * ByteArrayToResultMap