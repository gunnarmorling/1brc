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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class CalculateAverage_kuduwa_keshavram {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, IntSummaryStatistics> resultMap = getFileSegments(new File(FILE)).stream()
                .parallel()
                .flatMap(
                        segment -> {
                            try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                                MappedByteBuffer byteBuffer = fileChannel.map(
                                        MapMode.READ_ONLY, segment.start, segment.end - segment.start);
                                byteBuffer.order(ByteOrder.nativeOrder());
                                Iterator<Measurement> iterator = getMeasurementIterator(byteBuffer);
                                return StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL), true);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(
                        Collectors.groupingBy(
                                Measurement::city, Collectors.summarizingInt(Measurement::temp)));
        System.out.println(
                resultMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(
                                entry -> String.format(
                                        "%s=%.1f/%.1f/%.1f",
                                        entry.getKey(),
                                        entry.getValue().getMin() / 10f,
                                        entry.getValue().getAverage() / 10f,
                                        entry.getValue().getMax() / 10f))
                        .collect(Collectors.joining(", ", "{", "}")));
    }

    private static Iterator<Measurement> getMeasurementIterator(MappedByteBuffer byteBuffer) {
        return new Iterator<>() {

            private int initialPosition;
            private int delimiterIndex;

            @Override
            public boolean hasNext() {
                boolean hasRemaining = byteBuffer.hasRemaining();
                if (hasRemaining) {
                    initialPosition = byteBuffer.position();
                    delimiterIndex = 0;
                    while (true) {
                        byte b = byteBuffer.get();
                        if (b == 59) {
                            break;
                        }
                        delimiterIndex++;
                    }
                    return true;
                }
                return false;
            }

            @Override
            public Measurement next() {
                byteBuffer.position(initialPosition);

                byte[] city = new byte[delimiterIndex];
                for (int i = 0; i < delimiterIndex; i++) {
                    city[i] = byteBuffer.get();
                }

                byteBuffer.get();
                int measurement = 0;
                boolean negative = false;
                byte b;
                while ((b = byteBuffer.get()) != 10) {
                    if (b == 45) {
                        negative = true;
                    }
                    else if (b == 46) {
                        // skip
                    }
                    else {
                        final int n = b - '0';
                        measurement = measurement * 10 + n;
                    }
                }
                return new Measurement(new String(city), negative ? measurement * -1 : measurement);
            }
        };
    }

    private record FileSegment(long start, long end) {
    }

    private record Measurement(String city, int temp) {
    }

    private static List<FileSegment> getFileSegments(final File file) throws IOException {
        final int numberOfSegments = Runtime.getRuntime().availableProcessors() * 2;
        final long fileSize = file.length();
        final long segmentSize = fileSize / numberOfSegments;
        if (segmentSize < 1000) {
            return List.of(new FileSegment(0, fileSize));
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            int lastSegment = numberOfSegments - 1;
            return IntStream.range(0, numberOfSegments)
                    .mapToObj(
                            i -> {
                                long segStart = i * segmentSize;
                                long segEnd = (i == lastSegment) ? fileSize : segStart + segmentSize;
                                try {
                                    segStart = findSegment(i, 0, randomAccessFile, segStart, segEnd);
                                    segEnd = findSegment(i, lastSegment, randomAccessFile, segEnd, fileSize);
                                }
                                catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                return new FileSegment(segStart, segEnd);
                            })
                    .toList();
        }
    }

    private static long findSegment(
                                    final int i, final int skipSegment, RandomAccessFile raf, long location, final long fileSize)
            throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    return location;
            }
        }
        return location;
    }
}
