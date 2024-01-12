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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_kuduwa_keshavram {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException, InterruptedException {
        TreeMap<String, Measurement> resultMap = getFileSegments(new File(FILE)).stream()
                .parallel()
                .map(
                        segment -> {
                            final Measurement[][] measurements = new Measurement[1024 * 128][3];
                            try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                                MappedByteBuffer byteBuffer = fileChannel.map(
                                        MapMode.READ_ONLY, segment.start, segment.end - segment.start);
                                byteBuffer.order(ByteOrder.nativeOrder());
                                while (byteBuffer.hasRemaining()) {
                                    byte[] city = new byte[100];
                                    byte b;
                                    int hash = 0;
                                    int i = 0;
                                    while ((b = byteBuffer.get()) != 59) {
                                        hash = 31 * hash + b;
                                        city[i++] = b;
                                    }

                                    byte[] newCity = new byte[i];
                                    System.arraycopy(city, 0, newCity, 0, i);
                                    int measurement = 0;
                                    boolean negative = false;
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
                                    putOrMerge(
                                            measurements,
                                            new Measurement(
                                                    hash, newCity, negative ? measurement * -1 : measurement));
                                }
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return measurements;
                        })
                .flatMap(measurements -> Arrays.stream(measurements).flatMap(Arrays::stream))
                .filter(Objects::nonNull)
                .collect(
                        Collectors.toMap(
                                measurement -> new String(measurement.city),
                                Function.identity(),
                                (m1, m2) -> {
                                    m1.merge(m2);
                                    return m1;
                                },
                                TreeMap::new));

        System.out.println(resultMap);
    }

    private static void putOrMerge(Measurement[][] measurements, Measurement measurement) {
        int index = measurement.hash & (measurements.length - 1);
        Measurement[] existing = measurements[index];
        for (int i = 0; i < existing.length; i++) {
            Measurement existingMeasurement = existing[i];
            if (existingMeasurement == null) {
                measurements[index][i] = measurement;
                return;
            }
            if (equals(existingMeasurement.city, measurement.city)) {
                existingMeasurement.merge(measurement);
                return;
            }
        }
    }

    private static boolean equals(byte[] city1, byte[] city2) {
        for (int i = 0; i < city1.length; i++) {
            if (city1[i] != city2[i]) {
                return false;
            }
        }
        return true;
    }

    private record FileSegment(long start, long end) {
    }

    private static final class Measurement {

        private int hash;
        private byte[] city;

        int min;
        int max;
        int sum;
        int count;

        private Measurement(int hash, byte[] city, int temp) {
            this.hash = hash;
            this.city = city;
            this.min = this.max = this.sum = temp;
            this.count = 1;
        }

        private void merge(Measurement m2) {
            this.min = this.min < m2.min ? this.min : m2.min;
            this.max = this.max > m2.max ? this.max : m2.max;
            this.sum = this.sum + m2.sum;
            this.count = this.count + m2.count;
        }

        @Override
        public String toString() {
            return String.format(
                    "%.1f/%.1f/%.1f", this.min / 10f, (this.sum / 10f) / this.count, this.max / 10f);
        }
    }

    private static List<FileSegment> getFileSegments(final File file) throws IOException {
        final int numberOfSegments = Runtime.getRuntime().availableProcessors() * 4;
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
