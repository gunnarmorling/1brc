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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class CalculateAverage_kuduwa_keshavram {

    private static final String FILE = "./measurements.txt";
    private static final Measurement[][] MEASUREMENTS = new Measurement[1024 * 128][3];

    public static void main(String[] args) throws IOException, InterruptedException {
        getFileSegments(new File(FILE)).stream()
                .parallel()
                .flatMap(
                        segment -> {
                            try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                                MappedByteBuffer byteBuffer = fileChannel.map(MapMode.READ_ONLY, segment.start, segment.end - segment.start);
                                byteBuffer.order(ByteOrder.nativeOrder());
                                Iterator<Measurement> iterator = getMeasurementIterator(byteBuffer);
                                return StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL), true);
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .forEach(CalculateAverage_kuduwa_keshavram::putOrMerge);

        Map<String, String> resultMap = new TreeMap<>();
        Arrays.stream(MEASUREMENTS)
                .flatMap(Arrays::stream)
                .filter(Objects::nonNull)
                .forEach(
                        measurement -> resultMap.put(
                                new String(measurement.city),
                                String.format(
                                        "%.1f/%.1f/%.1f",
                                        measurement.min / 10f,
                                        (measurement.sum / 10f) / measurement.count,
                                        measurement.max / 10f)));
        System.out.println(resultMap);
    }

    private static void putOrMerge(Measurement measurement) {
        int index = measurement.hash & (MEASUREMENTS.length - 1);
        Measurement[] existing = MEASUREMENTS[index];
        for (int i = 0; i < existing.length; i++) {
            Measurement existingMeasurement = existing[i];
            if (existingMeasurement == null) {
                MEASUREMENTS[index][i] = measurement;
                return;
            }
            if (Arrays.equals(existingMeasurement.city, measurement.city)) {
                existingMeasurement.merge(measurement);
                return;
            }
        }
    }

    private static Iterator<Measurement> getMeasurementIterator(MappedByteBuffer byteBuffer) {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return byteBuffer.hasRemaining();
            }

            @Override
            public Measurement next() {
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
                return new Measurement(hash, newCity, negative ? measurement * -1 : measurement);
            }
        };
    }

    private record FileSegment(long start, long end) {
    }

    private record Result(String key, String value) {
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
