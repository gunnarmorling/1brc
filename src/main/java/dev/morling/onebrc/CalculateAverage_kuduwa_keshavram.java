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
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import sun.misc.Unsafe;

public class CalculateAverage_kuduwa_keshavram {

    private static final String FILE = "./measurements.txt";
    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        TreeMap<String, Measurement> resultMap = getFileSegments(new File(FILE))
                .flatMap(
                        segment -> {
                            Result result = new Result();
                            while (segment.start < segment.end) {
                                byte[] city = new byte[100];
                                byte b;
                                int hash = 0;
                                int i = 0;
                                while ((b = UNSAFE.getByte(segment.start++)) != 59) {
                                    hash = 31 * hash + b;
                                    city[i++] = b;
                                }

                                byte[] newCity = new byte[i];
                                System.arraycopy(city, 0, newCity, 0, i);
                                int measurement = 0;
                                boolean negative = false;
                                while ((b = UNSAFE.getByte(segment.start++)) != 10) {
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
                                        result,
                                        new Measurement(hash, newCity, negative ? measurement * -1 : measurement));
                            }
                            Iterator<Measurement> iterator = getMeasurementIterator(result);
                            return StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL), true);
                        })
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

    private static Iterator<Measurement> getMeasurementIterator(Result result) {
        return new Iterator<>() {
            final int uniqueIndex = result.uniqueIndex;
            final int[] indexArray = result.indexArray;
            final Measurement[][] measurements = result.measurements;

            int i = 0;
            int j = 0;

            @Override
            public boolean hasNext() {
                return i < uniqueIndex;
            }

            @Override
            public Measurement next() {
                Measurement measurement = measurements[indexArray[i]][j++];
                if (measurements[indexArray[i]][j] == null) {
                    i++;
                    j = 0;
                }
                return measurement;
            }
        };
    }

    static class Result {
        final Measurement[][] measurements = new Measurement[1024 * 128][3];
        final int[] indexArray = new int[10_000];
        int uniqueIndex = 0;
    }

    private static void putOrMerge(Result result, Measurement measurement) {
        int index = measurement.hash & (result.measurements.length - 1);
        Measurement[] existing = result.measurements[index];
        for (int i = 0; i < existing.length; i++) {
            Measurement existingMeasurement = existing[i];
            if (existingMeasurement == null) {
                result.measurements[index][i] = measurement;
                if (i == 0) {
                    result.indexArray[result.uniqueIndex++] = index;
                }
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

    private static final class FileSegment {
        long start;
        long end;

        private FileSegment(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    private static final class Measurement {

        private final int hash;
        private final byte[] city;

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

    private static Stream<FileSegment> getFileSegments(final File file) throws IOException {
        final int numberOfSegments = Runtime.getRuntime().availableProcessors() * 4;
        final long[] chunks = new long[numberOfSegments + 1];
        try (var fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            final long fileSize = fileChannel.size();
            final long segmentSize = (fileSize + numberOfSegments - 1) / numberOfSegments;
            final long mappedAddress = fileChannel.map(MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            chunks[0] = mappedAddress;
            final long endAddress = mappedAddress + fileSize;
            for (int i = 1; i < numberOfSegments; ++i) {
                long chunkAddress = mappedAddress + i * segmentSize;
                // Align to first row start.
                while (chunkAddress < endAddress && UNSAFE.getByte(chunkAddress++) != '\n') {
                    // nop
                }
                chunks[i] = Math.min(chunkAddress, endAddress);
            }
            chunks[numberOfSegments] = endAddress;
        }
        return IntStream.range(0, chunks.length - 1)
                .mapToObj(chunkIndex -> new FileSegment(chunks[chunkIndex], chunks[chunkIndex + 1]))
                .parallel();
    }

}
