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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class CalculateAverage_jatingala {

    private static final String FILE = "./measurements.txt";

    public static void main(final String[] args) throws IOException {
        final int processorCount = Math.max(Runtime.getRuntime().availableProcessors(), 8);

        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(FILE, "r")) {
            final long[][] chunks = getChunkPositions(randomAccessFile, processorCount);
            final Map<String, Statistics> result = Arrays.stream(chunks)
                    .parallel()
                    .map(chunk -> {
                        try {
                            final MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, chunk[0], chunk[1]);
                            return consumeChunk(mappedByteBuffer);
                        }
                        catch (final Exception e) {
                            System.out.println(e.getMessage());
                            return new HashMap<String, Statistics>();
                        }
                    })
                    .reduce((a, b) -> {
                        a.forEach((k, v) -> {
                            final Statistics s = b.get(k);
                            if (s != null)
                                v.merge(s);
                        });
                        b.forEach(a::putIfAbsent);
                        return a;
                    })
                    .orElseGet(Collections::emptyMap);

            System.out.println(new TreeMap<>(result));
        }
    }

    private static long[][] getChunkPositions(final RandomAccessFile file, final int chunks) throws IOException {
        final long[][] result = new long[chunks][];

        final long fileSize = file.length();
        final long chunkSize = Math.ceilDiv(fileSize, chunks);
        long chunkStartPosition = 0;
        for (int i = 0; i < chunks; i++) {
            file.seek(Math.min(chunkStartPosition + chunkSize, fileSize));

            while (file.getFilePointer() < fileSize && file.readByte() != '\n') {
                // find next newline, noop
            }

            // startPointer & length
            result[i] = new long[]{ chunkStartPosition, file.getFilePointer() - chunkStartPosition };
            chunkStartPosition = file.getFilePointer();
        }

        return result;
    }

    private static Map<String, Statistics> consumeChunk(final MappedByteBuffer mappedByteBuffer) {
        final Map<String, Statistics> statisticsMap = new HashMap<>();

        while (mappedByteBuffer.hasRemaining()) {
            final String key = parseKey(mappedByteBuffer);
            final double value = parseNumber(mappedByteBuffer);
            statisticsMap.computeIfAbsent(key, _ -> new Statistics()).update(value);
        }

        return statisticsMap;
    }

    private static String parseKey(final MappedByteBuffer mappedByteBuffer) {
        final ByteArrayOutputStream keyBytes = new ByteArrayOutputStream();
        while (mappedByteBuffer.hasRemaining()) {
            final byte val = mappedByteBuffer.get();
            if (val == ';')
                break;
            keyBytes.write(val);
        }
        return keyBytes.toString();
    }

    private static double parseNumber(final MappedByteBuffer mappedByteBuffer) {
        boolean negate = false;
        int temp = 0;
        while (mappedByteBuffer.hasRemaining()) {
            final byte val = mappedByteBuffer.get();

            if (val == '\n')
                break;
            if (val == '-') {
                negate = true;
                continue;
            }
            if (val == '.')
                continue;

            temp = 10 * temp + (val - '0');
        }
        return (negate ? -1 : 1) * (temp / 10.0);
    }

    private static class Statistics {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        private static double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public void update(final double reading) {
            this.min = Math.min(min, reading);
            this.max = Math.max(max, reading);
            this.sum += reading;
            ++count;
        }

        public void merge(final Statistics other) {
            this.min = Math.min(min, other.min);
            this.max = Math.max(max, other.max);
            this.sum += other.sum;
            this.count += other.count;
        }

        @Override
        public String toString() {
            return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
        }
    }
}
