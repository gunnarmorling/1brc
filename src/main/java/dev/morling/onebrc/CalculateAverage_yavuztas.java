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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class CalculateAverage_yavuztas {

    private static final Path FILE = Path.of("./measurements.txt");

    private static class Measurement {
        private double min;
        private double max;
        private double sum;
        private int count = 1;

        public Measurement(double initial) {
            this.min = initial;
            this.max = initial;
            this.sum = initial;
        }

        public String toString() {
            return STR."\{round(this.min)}/\{round(this.sum / this.count)}/\{round(this.max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class FixedRegionDataAccessor {

        static final byte SEMI_COLON = 59; // ';'
        static final byte LINE_BREAK = 10; // '\n'

        final byte[] workBuffer = new byte[64]; // assuming max 64 bytes for a row is enough

        long startPos;
        long size;
        ByteBuffer buffer;
        int position; // relative

        public FixedRegionDataAccessor(long startPos, long size, ByteBuffer buffer) {
            this.startPos = startPos;
            this.size = size;
            this.buffer = buffer;
        }

        void traverse(BiConsumer<String, Double> consumer) {

            int semiColonPos = 0;
            int lineBreakPos = 0;
            while (this.buffer.hasRemaining()) {

                while ((this.workBuffer[0] = this.buffer.get()) != LINE_BREAK) {
                    if (this.workBuffer[0] == SEMI_COLON) { // save semicolon pos
                        semiColonPos = this.buffer.position(); // semicolon exclusive
                    }
                }
                // found linebreak
                lineBreakPos = this.buffer.position();

                this.buffer.position(this.position); // set back to line start
                final int length1 = semiColonPos - this.position; // station length
                final int length2 = lineBreakPos - semiColonPos; // temperature length
                final String station = readString(length1); // read station
                final String temperature = readString(length2); // read temperature

                this.position = lineBreakPos; // skip to line end

                consumer.accept(station, Double.parseDouble(temperature));
            }
        }

        Map<String, Measurement> accumulate(Map<String, Measurement> initial) {

            traverse((station, temperature) -> {
                initial.compute(station, (_, m) -> {
                    if (m == null) {
                        return new Measurement(temperature);
                    }
                    // aggregate
                    m.min = Math.min(m.min, temperature);
                    m.max = Math.max(m.max, temperature);
                    m.sum += temperature;
                    m.count++;
                    return m;
                });
            });

            return initial;
        }

        String readString(int length) {
            this.buffer.get(this.workBuffer, 0, length);
            return new String(this.workBuffer, 0, length - 1, // strip the last char
                    StandardCharsets.UTF_8);
        }

    }

    static class FastDataReader implements Closeable {

        private final FixedRegionDataAccessor[] accessors;
        private final ExecutorService mergerThread;
        private final ExecutorService accessorPool;

        public FastDataReader(Path path) throws IOException {
            final var concurrency = Runtime.getRuntime().availableProcessors();
            final long fileSize = Files.size(path);
            final long regionSize = fileSize / concurrency;

            if (regionSize > Integer.MAX_VALUE) {
                // TODO multiply concurrency and try again
                throw new IllegalArgumentException("Bigger than integer!");
            }

            long startPosition = 0;
            this.accessors = new FixedRegionDataAccessor[concurrency];
            for (int i = 0; i < concurrency - 1; i++) {
                // map regions
                try (final FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
                    final long maxSize = startPosition + regionSize > fileSize ? fileSize - startPosition : regionSize;
                    final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, startPosition, maxSize);
                    this.accessors[i] = new FixedRegionDataAccessor(startPosition, maxSize, buffer);
                    // adjust positions back and forth until we find a linebreak!
                    final int closestPos = findClosestLineEnd((int) maxSize - 1, buffer);
                    buffer.limit(closestPos + 1);
                    startPosition += closestPos + 1;
                }
            }
            // map the last region
            try (final FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
                final long maxSize = fileSize - startPosition; // last region will take the rest
                final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, startPosition, maxSize);
                this.accessors[concurrency - 1] = new FixedRegionDataAccessor(startPosition, maxSize, buffer);
            }
            // create executors
            this.mergerThread = Executors.newSingleThreadExecutor();
            this.accessorPool = Executors.newFixedThreadPool(concurrency);
        }

        void readAndCollect(Map<String, Measurement> output) {
            for (final FixedRegionDataAccessor accessor : this.accessors) {
                this.accessorPool.submit(() -> {
                    final Map<String, Measurement> partial = accessor.accumulate(new HashMap<>(1 << 12, 1)); // aka 4k
                    this.mergerThread.submit(() -> mergeMaps(output, partial));
                });
            }
        }

        @Override
        public void close() {
            try {
                this.accessorPool.shutdown();
                this.accessorPool.awaitTermination(60, TimeUnit.SECONDS);
                this.mergerThread.shutdown();
                this.mergerThread.awaitTermination(60, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                this.accessorPool.shutdownNow();
                this.mergerThread.shutdownNow();
            }
        }

        /**
         * Scans the given buffer to the left
         */
        private static int findClosestLineEnd(int regionSize, ByteBuffer buffer) {
            int position = regionSize;
            int left = regionSize;
            while (buffer.get(position) != FixedRegionDataAccessor.LINE_BREAK) {
                position = --left;
            }
            return position;
        }

        private static Map<String, Measurement> mergeMaps(Map<String, Measurement> map1, Map<String, Measurement> map2) {
            map2.forEach((s, measurement) -> {
                map1.merge(s, measurement, (m1, m2) -> {
                    m1.min = Math.min(m1.min, m2.min);
                    m1.max = Math.max(m1.max, m2.max);
                    m1.sum += m2.sum;
                    m1.count += m2.count;
                    return m1;
                });
            });

            return map1;
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final Map<String, Measurement> output = new TreeMap<>();
        try (final FastDataReader reader = new FastDataReader(FILE)) {
            reader.readAndCollect(output);
        }
        System.out.println(output);
    }

}
