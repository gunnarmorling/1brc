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

    static class Measurement {
        private int min; // calculations over int is faster than double, we convert to double in the end only once
        private int max;
        private int sum;
        private int count = 1;

        public Measurement(int initial) {
            this.min = initial;
            this.max = initial;
            this.sum = initial;
        }

        public String toString() { // convert to double while generating the string output
            return valueOf(this.min) + "/" + round(valueOf(this.sum) / this.count) + "/" + valueOf(this.max);
        }

        private double valueOf(int value) {
            return value / 10.0;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class KeyBuffer {

        ByteBuffer value;
        int hash;

        public KeyBuffer(ByteBuffer buffer) {
            this.value = buffer;
            this.hash = buffer.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            final KeyBuffer keyBuffer = (KeyBuffer) o;
            if (o == null || getClass() != o.getClass() || this.hash != keyBuffer.hash)
                return false;

            return this.value.equals(keyBuffer.value);
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public String toString() {
            final int limit = this.value.limit();
            final byte[] bytes = new byte[limit];
            this.value.get(bytes);
            return new String(bytes, 0, limit, StandardCharsets.UTF_8);
        }
    }

    static class FixedRegionDataAccessor {

        long startPos;
        long size;
        ByteBuffer buffer;
        int position; // relative

        public FixedRegionDataAccessor(long startPos, long size, ByteBuffer buffer) {
            this.startPos = startPos;
            this.size = size;
            this.buffer = buffer;
        }

        void traverse(BiConsumer<KeyBuffer, Integer> consumer) {

            int semiColonPos = 0;
            int lineBreakPos = 0;
            while (this.buffer.hasRemaining()) {

                byte b;
                while ((b = this.buffer.get()) != '\n') {
                    if (b == ';') { // save semicolon pos
                        semiColonPos = this.buffer.position(); // semicolon exclusive
                    }
                }
                // found linebreak
                lineBreakPos = this.buffer.position();

                this.buffer.position(this.position); // set back to line start
                final int length1 = semiColonPos - this.position; // station length
                final int length2 = lineBreakPos - semiColonPos; // temperature length

                final ByteBuffer station = getKeyRef(length1); // read station
                final int temperature = readTemperature(length2); // read temperature

                this.position = lineBreakPos; // skip to line end

                consumer.accept(new KeyBuffer(station), temperature);
            }
        }

        Map<KeyBuffer, Measurement> accumulate(Map<KeyBuffer, Measurement> initial) {

            traverse((station, temperature) -> {
                initial.compute(station, (k, m) -> {
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

        int readTemperature(int length) {
            int temp = 0;
            boolean negative = false;
            int digits = length - 3;
            byte b;
            while ((b = this.buffer.get()) != '\n') {
                if (b == '-') {
                    negative = true;
                    digits--;
                    continue;
                }
                if (b == '.') {
                    continue;
                }
                temp += (int) (Math.pow(10, digits--) * (b - 48));
            }
            return (negative) ? -temp : temp;
        }

        ByteBuffer getKeyRef(int length) {
            final ByteBuffer slice = this.buffer.slice().limit(length - 1);
            skip(this.buffer, length);
            return slice;
        }

        static void skip(ByteBuffer buffer, int length) {
            final int pos = buffer.position();
            buffer.position(pos + length);
        }

    }

    static class FastDataReader implements Closeable {

        private final FixedRegionDataAccessor[] accessors;
        private final ExecutorService mergerThread;
        private final ExecutorService accessorPool;

        public FastDataReader(Path path) throws IOException {
            var concurrency = Runtime.getRuntime().availableProcessors();
            final long fileSize = Files.size(path);
            long regionSize = fileSize / concurrency;

            if (regionSize > Integer.MAX_VALUE) {
                // TODO multiply concurrency and try again
                throw new IllegalArgumentException("Bigger than integer!");
            }
            // handling extreme cases
            if (regionSize <= 256) { // small file, no need concurrency
                concurrency = 1;
                regionSize = fileSize;
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

        void readAndCollect(Map<KeyBuffer, Measurement> output) {
            for (final FixedRegionDataAccessor accessor : this.accessors) {
                this.accessorPool.submit(() -> {
                    final Map<KeyBuffer, Measurement> partial = accessor.accumulate(new HashMap<>(1 << 10, 1)); // aka 1k
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
            while (buffer.get(position) != '\n') {
                position = --left;
            }
            return position;
        }

        private static Map<KeyBuffer, Measurement> mergeMaps(Map<KeyBuffer, Measurement> map1, Map<KeyBuffer, Measurement> map2) {
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
        final Map<KeyBuffer, Measurement> output = new HashMap<>(1 << 10, 1); // aka 1k
        try (final FastDataReader reader = new FastDataReader(FILE)) {
            reader.readAndCollect(output);
        }

        final TreeMap<String, Measurement> sorted = new TreeMap<>();
        output.forEach((s, measurement) -> sorted.put(s.toString(), measurement));
        System.out.println(sorted);
    }

}
