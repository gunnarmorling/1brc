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

        // Only accessed by a single thread, so it is safe to share
        private static final StringBuilder STRING_BUILDER = new StringBuilder(14);

        private int min; // calculations over int is faster than double, we convert to double in the end only once
        private int max;
        private long sum;
        private long count = 1;

        public Measurement(int initial) {
            this.min = initial;
            this.max = initial;
            this.sum = initial;
        }

        public String toString() {
            STRING_BUILDER.setLength(0); // clear the builder to reuse
            STRING_BUILDER.append(this.min / 10.0); // convert to double while generating the string output
            STRING_BUILDER.append("/");
            STRING_BUILDER.append(round((this.sum / 10.0) / this.count));
            STRING_BUILDER.append("/");
            STRING_BUILDER.append(this.max / 10.0);
            return STRING_BUILDER.toString();
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class KeyBuffer {

        ByteBuffer buffer;
        int length;
        int hash;

        public KeyBuffer(ByteBuffer buffer, int length, int hash) {
            this.buffer = buffer;
            this.length = length;
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            final KeyBuffer keyBuffer = (KeyBuffer) o;
            if (this.length != keyBuffer.length || this.hash != keyBuffer.hash)
                return false;

            return this.buffer.equals(keyBuffer.buffer);
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public String toString() {
            final byte[] bytes = new byte[this.length];
            this.buffer.get(bytes);
            return new String(bytes, 0, this.length, StandardCharsets.UTF_8);
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
            int keyHash;
            int length;
            while (this.buffer.hasRemaining()) {

                this.position = this.buffer.position(); // save line start pos

                byte b;
                keyHash = 0;
                length = 0;
                while ((b = this.buffer.get()) != ';') { // read until semicolon
                    keyHash = 31 * keyHash + b; // calculate key hash ahead, eleminates one more loop later
                    length++;
                }

                final ByteBuffer station = this.buffer.slice(this.position, length);
                final KeyBuffer key = new KeyBuffer(station, length, keyHash);

                this.buffer.mark(); // semicolon pos
                skip(3); // skip more since minimum temperature length is 3
                length = 4; // +1 for semicolon

                while (this.buffer.get() != '\n') {
                    length++; // read until linebreak
                    // TODO how to read temperature here
                }

                this.buffer.reset(); // set to after semicolon
                consumer.accept(key, readTemperature(length));
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

        // caching Math.pow calculation improves a lot!
        // interestingly, instance field access is much faster than static field access
        final int[] powerOfTenCache = new int[]{ 1, 10, 100 };

        int readTemperature(int length) {
            int temp = 0;
            final byte b1 = this.buffer.get(); // get first byte

            int digits = length - 4; // digit position
            final boolean negative = b1 == '-';
            if (!negative) {
                temp += this.powerOfTenCache[digits + 1] * (b1 - 48); // add first digit ahead
            }

            byte b;
            while ((b = this.buffer.get()) != '.') { // read until dot
                temp += this.powerOfTenCache[digits--] * (b - 48);
            }
            b = this.buffer.get(); // read after dot, only one digit no loop
            temp += this.powerOfTenCache[digits] * (b - 48);
            this.buffer.get(); // skip line break

            return (negative) ? -temp : temp;
        }

        ByteBuffer getKeyRef(int length) {
            final ByteBuffer slice = this.buffer.slice().limit(length - 1);
            skip(length);
            return slice;
        }

        void skip(int length) {
            final int pos = this.buffer.position();
            this.buffer.position(pos + length);
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

            // handling extreme cases
            while (regionSize > Integer.MAX_VALUE) {
                concurrency *= 2;
                regionSize = fileSize / concurrency;
            }
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
