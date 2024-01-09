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
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class CalculateAverage_yavuztas {

    private static final Path FILE = Path.of("./measurements.txt");

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    /**
     * Merged into one class: Key + Measurements, this way less object creation
     */
    static class Record {

        // direct ref to underlying buffer, no copy is faster
        // It's safe as long as we make absolute get
        ByteBuffer buffer;
        int start;
        int length;
        int hash;

        private int min = 1000; // calculations over int is faster than double, we convert to double in the end only once
        private int max = -1000;
        private long sum;
        private long count;

        public Record(ByteBuffer buffer, int start, int length, int hash) {
            this.buffer = buffer;
            this.start = start;
            this.length = length;
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            final Record record = (Record) o;
            if (this.length != record.length || this.hash != record.hash)
                return false;

            int i = 0; // naive buffer mismatch check
            while (i < this.length && this.buffer.get(this.start + i) == record.buffer.get(record.start + i)) {
                i++;
            }
            return i == this.length;
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public String toString() {
            final byte[] bytes = new byte[this.length];
            this.buffer.get(this.start, bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        public void collect(int temp) {
            this.min = Math.min(this.min, temp);
            this.max = Math.max(this.max, temp);
            this.sum += temp;
            this.count++;
        }

        public void merge(Record other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
        }

        public String measurements() {
            final StringBuilder sb = new StringBuilder(14);
            sb.append(this.min / 10.0); // convert to double while generating the string output
            sb.append("/");
            sb.append(round((this.sum / 10.0) / this.count));
            sb.append("/");
            sb.append(this.max / 10.0);
            return sb.toString();
        }
    }

    // Inspired by @spullara - customized hashmap on purpose
    // The main difference is this map holds only one array instead of two
    static class KeyBufferMap {

        static final int SIZE = 1 << 15; // 32k - bigger bucket size less collisions
        static final int BITMASK = SIZE - 1;
        Record[] keys = new Record[SIZE];

        int hashBucket(int hash) {
            hash = hash ^ (hash >>> 16); // naive bit spreading but greatly decreases collision
            return hash & BITMASK; // fast modulo, to find bucket
        }

        void putAndCollect(Record key, int temp) {
            int bucket = hashBucket(key.hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                this.keys[bucket] = key;
                key.collect(temp);
                return;
            }

            if (!existing.equals(key)) {
                // collision, linear probing to find a slot
                while ((existing = this.keys[++bucket & BITMASK]) != null && !existing.equals(key)) {
                    // can be stuck here if all the buckets are full :(
                    // However, since the data set is max 10K (unique keys) this shouldn't happen
                    // So, I'm happily leave here branchless :)
                }
                if (existing == null) {
                    this.keys[bucket] = key;
                    key.collect(temp);
                    return;
                }
                existing.collect(temp);
            }
            else {
                existing.collect(temp);
            }
        }

        void putOrMerge(Record key) {
            int bucket = hashBucket(key.hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                this.keys[bucket] = key;
                return;
            }

            if (!existing.equals(key)) {
                // collision, linear probing to find a slot
                while ((existing = this.keys[++bucket & BITMASK]) != null && !existing.equals(key)) {
                    // can be stuck here if all the buckets are full :(
                    // However, since the data set is max 10K (unique keys) this shouldn't happen
                    // So, I'm happily leave here branchless :)
                }
                if (existing == null) {
                    this.keys[bucket] = key;
                    return;
                }
                existing.merge(key);
            }
            else {
                existing.merge(key);
            }
        }

        void forEach(Consumer<Record> consumer) {
            int pos = 0;
            Record key;
            while (pos < this.keys.length) {
                if ((key = this.keys[pos++]) == null) {
                    continue;
                }
                consumer.accept(key);
            }
        }

        void merge(KeyBufferMap other) {
            other.forEach(this::putOrMerge);
        }

    }

    static class FixedRegionDataAccessor {

        long startPos;
        long size;
        ByteBuffer buffer;

        public FixedRegionDataAccessor(long startPos, long size, ByteBuffer buffer) {
            this.startPos = startPos;
            this.size = size;
            this.buffer = buffer;
        }

        void accumulate(KeyBufferMap collector) {
            int start;
            int keyHash;
            int length;
            while (this.buffer.hasRemaining()) {

                byte b;
                keyHash = 0;
                length = 0;
                start = this.buffer.position(); // save line start position
                while ((b = this.buffer.get()) != ';') { // read until semicolon
                    keyHash = 31 * keyHash + b; // calculate key hash ahead, eleminates one more loop later
                    length++;
                }

                final Record key = new Record(this.buffer, start, length, keyHash);
                final int temp = readTemperature();
                this.buffer.get(); // skip linebreak

                collector.putAndCollect(key, temp);
            }
        }

        // Inspired by @yemreinci - Reading temparature value without Double.parse
        int readTemperature() {
            int temp = 0;
            final byte b1 = this.buffer.get(); // first byte
            final byte b2 = this.buffer.get(); // second byte
            final byte b3 = this.buffer.get(); // third byte
            if (b1 == '-') {
                if (b3 == '.') {
                    temp -= 10 * (b2 - '0') + this.buffer.get() - '0'; // fourth byte
                }
                else {
                    this.buffer.get(); // skip dot
                    temp -= 100 * (b2 - '0') + 10 * (b3 - '0') + this.buffer.get() - '0'; // fifth byte
                }
            }
            else {
                if (b2 == '.') {
                    temp = 10 * (b1 - '0') + b3 - '0';
                }
                else {
                    temp = 100 * (b1 - '0') + 10 * (b2 - '0') + this.buffer.get() - '0'; // fourth byte
                }
            }
            return temp;
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
            if (regionSize <= 8192) { // small file, no need concurrency
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
                    // shift position to back we find a linebreak
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

        void readAndCollect(KeyBufferMap output) {
            for (final FixedRegionDataAccessor accessor : this.accessors) {
                this.accessorPool.submit(() -> {
                    final KeyBufferMap partial = new KeyBufferMap();
                    accessor.accumulate(partial);
                    this.mergerThread.submit(() -> output.merge(partial));
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

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        final KeyBufferMap collector = new KeyBufferMap();
        try (final FastDataReader reader = new FastDataReader(FILE)) {
            reader.readAndCollect(collector);
        }

        final TreeMap<String, String> sorted = new TreeMap<>();
        collector.forEach(key -> sorted.put(key.toString(), key.measurements()));

        System.out.println(sorted);

    }

}
