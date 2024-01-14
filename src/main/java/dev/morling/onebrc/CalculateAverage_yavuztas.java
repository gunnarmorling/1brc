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

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import java.util.function.Consumer;

public class CalculateAverage_yavuztas {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final Unsafe UNSAFE = unsafe();

    // Tried all there: MappedByteBuffer, MemorySegment and Unsafe
    // Accessing the memory using Unsafe is still the fastest in my experience
    private static Unsafe unsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Only one object, both for measurements and keys, less object creation in hotpots is always faster
    static class Record {

        // keep memory starting address for each segment
        // since we use Unsafe, this is enough to align and fetch the data
        long segment;
        int start;
        int length;
        int hash;

        private int min = 1000; // calculations over int is faster than double, we convert to double in the end only once
        private int max = -1000;
        private long sum;
        private long count;

        public Record(long segment, int start, int length, int hash) {
            this.segment = segment;
            this.start = start;
            this.length = length;
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            final Record record = (Record) o;
            return equals(record.segment, record.start, record.length, record.hash);
        }

        /**
         * Stateless equals, no Record object needed
         */
        public boolean equals(long segment, int start, int length, int hash) {
            if (this.length != length || this.hash != hash)
                return false;

            int i = 0; // bytes mismatch check
            while (i < this.length
                    && UNSAFE.getByte(this.segment + this.start + i) == UNSAFE.getByte(segment + start + i)) {
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
            int i = 0;
            while (i < this.length) {
                bytes[i] = UNSAFE.getByte(this.segment + this.start + i++);
            }

            return new String(bytes, StandardCharsets.UTF_8);
        }

        public Record collect(int temp) {
            this.min = Math.min(this.min, temp);
            this.max = Math.max(this.max, temp);
            this.sum += temp;
            this.count++;
            return this;
        }

        public void merge(Record other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
        }

        public String measurements() {
            // here is only executed once for each unique key, so StringBuilder creation doesn't harm
            final StringBuilder sb = new StringBuilder(14);
            sb.append(this.min / 10.0);
            sb.append("/");
            sb.append(round((this.sum / 10.0) / this.count));
            sb.append("/");
            sb.append(this.max / 10.0);
            return sb.toString();
        }
    }

    // Inspired by @spullara - customized hashmap on purpose
    // The main difference is we hold only one array instead of two
    static class RecordMap {

        static final int SIZE = 1 << 15; // 32k - bigger bucket size less collisions
        static final int BITMASK = SIZE - 1;
        Record[] keys = new Record[SIZE];

        static int hashBucket(int hash) {
            hash = hash ^ (hash >>> 16); // naive bit spreading but surprisingly decreases collision :)
            return hash & BITMASK; // fast modulo, to find bucket
        }

        void putAndCollect(long segment, int start, int length, int hash, int temp) {
            int bucket = hashBucket(hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                this.keys[bucket] = new Record(segment, start, length, hash)
                        .collect(temp);
                return;
            }

            if (!existing.equals(segment, start, length, hash)) {
                // collision, linear probing to find a slot
                while ((existing = this.keys[++bucket & BITMASK]) != null && !existing.equals(segment, start, length, hash)) {
                    // can be stuck here if all the buckets are full :(
                    // However, since the data set is max 10K (unique) this shouldn't happen
                    // So, I'm happily leave here branchless :)
                }
                if (existing == null) {
                    this.keys[bucket & BITMASK] = new Record(segment, start, length, hash)
                            .collect(temp);
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
                    this.keys[bucket & BITMASK] = key;
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

        void merge(RecordMap other) {
            other.forEach(this::putOrMerge);
        }

    }

    // One actor for one thread, no synchronization
    static class RegionActor {

        final FileChannel channel;
        final long startPos;
        final int size;
        final RecordMap map = new RecordMap();
        long segmentAddress;
        int position;
        Thread runner; // each actor has its own thread

        public RegionActor(FileChannel channel, long startPos, int size) {
            this.channel = channel;
            this.startPos = startPos;
            this.size = size;
        }

        void accumulate() {
            this.runner = new Thread(() -> {
                try {
                    // get the segment memory address, this is the only thing we need for Unsafe
                    this.segmentAddress = this.channel.map(FileChannel.MapMode.READ_ONLY, this.startPos, this.size, Arena.global()).address();
                }
                catch (IOException e) {
                    // no-op - skip intentionally, no handling for the purpose of this challenge
                }

                int start;
                int keyHash;
                int length;
                while (this.position < this.size) {
                    byte b;
                    start = this.position; // save line start position
                    keyHash = UNSAFE.getByte(this.segmentAddress + this.position++); // first byte is guaranteed not to be ';'
                    length = 1; // min key length
                    while ((b = UNSAFE.getByte(this.segmentAddress + this.position++)) != ';') { // read until semicolon
                        keyHash = calculateHash(keyHash, b); // calculate key hash ahead, eleminates one more loop later
                        length++;
                    }

                    final int temp = readTemperature();
                    this.map.putAndCollect(this.segmentAddress, start, length, keyHash, temp);

                    this.position++; // skip linebreak
                }
            });
            this.runner.start();
        }

        static int calculateHash(int hash, int b) {
            return 31 * hash + b;
        }

        // 1. Inspired by @yemreinci - Reading temparature value without Double.parse
        // 2. Inspired by @obourgain - Fetching first 4 bytes ahead, then masking
        int readTemperature() {
            int temp = 0;
            // read 4 bytes ahead
            final int first4 = UNSAFE.getInt(this.segmentAddress + this.position);
            this.position += 3;

            final byte b1 = (byte) first4; // first byte
            final byte b2 = (byte) ((first4 >> 8) & 0xFF); // second byte
            final byte b3 = (byte) ((first4 >> 16) & 0xFF); // third byte
            if (b1 == '-') {
                if (b3 == '.') {
                    temp -= 10 * (b2 - '0') + (byte) ((first4 >> 24) & 0xFF) - '0'; // fourth byte
                    this.position++;
                }
                else {
                    this.position++; // skip dot
                    temp -= 100 * (b2 - '0') + 10 * (b3 - '0') + UNSAFE.getByte(this.segmentAddress + this.position++) - '0'; // fifth byte
                }
            }
            else {
                if (b2 == '.') {
                    temp = 10 * (b1 - '0') + b3 - '0';
                }
                else {
                    temp = 100 * (b1 - '0') + 10 * (b2 - '0') + (byte) ((first4 >> 24) & 0xFF) - '0'; // fourth byte
                    this.position++;
                }
            }

            return temp;
        }

        /**
         * blocks until the map is fully collected
         */
        RecordMap get() throws InterruptedException {
            this.runner.join();
            return this.map;
        }
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    /**
     * Scans the given buffer to the left
     */
    private static long findClosestLineEnd(long start, int size, FileChannel channel) throws IOException {
        final long position = start + size;
        final long left = Math.max(position - 101, 0);
        final ByteBuffer buffer = ByteBuffer.allocate(101); // enough size to find at least one '\n'
        if (channel.read(buffer.clear(), left) != -1) {
            int bufferPos = buffer.position() - 1;
            while (buffer.get(bufferPos) != '\n') {
                bufferPos--;
                size--;
            }
        }
        return size;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        var concurrency = Runtime.getRuntime().availableProcessors();
        final long fileSize = Files.size(FILE);
        long regionSize = fileSize / concurrency;

        // handling extreme cases
        while (regionSize > Integer.MAX_VALUE) {
            concurrency *= 2;
            regionSize /= 2;
        }
        if (fileSize <= 1 << 20) { // small file (1mb), no need concurrency
            concurrency = 1;
            regionSize = fileSize;
        }

        long startPos = 0;
        final FileChannel channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ);
        final RegionActor[] actors = new RegionActor[concurrency];
        for (int i = 0; i < concurrency; i++) {
            // calculate boundaries
            long maxSize = (startPos + regionSize > fileSize) ? fileSize - startPos : regionSize;
            // shift position to back until we find a linebreak
            maxSize = findClosestLineEnd(startPos, (int) maxSize, channel);

            final RegionActor region = (actors[i] = new RegionActor(channel, startPos, (int) maxSize));
            region.accumulate();

            startPos += maxSize;
        }

        final RecordMap output = new RecordMap(); // output to merge all regions
        for (RegionActor actor : actors) {
            final RecordMap partial = actor.get(); // blocks until get the result
            output.merge(partial);
        }

        // sort and print the result
        final TreeMap<String, String> sorted = new TreeMap<>();
        output.forEach(key -> sorted.put(key.toString(), key.measurements()));
        System.out.println(sorted);

    }

}
