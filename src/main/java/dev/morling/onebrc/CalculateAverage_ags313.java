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
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_ags313 {

    private static final int THREAD_COUNT = 8;
    private static final int FUTURE_BUFFER = 1024;
    private static final int ALLOCATION = 128 * 1024 * 1024;

    // private static final String FILE = "./measurementsCut.txt";
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./src/test/resources/samples/measurements-1.txt";
    // private static final String FILE = "./src/test/resources/samples/measurements-20.txt";

    private static final int NAME_LENGTH_LIMIT_BYTES = 128;
    private static final int BUFFER_SIZE = 108;
    private static final ExecutorService exec = Executors.newFixedThreadPool(THREAD_COUNT);

    /**
     *  1B rows
     *  8s  multithreaded
     *  44s single threaded
     *
     *  ideas:
     *  1) replace hashmap with something faster(er)
     *  2) play with graal
     **/

    private static class Key implements Comparable<Key> {
        private final byte[] value = new byte[NAME_LENGTH_LIMIT_BYTES];
        private int hashCode;
        private int length = 0;

        // https://stackoverflow.com/questions/20952739/how-would-you-convert-a-string-to-a-64-bit-integer
        public void accept(byte b) {
            value[length] = b;
            length += 1;
            hashCode = hashCode * 10191 + b;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that)
                return true;
            if (that == null)
                return false;
            Key key = (Key) that; // not checking class, nothing else uses this
            if (hashCode != key.hashCode)
                return false;
            if (length != key.length)
                return false;
            for (int i = 0; i < length; i++) {
                if (UNSAFE.getByte(value, i) != UNSAFE.getByte(key.value, i)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return new String(value, 0, length);
        }

        void reset() {
            length = 0;
            hashCode = 0;
        }

        @Override
        public int compareTo(Key other) {
            return toString().compareTo(other.toString());
        }
    }

    private static HashMap<Key, Stats> readChunk(FileChannel from, long start, int bytesToRead) throws IOException {
        HashMap<Key, Stats> result = new HashMap<>(1024 * 16);

        var bbuffer = from.map(FileChannel.MapMode.READ_ONLY, start, bytesToRead);

        var key = new Key();
        while (bbuffer.remaining() > 1) {
            key.reset();
            boolean negative = false;
            var temperature = 0;

            while (bbuffer.remaining() > 0) {
                byte b = bbuffer.get();
                if (b == ';') {
                    break;
                }
                key.accept(b);
            }

            loop2: while (bbuffer.remaining() > 0) {
                byte b = bbuffer.get();
                switch (b) {
                    case '-':
                        negative = true;
                        break;
                    case '.':
                        temperature = (temperature * 10) + (bbuffer.get() - '0'); // single decimal
                        break;
                    case '\n':
                        break loop2;
                    default:
                        temperature = (temperature * 10) + (b - '0');
                }
            }

            int measure = negative ? -temperature : temperature;
            Stats stats = result.computeIfAbsent(key, c -> new Stats());
            if (stats.count == 0) {
                key = new Key();
            }

            stats.count++;
            stats.total += measure;
            if (measure < stats.min) {
                stats.min = measure;
            }
            if (measure > stats.max) {
                stats.max = measure;
            }
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        var channel = new RandomAccessFile(FILE, "r").getChannel();
        long totalToRead = channel.size();

        Future<HashMap<Key, Stats>>[] futures = new Future[FUTURE_BUFFER];

        long allocated = 0;
        int chunkCounter = 0;
        while (allocated < totalToRead) {
            var start = allocated;
            var bytesToReadInPass = Math.min(totalToRead - allocated, ALLOCATION);

            if (bytesToReadInPass < BUFFER_SIZE) {
                // System.out.println("Want to read: " + bytesToReadInPass + ", starting buffer at: " + startBufferAt);
                // System.out.println("Total: " + totalToRead + ", allocated: " + allocated + ", allocating: " + (correctedBytesToRead));
                futures[chunkCounter++] = exec.submit(() -> readChunk(channel, start, (int) bytesToReadInPass));
                allocated += bytesToReadInPass;
            }
            else {
                var startBufferAt = Math.max(0, allocated + bytesToReadInPass - BUFFER_SIZE);
                var buffer = channel.map(FileChannel.MapMode.READ_ONLY, startBufferAt, BUFFER_SIZE);

                // System.out.println("Want to read: " + bytesToReadInPass + ", starting buffer at: " + startBufferAt);

                var endOfLine = 0;
                for (int i = 0; i < BUFFER_SIZE; i++) {
                    if (buffer.get() == '\n') {
                        endOfLine = i;
                        break;
                    }
                }
                long correctedBytesToRead = bytesToReadInPass - BUFFER_SIZE + endOfLine;

                // System.out.println("Total: " + totalToRead + ", allocated: " + allocated + ", allocating: " + (correctedBytesToRead));
                futures[chunkCounter++] = exec.submit(() -> readChunk(channel, start, (int) correctedBytesToRead));
                allocated += correctedBytesToRead + 1;
            }
        }

        var accumulator = new HashMap<Key, Stats>(1024 * 16);
        for (int i = 0; i < chunkCounter; i++) {
            var aMap = futures[i].get();
            aMap.forEach((key, value) -> accumulator.computeIfAbsent(key, __ -> new Stats()).merge(value));
        }

        exec.shutdown();

        System.out.println(new TreeMap(accumulator));
    }

    static class Stats {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long total;
        private int count;

        Stats() {
        }

        @Override
        public String toString() {
            return BigDecimal.valueOf(min / (10.0)).setScale(1, RoundingMode.HALF_UP) + "/"
                    + BigDecimal.valueOf(total / (10.0 * count)).setScale(1, RoundingMode.HALF_UP) + '/'
                    + BigDecimal.valueOf(max / (10.0)).setScale(1, RoundingMode.HALF_UP);
        }

        void merge(Stats that) {
            max = Math.max(this.max, that.max);
            min = Math.min(this.min, that.min);
            total += that.total;
            count += that.count;
        }
    }

    private static final Unsafe UNSAFE = unsafe();

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
