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
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateAverage_JamalMulla {

    private static final long ALL_SEMIS = 0x3B3B3B3B3B3B3B3BL;
    private static final Map<String, ResultRow> global = new TreeMap<>();
    private static final String FILE = "./measurements.txt";
    private static final Unsafe UNSAFE = initUnsafe();
    private static final Lock lock = new ReentrantLock();
    private static final long FXSEED = 0x517cc1b727220a95L;

    private static final long[] masks = {
            0x0,
            0x00000000000000FFL,
            0x000000000000FFFFL,
            0x0000000000FFFFFFL,
            0x00000000FFFFFFFFL,
            0x000000FFFFFFFFFFL,
            0x0000FFFFFFFFFFFFL,
            0x00FFFFFFFFFFFFFFL
    };

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class ResultRow {
        private int min;
        private int max;
        private long sum;
        private int count;
        private final long keyStart;
        private final byte keyLength;

        private ResultRow(int v, final long keyStart, final byte keyLength) {
            this.min = v;
            this.max = v;
            this.sum = v;
            this.count = 1;
            this.keyStart = keyStart;
            this.keyLength = keyLength;
        }

        public String toString() {
            return round(min) + "/" + round((double) (sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }

    }

    private record Chunk(Long start, Long length) {
    }

    static Chunk[] getChunks(int numThreads, FileChannel channel) throws IOException {
        // get all chunk boundaries
        final long filebytes = channel.size();
        final long roughChunkSize = filebytes / numThreads;
        final Chunk[] chunks = new Chunk[numThreads];
        final long mappedAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, filebytes, Arena.global()).address();
        long chunkStart = 0;
        long chunkLength = Math.min(filebytes - chunkStart - 1, roughChunkSize);
        int i = 0;
        while (chunkStart < filebytes) {
            while (UNSAFE.getByte(mappedAddress + chunkStart + chunkLength) != 0xA /* \n */) {
                chunkLength++;
            }

            chunks[i++] = new Chunk(mappedAddress + chunkStart, chunkLength + 1);
            // to skip the nl in the next chunk
            chunkStart += chunkLength + 1;
            chunkLength = Math.min(filebytes - chunkStart - 1, roughChunkSize);
        }

        return chunks;
    }

    private static void run(Chunk chunk) {

        // can't have more than 10000 unique keys but want to match max hash
        final int MAPSIZE = 65536;
        final ResultRow[] slots = new ResultRow[MAPSIZE];

        byte nameLength;
        int temp;
        long hash;

        long i = chunk.start;
        final long cl = chunk.start + chunk.length;
        long word;
        long hs;
        long start;
        byte c;
        int slot;
        long n;
        ResultRow slotValue;

        while (i < cl) {
            start = i;
            hash = 0;

            word = UNSAFE.getLong(i);

            while (true) {
                n = word ^ ALL_SEMIS;
                hs = (n - 0x0101010101010101L) & (~n & 0x8080808080808080L);
                if (hs != 0)
                    break;
                hash = (hash ^ word) * FXSEED;
                i += 8;
                word = UNSAFE.getLong(i);
            }

            i += Long.numberOfTrailingZeros(hs) >> 3;

            // hash of what's left ((hs >>> 7) - 1) masks off the bytes from word that are before the semicolon
            hash = (hash ^ word & (hs >>> 7) - 1) * FXSEED;
            nameLength = (byte) (i++ - start);

            // temperature value follows
            c = UNSAFE.getByte(i++);
            // we know the val has to be between -99.9 and 99.8
            // always with a single fractional digit
            // represented as a byte array of either 4 or 5 characters
            if (c != 0x2D /* minus sign */) {
                // could be either n.x or nn.x
                if (UNSAFE.getByte(i + 2) == 0xA) {
                    temp = (c - 48) * 10; // char 1
                }
                else {
                    temp = (c - 48) * 100; // char 1
                    temp += (UNSAFE.getByte(i++) - 48) * 10; // char 2
                }
                temp += (UNSAFE.getByte(++i) - 48); // char 3
            }
            else {
                // could be either n.x or nn.x
                if (UNSAFE.getByte(i + 3) == 0xA) {
                    temp = (UNSAFE.getByte(i) - 48) * 10; // char 1
                    i += 2;
                }
                else {
                    temp = (UNSAFE.getByte(i) - 48) * 100; // char 1
                    temp += (UNSAFE.getByte(i + 1) - 48) * 10; // char 2
                    i += 3;
                }
                temp += (UNSAFE.getByte(i) - 48); // char 2
                temp = -temp;
            }
            i += 2;

            // xor folding
            slot = (int) (hash ^ hash >> 32) & 65535;

            // Linear probe for open slot
            while ((slotValue = slots[slot]) != null && (slotValue.keyLength != nameLength || !unsafeEquals(slotValue.keyStart, start, nameLength))) {
                slot = (slot + 1) % MAPSIZE;
            }

            // existing
            if (slotValue != null) {
                slotValue.sum += temp;
                slotValue.count++;
                if (temp > slotValue.max) {
                    slotValue.max = temp;
                    continue;
                }
                if (temp < slotValue.min)
                    slotValue.min = temp;

            }
            else {
                // new value
                slots[slot] = new ResultRow(temp, start, nameLength);
            }
        }

        // merge results with overall results
        ResultRow rr;
        String key;
        byte[] bytes;
        lock.lock();
        try {
            for (ResultRow resultRow : slots) {
                if (resultRow != null) {
                    bytes = new byte[resultRow.keyLength];
                    // copy the name bytes
                    UNSAFE.copyMemory(null, resultRow.keyStart, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, resultRow.keyLength);
                    key = new String(bytes, StandardCharsets.UTF_8);
                    if ((rr = global.get(key)) != null) {
                        rr.min = Math.min(rr.min, resultRow.min);
                        rr.max = Math.max(rr.max, resultRow.max);
                        rr.count += resultRow.count;
                        rr.sum += resultRow.sum;
                    }
                    else {
                        global.put(key, resultRow);
                    }
                }
            }
        }
        finally {
            lock.unlock();
        }

    }

    static boolean unsafeEquals(final long a_address, final long b_address, final byte b_length) {
        // byte by byte comparisons are slow, so do as big chunks as possible
        byte i = 0;
        for (; i < (b_length & -8); i += 8) {
            if (UNSAFE.getLong(a_address + i) != UNSAFE.getLong(b_address + i)) {
                return false;
            }
        }
        if (i == b_length)
            return true;
        return (UNSAFE.getLong(a_address + i) & masks[b_length - i]) == (UNSAFE.getLong(b_address + i) & masks[b_length - i]);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int numThreads = 1;
        FileChannel channel = new RandomAccessFile(FILE, "r").getChannel();
        if (channel.size() > 64000) {
            numThreads = Runtime.getRuntime().availableProcessors();
        }
        Chunk[] chunks = getChunks(numThreads, channel);
        Thread[] threads = new Thread[chunks.length];
        for (int i = 0; i < chunks.length; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> run(chunks[finalI]));
            thread.setPriority(Thread.MAX_PRIORITY);
            thread.start();
            threads[i] = thread;
        }
        for (Thread t : threads) {
            t.join();
        }
        System.out.println(global);
        channel.close();
    }
}
