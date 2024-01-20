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
import java.util.ArrayList;
import java.util.List;
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

        private ResultRow(int v) {
            this.min = v;
            this.max = v;
            this.sum = v;
            this.count = 1;
        }

        public String toString() {
            return STR."\{round(min)}/\{round((double) (sum) / count)}/\{round(max)}";
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

            chunks[i] = new Chunk(mappedAddress + chunkStart, chunkLength + 1);
            // to skip the nl in the next chunk
            chunkStart += chunkLength + 1;
            chunkLength = Math.min(filebytes - chunkStart - 1, roughChunkSize);
            i++;
        }

        return chunks;
    }

    private static void run(Chunk chunk) {

        // can't have more than 10000 unique keys but want to match max hash
        final int MAPSIZE = 65536;
        final ResultRow[] slots = new ResultRow[MAPSIZE];
        final byte[][] keys = new byte[MAPSIZE][];

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
        ResultRow slotValue;

        while (i < cl) {
            start = i;
            hash = 0;

            word = UNSAFE.getLong(i);

            while ((hs = hasSemiColon(word)) == 0) {
                hash = (Long.rotateLeft(hash, 5) ^ word) * FXSEED; // fxhash
                i += 8;
                word = UNSAFE.getLong(i);
            }

            if (hs == 0x8000L || hs == -0x7FFFFFFFFFFF8000L)
                i++;
            else if (hs == 0x800000L)
                i += 2;
            else if (hs == 0x80000000L)
                i += 3;
            else if (hs == 0x8000000000L)
                i += 4;
            else if (hs == 0x800000000000L)
                i += 5;
            else if (hs == 0x80000000000000L)
                i += 6;
            else if (hs == 0x8000000000000000L)
                i += 7;

            // fxhash of what's left ((hs >>> 7) - 1) masks off the bytes from word that are before the semicolon
            hash = (Long.rotateLeft(hash, 5) ^ word & (hs >>> 7) - 1) * FXSEED;
            nameLength = (byte) (i++ - start);

            // temperature value follows
            c = UNSAFE.getByte(i++);
            // we know the val has to be between -99.9 and 99.8
            // always with a single fractional digit
            // represented as a byte array of either 4 or 5 characters
            if (c == 0x2D /* minus sign */) {
                // could be either n.x or nn.x
                if (UNSAFE.getByte(i + 3) == 0xA) {
                    temp = (UNSAFE.getByte(i++) - 48) * 10; // char 1
                }
                else {
                    temp = (UNSAFE.getByte(i++) - 48) * 100; // char 1
                    temp += (UNSAFE.getByte(i++) - 48) * 10; // char 2
                }
                i++; // skip dot
                temp += (UNSAFE.getByte(i++) - 48); // char 2
                temp = -temp;
            }
            else {
                // could be either n.x or nn.x
                if (UNSAFE.getByte(i + 2) == 0xA) {
                    temp = (c - 48) * 10; // char 1
                }
                else {
                    temp = (c - 48) * 100; // char 1
                    temp += (UNSAFE.getByte(i++) - 48) * 10; // char 2
                }
                i++; // skip dot
                temp += (UNSAFE.getByte(i++) - 48); // char 3
            }

            i++;// nl
            // xor folding
            slot = (int) (hash ^ hash >> 32) & 65535;

            // Linear probe for open slot
            while (slots[slot] != null && nameLength != keys[slot].length && !unsafeEquals(keys[slot], start, nameLength)) {
                slot = ++slot % MAPSIZE;
            }
            slotValue = slots[slot];

            // existing
            if (slotValue != null) {
                slotValue.min = Math.min(slotValue.min, temp);
                slotValue.max = Math.max(slotValue.max, temp);
                slotValue.sum += temp;
                slotValue.count++;
            }
            else {
                // new value
                slots[slot] = new ResultRow(temp);
                byte[] bytes = new byte[nameLength];
                // copy the name bytes
                UNSAFE.copyMemory(null, start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameLength);
                keys[slot] = bytes;
            }
        }

        // merge results with overall results
        final List<MapEntry> result = new ArrayList<>();
        for (int j = 0; j < slots.length; j++) {
            slotValue = slots[j];
            if (slotValue != null) {
                result.add(new MapEntry(new String(keys[j], StandardCharsets.UTF_8), slotValue));
            }
        }

        lock.lock();
        try {
            ResultRow rr;
            ResultRow lr;
            for (MapEntry me : result) {
                lr = me.row;
                if ((rr = global.get(me.key)) != null) {
                    rr.min = Math.min(rr.min, lr.min);
                    rr.max = Math.max(rr.max, lr.max);
                    rr.count += lr.count;
                    rr.sum += lr.sum;
                }
                else {
                    global.put(me.key, lr);
                }
            }
        }
        finally {
            lock.unlock();
        }

    }

    static boolean unsafeEquals(final byte[] a, final long b_address, final short length) {
        // byte by byte comparisons are slow, so do as big chunks as possible
        final int baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;

        short i = 0;
        // round down to nearest power of 8
        for (; i < (length & -8); i += 8) {
            if (UNSAFE.getLong(a, i + baseOffset) != UNSAFE.getLong(b_address + i)) {
                return false;
            }
        }
        // leftover ints
        for (; i < (length - i & -4); i += 4) {
            if (UNSAFE.getInt(a, i + baseOffset) != UNSAFE.getInt(b_address + i)) {
                return false;
            }
        }
        // leftover shorts
        for (; i < (length - i & -2); i += 2) {
            if (UNSAFE.getShort(a, i + baseOffset) != UNSAFE.getShort(b_address + i)) {
                return false;
            }
        }
        // leftover bytes
        for (; i < length - i; i++) {
            if (UNSAFE.getByte(a, i + baseOffset) != UNSAFE.getByte(b_address + i)) {
                return false;
            }
        }

        return true;
    }

    private static long hasSemiColon(final long n) {
        // long filled just with semicolon
        // taken from https://graphics.stanford.edu/~seander/bithacks.html#ValueInWord
        final long v = n ^ ALL_SEMIS;
        return (v - 0x0101010101010101L) & (~v & 0x8080808080808080L);
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
            threads[i] = thread;
            thread.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        System.out.println(global);
    }

    record MapEntry(String key, ResultRow row) {
    }
}
