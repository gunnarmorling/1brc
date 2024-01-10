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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateAverage_JamalMulla {

    private static final Map<String, ResultRow> global = new HashMap<>();
    private static final String FILE = "./measurements.txt";
    private static final Unsafe UNSAFE = initUnsafe();
    private static final Lock lock = new ReentrantLock();
    private static final int FNV_32_INIT = 0x811c9dc5;
    private static final int FNV_32_PRIME = 0x01000193;

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
            return round(min) + "/" + round((double) (sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private record Chunk(Long start, Long length) {
    }

    static List<Chunk> getChunks(int numThreads, FileChannel channel) throws IOException {
        // get all chunk boundaries
        final long filebytes = channel.size();
        final long roughChunkSize = filebytes / numThreads;
        final List<Chunk> chunks = new ArrayList<>(numThreads);
        final long mappedAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, filebytes, Arena.global()).address();
        long chunkStart = 0;
        long chunkLength = Math.min(filebytes - chunkStart - 1, roughChunkSize);
        while (chunkStart < filebytes) {
            // unlikely we need to read more than this many bytes to find the next newline
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, chunkStart + chunkLength,
                    Math.min(Math.min(filebytes - chunkStart - chunkLength, chunkLength), 100));

            while (mbb.get() != 0xA /* \n */) {
                chunkLength++;
            }

            chunks.add(new Chunk(mappedAddress + chunkStart, chunkLength + 1));
            // to skip the nl in the next chunk
            chunkStart += chunkLength + 1;
            chunkLength = Math.min(filebytes - chunkStart - 1, roughChunkSize);
        }
        return chunks;
    }

    private static class CalculateTask implements Runnable {

        private final SimplerHashMap results;
        private final Chunk chunk;

        public CalculateTask(Chunk chunk) {
            this.results = new SimplerHashMap();
            this.chunk = chunk;
        }

        @Override
        public void run() {
            // no names bigger than this
            final byte[] nameBytes = new byte[100];
            short nameIndex = 0;
            int ot;
            // fnv hash
            int hash = FNV_32_INIT;

            long i = chunk.start;
            final long cl = chunk.start + chunk.length;
            while (i < cl) {
                byte c;
                while ((c = UNSAFE.getByte(i++)) != 0x3B /* semi-colon */) {
                    nameBytes[nameIndex++] = c;
                    hash ^= c;
                    hash *= FNV_32_PRIME;
                }

                // temperature value follows
                c = UNSAFE.getByte(i++);
                // we know the val has to be between -99.9 and 99.8
                // always with a single fractional digit
                // represented as a byte array of either 4 or 5 characters
                if (c == 0x2D /* minus sign */) {
                    // could be either n.x or nn.x
                    if (UNSAFE.getByte(i + 3) == 0xA) {
                        ot = (UNSAFE.getByte(i++) - 48) * 10; // char 1
                    }
                    else {
                        ot = (UNSAFE.getByte(i++) - 48) * 100; // char 1
                        ot += (UNSAFE.getByte(i++) - 48) * 10; // char 2
                    }
                    i++; // skip dot
                    ot += (UNSAFE.getByte(i++) - 48); // char 2
                    ot = -ot;
                }
                else {
                    // could be either n.x or nn.x
                    if (UNSAFE.getByte(i + 2) == 0xA) {
                        ot = (c - 48) * 10; // char 1
                    }
                    else {
                        ot = (c - 48) * 100; // char 1
                        ot += (UNSAFE.getByte(i++) - 48) * 10; // char 2
                    }
                    i++; // skip dot
                    ot += (UNSAFE.getByte(i++) - 48); // char 3
                }

                i++;// nl
                hash &= 65535;
                results.putOrMerge(nameBytes, nameIndex, hash, ot);
                // reset
                nameIndex = 0;
                hash = 0x811c9dc5;
            }

            // merge results with overall results
            List<MapEntry> all = results.getAll();
            lock.lock();
            try {
                for (MapEntry me : all) {
                    ResultRow rr;
                    ResultRow lr = me.row;
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
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        FileChannel channel = new RandomAccessFile(FILE, "r").getChannel();
        int numThreads = 1;
        if (channel.size() > 64000) {
            numThreads = Runtime.getRuntime().availableProcessors();
        }
        List<Chunk> chunks = getChunks(numThreads, channel);
        List<Thread> threads = new ArrayList<>();
        for (Chunk chunk : chunks) {
            Thread thread = new Thread(new CalculateTask(chunk));
            thread.setPriority(Thread.MAX_PRIORITY);
            thread.start();
            threads.add(thread);
        }
        for (Thread t : threads) {
            t.join();
        }
        // create treemap just to sort
        System.out.println(new TreeMap<>(global));
    }

    record MapEntry(String key, ResultRow row) {
    }

    static class SimplerHashMap {
        // can't have more than 10000 unique keys but want to match max hash
        final int MAPSIZE = 65536;
        final ResultRow[] slots = new ResultRow[MAPSIZE];
        final byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(final byte[] key, final short length, final int hash, final int temp) {
            int slot = hash;
            ResultRow slotValue;

            // Linear probe for open slot
            while ((slotValue = slots[slot]) != null && (keys[slot].length != length || !unsafeEquals(keys[slot], key, length))) {
                slot++;
            }

            // existing
            if (slotValue != null) {
                slotValue.min = Math.min(slotValue.min, temp);
                slotValue.max = Math.max(slotValue.max, temp);
                slotValue.sum += temp;
                slotValue.count++;
                return;
            }

            // new value
            slots[slot] = new ResultRow(temp);
            byte[] bytes = new byte[length];
            System.arraycopy(key, 0, bytes, 0, length);
            keys[slot] = bytes;
        }

        static boolean unsafeEquals(final byte[] a, final byte[] b, final short length) {
            // byte by byte comparisons are slow, so do as big chunks as possible
            final int baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;

            short i = 0;
            // round down to nearest power of 8
            for (; i < (length & -8); i += 8) {
                if (UNSAFE.getLong(a, i + baseOffset) != UNSAFE.getLong(b, i + baseOffset)) {
                    return false;
                }
            }
            if (i == length) {
                return true;
            }
            // leftover ints
            for (; i < (length - i & -4); i += 4) {
                if (UNSAFE.getInt(a, i + baseOffset) != UNSAFE.getInt(b, i + baseOffset)) {
                    return false;
                }
            }
            if (i == length) {
                return true;
            }
            // leftover shorts
            for (; i < (length - i & -2); i += 2) {
                if (UNSAFE.getShort(a, i + baseOffset) != UNSAFE.getShort(b, i + baseOffset)) {
                    return false;
                }
            }
            if (i == length) {
                return true;
            }
            // leftover bytes
            for (; i < (length - i); i++) {
                if (UNSAFE.getByte(a, i + baseOffset) != UNSAFE.getByte(b, i + baseOffset)) {
                    return false;
                }
            }

            return true;
        }

        // Get all pairs
        public List<MapEntry> getAll() {
            final List<MapEntry> result = new ArrayList<>(slots.length);
            for (int i = 0; i < slots.length; i++) {
                ResultRow slotValue = slots[i];
                if (slotValue != null) {
                    result.add(new MapEntry(new String(keys[i], StandardCharsets.UTF_8), slotValue));
                }
            }
            return result;
        }
    }

}
