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
import java.util.concurrent.ConcurrentHashMap;

public class CalculateAverage_JamalMulla {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

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

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ResultRow) obj;
            return Double.doubleToLongBits(this.min) == Double.doubleToLongBits(that.min) &&
                    Double.doubleToLongBits(this.sum) == Double.doubleToLongBits(that.sum) &&
                    Double.doubleToLongBits(this.max) == Double.doubleToLongBits(that.max);
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, sum, max);
        }

    }

    private record Chunk(Long start, Long length) {
    }

    static List<Chunk> getChunks(int numThreads, FileChannel channel) throws IOException {
        // get all chunk boundaries
        long filebytes = channel.size();
        long roughChunkSize = filebytes / numThreads;
        List<Chunk> chunks = new ArrayList<>();
        long mappedAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, filebytes, Arena.global()).address();
        // System.out.println("filebytes:" + filebytes + " roughsize: " + roughChunkSize + " numthreads: " + numThreads);

        long chunkStart = 0;
        long chunkLength = Math.min(filebytes - chunkStart - 1, roughChunkSize);
        while (chunkStart < filebytes) {
            // unlikely we need to read more than this many bytes to find the next newline
            // System.out.println("Chunk start: " + chunkStart + " chunkLength: " + chunkLength);
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
        // System.out.println(chunks);
        // for the last chunk, we can set it to what's left
        // chunks.add(new Chunk(chunkStart, filebytes - chunkStart));
        return chunks;
    }

    private static int fnv(final byte[] bytes, int length) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < length; i++) {
            hash ^= bytes[i];
            hash *= 0x01000193;
        }
        return ((hash >> 16) ^ hash) & 65535;
    }

    private static class CalculateTask implements Runnable {

        private final FileChannel channel;
        private final SimplerHashMap results;
        private final Map<String, ResultRow> global;
        private final Chunk chunk;

        public CalculateTask(FileChannel fileChannel, Map<String, ResultRow> global, Chunk chunk) {
            this.channel = fileChannel;
            this.results = new SimplerHashMap();
            this.global = global;
            this.chunk = chunk;
        }

        @Override
        public void run() {
            // no names bigger than this
            byte[] nameBytes = new byte[100];
            short nameIndex = 0;
            int ot;
            int hash = 0x811c9dc5;

            long i = chunk.start;
            final long cl = chunk.start + chunk.length;
            while (i < cl) {
                byte c;
                while ((c = UNSAFE.getByte(i++)) != 0x3B) {
                    nameBytes[nameIndex++] = c;
                    hash ^= c;
                    hash *= 0x01000193;
                }

                // the temp
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
                hash = ((hash >> 16) ^ hash) & 65535;
                results.putOrMerge(nameBytes, nameIndex, hash, ot);
                // reset
                nameIndex = 0;
                hash = 0x811c9dc5;
            }

            // merge results with overall results
            for (MapEntry me : results.getAll()) {
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
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, ResultRow> results = new ConcurrentHashMap<>();

        RandomAccessFile raFile = new RandomAccessFile(FILE, "r");
        FileChannel channel = raFile.getChannel();
        int numThreads = 1;
        if (channel.size() > 64000) {
            numThreads = Runtime.getRuntime().availableProcessors();
        }
        List<Chunk> chunks = getChunks(numThreads, channel);
        List<Thread> threads = new ArrayList<>();
        for (Chunk chunk : chunks) {
            Thread t = new Thread(new CalculateTask(channel, results, chunk));
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }
        // just to sort
        System.out.println(new TreeMap<>(results));
    }

    record MapEntry(String key, ResultRow row) {
    }

    static class SimplerHashMap {
        // based on spullara'ss
        // can't have more than 10000 unique keys butwant to match max hash
        int MAPSIZE = 65536;
        ResultRow[] slots = new ResultRow[MAPSIZE];
        byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(byte[] key, int length, int hash, int temp) {
            int slot = hash;
            ResultRow slotValue = slots[slot];

            // Linear probe for open slot
            while (slotValue != null && (keys[slot].length != length || !unsafeEquals(keys[slot], key, length))) {
                slotValue = slots[++slot];
            }
            if (slotValue == null) {
                slots[slot] = new ResultRow(temp);
                byte[] bytes = new byte[length];
                System.arraycopy(key, 0, bytes, 0, length);
                keys[slot] = bytes;
            }
            else {
                slotValue.min = Math.min(slotValue.min, temp);
                slotValue.max = Math.max(slotValue.max, temp);
                slotValue.sum += temp;
                slotValue.count++;
            }
        }

        static boolean unsafeEquals(final byte[] a, final byte[] b, final int length) {
            int baseOffset = UNSAFE.arrayBaseOffset(byte[].class);
            for (int i = 0; i < length; i++) {
                if (UNSAFE.getByte(a, i + baseOffset) != UNSAFE.getByte(b, i + baseOffset)) {
                    return false;
                }
            }
            return true;
        }

        // Get all pairs
        public List<MapEntry> getAll() {
            List<MapEntry> result = new ArrayList<>(slots.length);
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
