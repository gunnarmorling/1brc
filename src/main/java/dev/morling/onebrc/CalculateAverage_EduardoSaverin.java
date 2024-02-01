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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.READ;

public class CalculateAverage_EduardoSaverin {
    private static final Path FILE = Path.of("./measurements.txt");
    private static final int NO_OF_THREADS = Runtime.getRuntime().availableProcessors();
    private static final Unsafe UNSAFE = initUnsafe();
    private static final int FNV_32_OFFSET = 0x811c9dc5;
    private static final int FNV_32_PRIME = 0x01000193;
    private static final Map<String, ResultRow> resultRowMap = new HashMap<>();
    private static final Lock lock = new ReentrantLock();

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

    public record Chunk(long start, long length) {
    }

    record MapEntry(String key, ResultRow row) {
    }

    private static final class ResultRow {
        private double min;
        private double max;
        private double sum;
        private int count;

        private ResultRow(double v) {
            this.min = v;
            this.max = v;
            this.sum = v;
            this.count = 1;
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    /**
     * 0xA - Represents New Line
     *
     * @param fileChannel
     * @return
     * @throws IOException
     */
    static List<Chunk> getChunks(FileChannel fileChannel) throws IOException {
        int numThreads = 1;
        if (fileChannel.size() > 64000) {
            numThreads = NO_OF_THREADS;
        }
        final long fileBytes = fileChannel.size();
        final long chunkSize = fileBytes / numThreads;
        final List<Chunk> chunks = new ArrayList<>(numThreads);
        final long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileBytes, Arena.global()).address();
        long chunkStart = 0;
        // Ensures that the chunk size does not exceed the remaining bytes in the file.
        long chunkLength = Math.min(fileBytes - chunkStart - 1, chunkSize);
        while (chunkStart < fileBytes) {
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStart + chunkLength,
                    Math.min(Math.min(fileBytes - chunkStart - chunkLength, chunkLength), 100));
            // Until \n found
            while (mappedByteBuffer.get() != 0xA) {
                chunkLength++;
            }
            chunks.add(new Chunk(mappedAddress + chunkStart, chunkLength + 1));
            chunkStart += (chunkLength + 1);
            chunkLength = Math.min(fileBytes - chunkStart - 1, chunkSize);
        }
        return chunks;
    }

    static class SimplerHashMap {
        final int MAPSIZE = 65536;
        final ResultRow[] slots = new ResultRow[MAPSIZE];
        final byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(final byte[] key, final short length, final int hash, final int temp) {
            int slot = hash;
            ResultRow slotValue;

            // Doing Linear Probing if Collision
            while ((slotValue = slots[slot]) != null && (keys[slot].length != length || !unsafeEquals(keys[slot], key, length))) {
                slot++;
            }

            // Existing Key
            if (slotValue != null) {
                slotValue.min = Math.min(slotValue.min, temp);
                slotValue.max = Math.max(slotValue.max, temp);
                slotValue.sum += temp;
                slotValue.count++;
                return;
            }

            // New Key
            slots[slot] = new ResultRow(temp);
            byte[] bytes = new byte[length];
            System.arraycopy(key, 0, bytes, 0, length);
            keys[slot] = bytes;
        }

        static boolean unsafeEquals(final byte[] a, final byte[] b, final short length) {
            // byte by byte comparisons are slow, so do as big chunks as possible
            final int baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;

            short i = 0;
            // Double
            for (; i < (length & -8); i += 8) {
                if (UNSAFE.getDouble(a, i + baseOffset) != UNSAFE.getDouble(b, i + baseOffset)) {
                    return false;
                }
            }

            // Long
            for (; i < (length & -8); i += 8) {
                if (UNSAFE.getLong(a, i + baseOffset) != UNSAFE.getLong(b, i + baseOffset)) {
                    return false;
                }
            }
            if (i == length) {
                return true;
            }
            // Int
            for (; i < (length - i & -4); i += 4) {
                if (UNSAFE.getInt(a, i + baseOffset) != UNSAFE.getInt(b, i + baseOffset)) {
                    return false;
                }
            }
            if (i == length) {
                return true;
            }
            // Short
            for (; i < (length - i & -2); i += 2) {
                if (UNSAFE.getShort(a, i + baseOffset) != UNSAFE.getShort(b, i + baseOffset)) {
                    return false;
                }
            }
            if (i == length) {
                return true;
            }
            // Byte
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

    private static class Task implements Runnable {

        private final SimplerHashMap results;
        private final Chunk chunk;

        public Task(Chunk chunk) {
            this.results = new SimplerHashMap();
            this.chunk = chunk;
        }

        @Override
        public void run() {
            // Max length of any city name
            final byte[] nameBytes = new byte[100];
            short nameIndex = 0;
            int ot;
            int hash = FNV_32_OFFSET;

            long i = chunk.start;
            final long cl = chunk.start + chunk.length;
            while (i < cl) {
                byte c;
                // 0x3B is ;
                while ((c = UNSAFE.getByte(i++)) != 0x3B) {
                    nameBytes[nameIndex++] = c;
                    // FNV-1a hash : https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
                    hash ^= c;
                    hash *= FNV_32_PRIME;
                }

                // Temperature just after Semicolon
                c = UNSAFE.getByte(i++);
                // 0x2D is Minus(-)
                // Below you will see -48 which is used to convert from ASCII to Integer, 48 represents 0 in ASCII
                if (c == 0x2D) {
                    // X.X or XX.X
                    if (UNSAFE.getByte(i + 3) == 0xA) {
                        ot = (UNSAFE.getByte(i++) - 48) * 10;
                    }
                    else {
                        ot = (UNSAFE.getByte(i++) - 48) * 100;
                        ot += (UNSAFE.getByte(i++) - 48) * 10;
                    }
                    // Now dot
                    i++; // Skipping Dot
                    ot += (UNSAFE.getByte(i++) - 48);
                    // Make Number Negative Since we detected (-) sign
                    ot = -ot;
                }
                else {
                    // X.X or XX.X
                    if (UNSAFE.getByte(i + 2) == 0xA) {
                        ot = (c - 48) * 10;
                    }
                    else {
                        ot = (c - 48) * 100;
                        ot += (UNSAFE.getByte(i++) - 48) * 10;
                    }
                    // Now dot
                    i++; // Skipping Dot
                    // Number after dot
                    ot += (UNSAFE.getByte(i++) - 48);
                }
                // Since Parsed Line, Next thing must be newline
                i++;
                hash &= 65535;
                results.putOrMerge(nameBytes, nameIndex, hash, ot);
                // Reset
                nameIndex = 0;
                hash = FNV_32_OFFSET;
            }
            List<MapEntry> all = results.getAll();
            lock.lock();
            try {
                for (MapEntry me : all) {
                    ResultRow rr;
                    ResultRow lr = me.row;
                    if ((rr = resultRowMap.get(me.key)) != null) {
                        rr.min = Math.min(rr.min, lr.min);
                        rr.max = Math.max(rr.max, lr.max);
                        rr.count += lr.count;
                        rr.sum += lr.sum;
                    }
                    else {
                        resultRowMap.put(me.key, lr);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(FILE, READ);
        List<Chunk> chunks = getChunks(fileChannel);
        List<Thread> threads = new ArrayList<>();
        for (Chunk chunk : chunks) {
            Thread thread = new Thread(new Task(chunk));
            thread.setPriority(Thread.MAX_PRIORITY); // Make this thread of highest priority
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println(new TreeMap<>(resultRowMap));
    }
}
