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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CalculateAverage_JamalMulla {

    private static final String FILE = "./measurements.txt";

    private static final class ResultRow {
        private double min;
        private double max;

        private double sum;
        private long count;

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
            return Math.round(value * 10.0) / 10.0;
        }

        public double min() {
            return min;
        }

        public double mean() {
            return sum / count;
        }

        public double max() {
            return max;
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

        long chunkStart = 0;
        long chunkLength = roughChunkSize;
        for (int i = 0; i < numThreads - 1; i++) {
            // unlikely we need to read more than this many bytes to find the next newline
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, chunkStart + chunkLength, 100);
            while (mbb.get() != 0xA /* \n */) {
                chunkLength++;
            }

            chunks.add(new Chunk(chunkStart, chunkLength + 1));
            // to skip the nl in the next chunk
            chunkStart += chunkLength + 1;
            chunkLength = roughChunkSize;
        }
        // for the last chunk, we can set it to what's left
        chunks.add(new Chunk(chunkStart, filebytes - chunkStart));
        return chunks;
    }

    private static int fnv(byte[] bytes, int length) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < length; i++) {
            hash ^= bytes[i];
            hash *= 0x01000193;
        }
        return hash;
    }

    private static class CalculateTask implements Runnable {

        private final FileChannel channel;
        private final Map<String, ResultRow> results;
        private final Map<String, ResultRow> global;
        private final Chunk chunk;

        public CalculateTask(FileChannel fileChannel, Map<String, ResultRow> global, Chunk chunk) {
            this.channel = fileChannel;
            this.results = new HashMap<>();
            this.global = global;
            this.chunk = chunk;
        }

        @Override
        public void run() {
            // no names bigger than this
            byte[] nameBytes = new byte[127];
            boolean inName = true;
            MappedByteBuffer mappedByteBuffer;
            try {
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start, chunk.length);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            short nameIndex = 0;
            double ot = 0;
            while (mappedByteBuffer.hasRemaining()) {
                byte c = mappedByteBuffer.get();
                if (c == 0x3B /* Semicolon */) {
                    // no longer in name
                    inName = false;
                }
                else if (c == 0xA /* Newline */) {
                    // back to name and reset buffers at end
                    String name = new String(nameBytes, 0, nameIndex, StandardCharsets.UTF_8);
                    // int nameHash = fnv(nameBytes, nameIndex);
                    ResultRow rr;
                    if ((rr = results.get(name)) != null) {
                        rr.min = Math.min(rr.min, ot);
                        rr.max = Math.max(rr.max, ot);
                        rr.count++;
                        rr.sum += ot;
                    }
                    else {
                        results.put(name, new ResultRow(ot));
                    }
                    inName = true;
                    nameIndex = 0;
                }
                else if (inName) {
                    nameBytes[nameIndex++] = c;
                }
                else {
                    // we know the val has to be between -99.9 and 99.8
                    // always with a single fractional digit
                    // represented as a byte array of either 4 or 5 characters
                    if (c == 0x2D /* minus sign */) {
                        // minus sign so number will be negative

                        // could be either n.x or nn.x
                        // char 3
                        // skip dot
                        if (mappedByteBuffer.get(mappedByteBuffer.position() + 3) == 0xA) {
                            ot = (mappedByteBuffer.get() - 48) * 10; // char 1
                        }
                        else {
                            ot = (mappedByteBuffer.get() - 48) * 100; // char 1
                            ot += (mappedByteBuffer.get() - 48) * 10; // char 2
                        }
                        mappedByteBuffer.get(); // skip dot
                        ot += (mappedByteBuffer.get() - 48); // char 2
                        ot = -(ot / 10f);
                    }
                    else {

                        // could be either n.x or nn.x
                        // char 3
                        // skip dot
                        if (mappedByteBuffer.get(mappedByteBuffer.position() + 2) == 0xA) {
                            ot = (c - 48) * 10; // char 1
                        }
                        else {
                            ot = (c - 48) * 100; // char 1
                            ot += (mappedByteBuffer.get() - 48) * 10; // char 2
                        }
                        mappedByteBuffer.get(); // skip dot
                        ot += (mappedByteBuffer.get() - 48); // char 3
                        ot = ot / 10f;
                    }
                }
            }

            // merge my results with overall results
            for (String k : results.keySet()) {
                ResultRow rr;
                ResultRow lr = results.get(k);
                if ((rr = global.get(k)) != null) {
                    rr.min = Math.min(rr.min, lr.min);
                    rr.max = Math.max(rr.max, lr.max);
                    rr.count += lr.count;
                    rr.sum += lr.sum;
                }
                else {
                    global.put(k, lr);
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, ResultRow> results = new ConcurrentHashMap<>();

        RandomAccessFile raFile = new RandomAccessFile(FILE, "r");
        FileChannel channel = raFile.getChannel();

        int numThreads = Runtime.getRuntime().availableProcessors();
        List<Chunk> chunks = getChunks(numThreads, channel);
        List<Thread> threads = new ArrayList<>();
        for (Chunk chunk : chunks) {
            // Thread t = Thread.ofVirtual().name(chunk.toString()).start(new CalculateTask(channel, results, chunk));
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
}
