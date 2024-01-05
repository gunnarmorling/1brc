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

    private static class CalculateTask implements Runnable {

        private final FileChannel channel;
        private final Map<String, ResultRow> results;
        private final Chunk chunk;

        public CalculateTask(FileChannel fileChannel, Map<String, ResultRow> results, Chunk chunk) {
            this.channel = fileChannel;
            this.results = results;
            this.chunk = chunk;
        }

        @Override
        public void run() {
            // no names bigger than this
            byte[] nameBytes = new byte[127];
            byte[] valBytes = new byte[127];
            boolean inName = true;
            MappedByteBuffer mappedByteBuffer;
            try {
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start, chunk.length);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            short nameIndex = 0;
            short valIndex = 0;
            while (mappedByteBuffer.hasRemaining()) {
                byte c = mappedByteBuffer.get();
                if (c == 0x3B /* Semicolon */) {
                    // no longer in name
                    inName = false;
                }
                else if (c == 0xA /* Newline */) {
                    // back to name and reset buffers at end
                    String name = new String(nameBytes, 0, nameIndex, StandardCharsets.UTF_8);
                    double t = Double.parseDouble(new String(valBytes, 0, valIndex, StandardCharsets.UTF_8));
                    if (results.containsKey(name)) {
                        ResultRow rr = results.get(name);
                        rr.min = Math.min(rr.min, t);
                        rr.max = Math.max(rr.max, t);
                        rr.count++;
                        rr.sum += t;
                    }
                    else {
                        results.put(name, new ResultRow(t));
                    }
                    inName = true;
                    nameIndex = 0;
                    valIndex = 0;
                }
                else if (inName) {
                    nameBytes[nameIndex++] = c;
                }
                else {
                    valBytes[valIndex++] = c;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, ResultRow> results = new ConcurrentHashMap<>();

        RandomAccessFile raFile = new RandomAccessFile(FILE, "r");
        FileChannel channel = raFile.getChannel();

        int numThreads = 64;
        List<Chunk> chunks = getChunks(numThreads, channel);
        List<Thread> threads = new ArrayList<>();
        for (Chunk chunk : chunks) {
            Thread t = Thread.ofVirtual().name(chunk.toString()).start(new CalculateTask(channel, results, chunk));
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

        // just to sort
        System.out.println(new TreeMap<>(results));
    }
}
