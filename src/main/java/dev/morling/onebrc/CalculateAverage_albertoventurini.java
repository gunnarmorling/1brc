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

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateAverage_albertoventurini {

    // Define a prefix tree that is used to store results.
    // Each node in the trie represents a byte (NOT character) from a location name.
    // A nice side effect is, when traversing the trie to print results,
    // the names will be printed in alphabetical order.
    private static final class TrieNode {
        final TrieNode[] children = new TrieNode[256];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum;
        long count;
    }

    // Process a chunk and write results in a Trie rooted at 'root'.
    private static void processChunk(final TrieNode root, final ChunkReader cr) {
        while (cr.hasNext()) {
            TrieNode node = root;

            // Process the location name navigating through the trie
            int b = cr.getNext() & 0xFF;
            while (b != ';') {
                if (node.children[b] == null) {
                    node.children[b] = new TrieNode();
                }
                node = node.children[b];
                b = cr.getNext() & 0xFF;
            }

            // Process the reading value (temperature)
            long reading = 0;
            boolean negative = false;
            while (true) {
                byte c = cr.getNext();
                if (c == '\n') {
                    break;
                }
                else if (c == '-') {
                    negative = true;
                    continue;
                }
                else if (c == '.') {
                    continue;
                }
                reading = reading * 10 + (c - '0');
            }

            final long signedReading = negative ? -reading : reading;

            node.min = Math.min(node.min, signedReading);
            node.max = Math.max(node.max, signedReading);
            node.sum += signedReading;
            node.count++;
        }
    }

    // Print results.
    // Because there are multiple tries (one for each thread), this method
    // aggregates results from all tries.
    static class ResultPrinter {
        // Contains the bytes for the current location name. 100 bytes should be enough
        // to represent all location names encoded in UTF-8.
        final byte[] bytes = new byte[100];

        boolean firstOutput = true;

        void printResults(final TrieNode[] roots) {
            System.out.print("{");
            printResultsRec(roots, bytes, 0);
            System.out.println("}");
        }

        private double round(long value) {
            return Math.round(value) / 10.0;
        }

        // Find and print results recursively.
        private void printResultsRec(final TrieNode[] nodes, final byte[] bytes, final int index) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long sum = 0;
            long count = 0;

            for (final TrieNode node : nodes) {
                if (node != null && node.count > 0) {
                    min = Math.min(min, node.min);
                    max = Math.max(max, node.max);
                    sum += node.sum;
                    count += node.count;
                }
            }

            if (count > 0) {
                final String location = new String(bytes, 0, index);
                if (firstOutput) {
                    firstOutput = false;
                }
                else {
                    System.out.print(", ");
                }
                double mean = Math.round((double) sum / (double) count) / 10.0;
                System.out.print(location + "=" + round(min) + "/" + mean + "/" + round(max));
            }

            for (int i = 0; i < 256; i++) {
                final TrieNode[] childNodes = new TrieNode[nodes.length];
                boolean shouldRecurse = false;
                for (int j = 0; j < nodes.length; j++) {
                    if (nodes[j] != null && nodes[j].children[i] != null) {
                        childNodes[j] = nodes[j].children[i];

                        // Only recurse if there's at least one trie that has non-null child for index 'i'.
                        shouldRecurse = true;
                    }
                }
                if (shouldRecurse) {
                    bytes[index] = (byte) i;
                    printResultsRec(childNodes, bytes, index + 1);
                }

            }
        }
    }

    private static final String FILE = "./measurements.txt";

    private static final class ChunkReader {
        // Byte arrays of size 2^22 seem to have the best performance on my machine.
        private static final int BYTE_ARRAY_SIZE = 1 << 22;
        private final byte[] bytes;

        private final RandomAccessFile file;
        private final long chunkBegin;
        private final long chunkLength;

        private int readBytes = 0;

        private int cursor = 0;
        private long offset = 0;

        ChunkReader(
                    final RandomAccessFile file,
                    final long chunkBegin,
                    final long chunkLength) {
            this.file = file;
            this.chunkBegin = chunkBegin;
            this.chunkLength = chunkLength;

            int byteArraySize = chunkLength < BYTE_ARRAY_SIZE ? (int) chunkLength : BYTE_ARRAY_SIZE;
            this.bytes = new byte[byteArraySize];

            readNextBytes();
        }

        boolean hasNext() {
            return (offset + cursor) < chunkLength;
        }

        byte getNext() {
            if (cursor >= readBytes) {
                readNextBytes();
            }
            return bytes[cursor++];
        }

        private void readNextBytes() {
            try {
                offset += readBytes;
                synchronized (file) {
                    file.seek(chunkBegin + offset);
                    readBytes = file.read(bytes);
                }
                cursor = 0;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static ChunkReader[] makeChunkReaders(
                                                  final int count,
                                                  final RandomAccessFile file)
            throws Exception {

        final ChunkReader[] chunkReaders = new ChunkReader[count];

        // The total size of each chunk
        final long chunkReaderSize = file.length() / count;

        long previousPosition = 0;
        long currentPosition;

        for (int i = 0; i < count; i++) {
            // Go to the end of the chunk
            file.seek(chunkReaderSize * (i + 1));

            // Align to the next end of line or end of file
            try {
                while (file.readByte() != '\n')
                    ;
            }
            catch (EOFException e) {
            }

            currentPosition = file.getFilePointer();
            long chunkBegin = previousPosition;
            long chunkLength = currentPosition - previousPosition;
            chunkReaders[i] = new ChunkReader(file, chunkBegin, chunkLength);

            previousPosition = currentPosition;
        }

        return chunkReaders;
    }

    private static void processWithChunkReaders() throws Exception {
        final var randomAccessFile = new RandomAccessFile(FILE, "r");

        final int nThreads = Runtime.getRuntime().availableProcessors() - 1;

        final CountDownLatch latch = new CountDownLatch(nThreads);

        final ChunkReader[] chunkReaders = makeChunkReaders(nThreads, randomAccessFile);
        final TrieNode[] roots = new TrieNode[nThreads];
        for (int i = 0; i < nThreads; i++) {
            roots[i] = new TrieNode();
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            final int idx = i;
            executorService.submit(() -> {
                processChunk(roots[idx], chunkReaders[idx]);
                latch.countDown();
            });
        }
        executorService.shutdown();
        latch.await();

        new ResultPrinter().printResults(roots);

        executorService.close();
    }

    public static void main(String[] args) throws Exception {

        processWithChunkReaders();

    }
}