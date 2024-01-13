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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_arjenvaneerde {

    static class Measure {
        byte[] station;
        int length;
        int count;
        int minTemp;
        int maxTemp;
        long sumTemp;

        Measure(final byte[] station, int count, int minTemp, int maxTemp, long sumTemp) {
            this.station = station;
            this.length = station.length;
            this.count = count;
            this.minTemp = minTemp;
            this.maxTemp = maxTemp;
            this.sumTemp = sumTemp;
        }

        Measure(byte[] bytes, int startPos, int endPos, int temp) {
            this.length = endPos - startPos;
            this.station = new byte[this.length];
            System.arraycopy(bytes, startPos, this.station, 0, this.station.length);
            this.count = 1;
            this.minTemp = temp;
            this.maxTemp = temp;
            this.sumTemp = temp;
        }

        void add(int temp) {
            this.count++;
            this.minTemp = Math.min(this.minTemp, temp);
            this.maxTemp = Math.max(this.maxTemp, temp);
            this.sumTemp += temp;
        }

        public static Measure merge(Measure m1, Measure m2) {
            return new Measure(m1.station, m1.count + m2.count, Math.min(m1.minTemp, m2.minTemp), Math.max(m1.maxTemp, m2.maxTemp), m1.sumTemp + m2.sumTemp);
        }

        public String toString() {
            return String.format("%.1f/%.1f/%.1f", this.minTemp / 10.0, this.sumTemp / (10.0 * this.count), this.maxTemp / 10.0);
        }
    }

    static class Measures {
        static final int HASH_TABLE_SIZE = 8 * 1024;

        Measure[][] measureHashTable;

        Measures() {
            this.measureHashTable = new Measure[HASH_TABLE_SIZE][20];
        }

        void add(byte[] bytes, int startPos, int endPos, int temp) {
            int len = endPos - startPos;
            int index = ((len - 2) & 0x0f | // 4 bits of the length
                    ((bytes[startPos + 0] & 0x1f) << 4) | // 5 bits of first char
                    ((bytes[startPos + 2] & 0x1f) << 9) // 5 bits of third char
            // ((bytes[startPos + 1] & 0x1f) << 14) // 5 bits of second char
            ) & (HASH_TABLE_SIZE - 1);
            Measure[] arr = this.measureHashTable[index];
            int i = 0;
            boolean found = false;
            int arrLength = arr.length;
            while (i < arrLength) {
                Measure m = arr[i];
                if (m == null) {
                    // Not found. Add new entry.
                    arr[i] = new Measure(bytes, startPos, endPos, temp);
                    return;
                }
                if (m.length == len) {
                    switch (len) {
                        case 0:
                            break;
                        case 1:
                            if (m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        case 2:
                            if (m.station[1] == bytes[startPos + 1] &&
                                    m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        case 3:
                            if (m.station[2] == bytes[startPos + 2] &&
                                    m.station[1] == bytes[startPos + 1] &&
                                    m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        case 4:
                            if (m.station[3] == bytes[startPos + 3] &&
                                    m.station[2] == bytes[startPos + 2] &&
                                    m.station[1] == bytes[startPos + 1] &&
                                    m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        case 5:
                            if (m.station[4] == bytes[startPos + 4] &&
                                    m.station[3] == bytes[startPos + 3] &&
                                    m.station[2] == bytes[startPos + 2] &&
                                    m.station[1] == bytes[startPos + 1] &&
                                    m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        case 6:
                            if (m.station[5] == bytes[startPos + 5] &&
                                    m.station[4] == bytes[startPos + 4] &&
                                    m.station[3] == bytes[startPos + 3] &&
                                    m.station[2] == bytes[startPos + 2] &&
                                    m.station[1] == bytes[startPos + 1] &&
                                    m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        case 7:
                            if (m.station[6] == bytes[startPos + 6] &&
                                    m.station[5] == bytes[startPos + 5] &&
                                    m.station[4] == bytes[startPos + 4] &&
                                    m.station[3] == bytes[startPos + 3] &&
                                    m.station[2] == bytes[startPos + 2] &&
                                    m.station[1] == bytes[startPos + 1] &&
                                    m.station[0] == bytes[startPos]) {
                                found = true;
                            }
                            break;
                        default:
                            found = Arrays.mismatch(m.station, 0, len, bytes, startPos, endPos) == -1;
                            break;
                    }
                    if (found) {
                        // Add info.
                        m.add(temp);
                        return;
                    }
                }
                i++;
            }
            // throw new RuntimeException("Reached end of Measures array.");
            Measure[] newArr = new Measure[arr.length * 2];
            System.arraycopy(arr, 0, newArr, 0, arr.length);
            newArr[i] = new Measure(bytes, startPos, endPos, temp);
            this.measureHashTable[index] = newArr;
        }
    }

    private static class BytesProcessor implements Runnable {
        final Measures measures;
        final byte[] buffer;
        long absoluteStartPos;
        int startPos;
        int endPos;

        BytesProcessor(byte[] buffer, long absoluteStartPos, int startPos, int endPos, final Measures measures) {
            this.buffer = buffer;
            this.absoluteStartPos = absoluteStartPos;
            this.startPos = startPos;
            this.endPos = endPos;
            this.measures = measures;
        }

        @Override
        public void run() {
            int startOfLinePos = startPos;
            int endOfLinePos = startOfLinePos;
            int sepPos;
            // Process all lines
            while (endOfLinePos < endPos) {
                while (buffer[endOfLinePos] != ';') {
                    endOfLinePos++;
                }
                sepPos = endOfLinePos;
                int temperature = 0;
                byte lineByte;
                endOfLinePos++;
                lineByte = buffer[endOfLinePos];
                while (lineByte != 0x0A) {
                    if (lineByte >= '0' && lineByte <= '9') {
                        temperature = temperature * 10 + (lineByte - '0');
                    }
                    lineByte = buffer[++endOfLinePos];
                }
                if (buffer[sepPos + 1] == '-') {
                    temperature = -temperature;
                }
                measures.add(buffer, startOfLinePos, sepPos, temperature);
                startOfLinePos = ++endOfLinePos;
            }
        }
    }

    private static class MeasuresMergeProcessor implements Runnable {
        final Measures[] measures;
        final int startIndex;
        final int endIndex;
        final ConcurrentSkipListMap<String, Measure> results;

        MeasuresMergeProcessor(final Measures[] measures,
                               final int startIndex,
                               final int endIndex,
                               final ConcurrentSkipListMap<String, Measure> results) {
            this.measures = measures;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.results = results;
        }

        @Override
        public void run() {
            for (int mIdx = 0; mIdx < this.measures.length; mIdx++) {
                for (int hashIdx = this.startIndex; hashIdx < this.endIndex; hashIdx++) {
                    Measure[] mArr = this.measures[mIdx].measureHashTable[hashIdx];
                    int i = 0;
                    Measure measure = mArr[i];
                    while (measure != null) {
                        Measure finalMeasure = measure;
                        this.results.compute(new String(measure.station, StandardCharsets.UTF_8), (k, v) -> v == null ? finalMeasure : Measure.merge(v, finalMeasure));
                        measure = mArr[++i];
                    }
                }
            }
        }
    }

    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./src/test/resources/samples/measurements-1.txt";
    // private static final String FILE = "./src/test/resources/samples/measurements-10000-unique-keys.txt";
    private static final int NUM_THREADS = 8; // Runtime.getRuntime().availableProcessors();
    private static final int BYTE_BUFFER_SIZE = 16 * 1024 * 1024;
    private static final ExecutorService threads = Executors.newFixedThreadPool(NUM_THREADS);
    private static final List<Future<Integer>> futures = new ArrayList<>(NUM_THREADS);
    private static final Measures[] measures = new Measures[NUM_THREADS];

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUM_THREADS; i++) {
            measures[i] = new Measures();
        }
        File file = new File(FILE);
        try (RandomAccessFile raFile = new RandomAccessFile(file, "r");
                FileChannel inChannel = raFile.getChannel()) {

            ByteBuffer[] byteBuffers = new ByteBuffer[2];
            byteBuffers[0] = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
            byteBuffers[1] = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
            int currrentByteBuffer = 0;
            long numBytesRead = 0;
            long numTotBytesRead = 0;
            numBytesRead = inChannel.read(byteBuffers[currrentByteBuffer]);
            byteBuffers[currrentByteBuffer].flip();
            while (numBytesRead > 0 || byteBuffers[currrentByteBuffer].position() > 0) {
                if (numBytesRead > 0) {
                    numTotBytesRead += numBytesRead;
                }
                // System.out.println((double) numTotBytesRead/(1024*1024));
                // Distribute buffer chunks across threads.
                int bufferLimit = byteBuffers[currrentByteBuffer].limit();
                byte[] fileBytes = byteBuffers[currrentByteBuffer].array();
                int chunckSize = bufferLimit / NUM_THREADS;
                int startOfChunkPos = 0;
                int endOfChunkPos = 0;
                for (int chunk = 0; chunk < NUM_THREADS; chunk++) {
                    if (chunk == NUM_THREADS - 1) {
                        endOfChunkPos = bufferLimit - 1;
                    }
                    else {
                        endOfChunkPos = Math.min((chunk + 1) * chunckSize, bufferLimit - 1);
                    }
                    while (endOfChunkPos > startOfChunkPos &&
                            fileBytes[endOfChunkPos] != 0x0A) {
                        endOfChunkPos--;
                    }
                    if (endOfChunkPos > startOfChunkPos) {
                        endOfChunkPos++;
                        Future<Integer> f = threads.submit(new BytesProcessor(fileBytes, numTotBytesRead - numBytesRead, startOfChunkPos, endOfChunkPos, measures[chunk]),
                                chunk);
                        futures.add(f);
                        startOfChunkPos = endOfChunkPos;
                    }
                }
                int nextByteBuffer = (currrentByteBuffer + 1) % 2;
                byteBuffers[nextByteBuffer].clear();
                if (numBytesRead > 0) {
                    // Copy over remaining bytes to next buffer.
                    for (; endOfChunkPos < bufferLimit; endOfChunkPos++) {
                        byteBuffers[nextByteBuffer].put(fileBytes[endOfChunkPos]);
                    }
                    // Read next set from channel.
                    numBytesRead = inChannel.read(byteBuffers[nextByteBuffer]);
                    byteBuffers[nextByteBuffer].flip();
                    // Wait for all threads to finish.
                    for (Future<Integer> future : futures) {
                        future.get();
                    }
                    futures.clear();
                }
                currrentByteBuffer = nextByteBuffer;
            }
        }
        ConcurrentSkipListMap<String, Measure> measurements = new ConcurrentSkipListMap<>();
        int chunkSize = Measures.HASH_TABLE_SIZE / measures.length;
        for (int i = 0; i < measures.length; i++) {
            Future<Integer> f = threads.submit(new MeasuresMergeProcessor(measures, i * chunkSize, (i + 1) * chunkSize, measurements), i);
            futures.add(f);
        }
        for (Future<Integer> future : futures) {
            future.get();
        }
        futures.clear();
        threads.shutdown();
        threads.awaitTermination(1, TimeUnit.MILLISECONDS);
        System.out.println(measurements);
        // long endTime = System.currentTimeMillis();
        // System.out.printf("Duration : %.3f%n", (endTime - startTime) / 1000.0);
    }

}
