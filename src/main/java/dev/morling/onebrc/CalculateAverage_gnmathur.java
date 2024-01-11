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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * This solution uses an imperative approach. There's essentially two elements to performance tuning attempted in this
 * solution:
 * 1. Use a thread pool to process the file in chunks, setting it to the number of available processors
 * 2. Use a memory mapped file to read the file
 *
 * On an Intel(R) Core(TM) i9-10920X CPU @ 3.50GHz its taking around 38 seconds to aggregate the measurements.
 */
public class CalculateAverage_gnmathur {
    private static final ExecutorService es = newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static final Map<String, Measurement> result = new ConcurrentHashMap<>();
    private static final String FILE_NAME = "measurements.txt";
    private static final long CHUNK_SIZE = 1024 * 1024 * 1024; // 1 GB

    public static final class Measurement {
        private double max = -5000; // impossibly low
        private double min = 5000; // impossibly high
        private double sum = 0;
        private double count = 0;

        public Measurement() {
        }

        public synchronized void addReading(double reading) {
            this.min = Math.min(this.min, reading);
            this.max = Math.max(this.max, reading);
            this.sum += reading;
            this.count++;
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public String toString() {
            double mean = sum / count;
            return round(min) + "/" + round(mean) + "/" + round(max);
        }
    }

    private static void updateMeasurement(String line) {
        String[] parts = line.split(";");
        String station = parts[0];
        double measurement = Double.parseDouble(parts[1]);
        Measurement m = null;
        if (result.containsKey(station)) {
            m = result.get(station);
        }
        else {
            m = new Measurement();
            result.put(station, m);
        }
        m.addReading(measurement);
    }

    private record FileChunkProcessor(String fileName, ExecutorService es, long start, long end) implements Runnable {

    @Override
    public void run() {
        // Process a chunk of the file
        try (RandomAccessFile fp = new RandomAccessFile(FILE_NAME, "r");
                FileChannel fpChannel = fp.getChannel()) {
            final MappedByteBuffer mappedByteBuffer = fpChannel.map(FileChannel.MapMode.READ_ONLY, start, end - start);
            final ByteBuffer bb = ByteBuffer.allocate(1024);

            while (mappedByteBuffer.hasRemaining()) {
                byte b = mappedByteBuffer.get();
                if (b == '\n') {
                    // We have read a line. Convert the tempBuffer to a string and process it
                    bb.flip();
                    String line = StandardCharsets.UTF_8.decode(bb).toString();
                    updateMeasurement(line);
                    bb.clear();
                }
                else {
                    bb.put(b);
                }
            }
            // There should be nothing in the byte buffer at this point
            if (bb.position() > 0) {
                throw new RuntimeException("byte buffer not empty");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    }

    // We want to make sure that we don't split the data in the middle of a line. So we find the closest next
    // newline character and adjust the end to that.
    public static long adjustEnd(final String fileName, long end) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            raf.seek(end);
            while (true) {
                int read = raf.read();
                if (read == -1 || read == '\n') {
                    break;
                }
                end++;
            }
        }
        return end;
    }

    // Read the file in chunks and submit each chunk to a thread for processing
    public static void readChunked(final String fileName, final long chunkSize) {
        try (RandomAccessFile fp = new RandomAccessFile(fileName, "r")) {
            long fileSize = fp.length();
            long start = 0;

            while (start < fileSize) {
                long end = Math.min(start + chunkSize, fileSize);
                end = adjustEnd(fileName, end);
                es.submit(new FileChunkProcessor(fileName, es, start, end));
                start = end + 1;
            }
            es.shutdown();
            // Wait for all the threads to finish processing
            es.awaitTermination(4, java.util.concurrent.TimeUnit.MINUTES);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        readChunked(FILE_NAME, CHUNK_SIZE);
        // Print the results
        System.out.println(new TreeMap<>(result));
    }
}
