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

import java.util.concurrent.*;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_dalogax {

    // Tunning parameters
    private static final int QUEUE_CAPACITY = 6;
    private static final int PROCESSOR_POOL_SIZE = 6;
    public static final int CHUNK_SIZE = 1024 * 1024 * 10;

    private static final String END_MARKER = "END";

    public static void main(String[] args) throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        Thread readerThread = new Thread(() -> readChunks(queue));
        Thread processorThread = new Thread(() -> processChunks(queue));

        readerThread.start();
        processorThread.start();
    }

    private static void readChunks(BlockingQueue<String> queue) {
        try {
            ChunkReader reader = new ChunkReader("./measurements.txt");
            while (reader.hasMoreChunks()) {
                queue.put(reader.readChunk());
            }
            queue.put(END_MARKER);
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void processChunks(BlockingQueue<String> queue) {
        try {
            ExecutorService executor = Executors.newFixedThreadPool(PROCESSOR_POOL_SIZE);
            List<Future<Map<String, CityTemperatureStats>>> futures = new ArrayList<>(PROCESSOR_POOL_SIZE);

            while (true) {
                String chunk = queue.take();
                if (chunk.equals(END_MARKER)) {
                    break;
                }
                futures.add(executor.submit(new CityStatsProcessor(chunk)));
                chunk = null;
            }

            executor.shutdown();

            CityStats overallStats = new CityStats();
            for (Future<Map<String, CityTemperatureStats>> future : futures) {
                overallStats.combine(future.get());
            }

            overallStats.print();
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}

class ChunkReader {

    private final RandomAccessFile file;
    private long nextChunkStart;
    private byte[] buffer;

    public ChunkReader(String filePath) throws IOException {
        this.file = new RandomAccessFile(filePath, "r");
        this.nextChunkStart = 0;
        this.buffer = new byte[CalculateAverage_dalogax.CHUNK_SIZE];
    }

    public boolean hasMoreChunks() throws IOException {
        return nextChunkStart < file.length();
    }

    public String readChunk() throws IOException {
        // System.out.println("Reading chunk at " + nextChunkStart);
        file.seek(nextChunkStart);
        int bytesRead = file.read(buffer);
        int lastNewlineIndex = findLastNewlineIndex(buffer, bytesRead);

        nextChunkStart += lastNewlineIndex + 1;
        // System.out.println("Readed chunk at " + nextChunkStart);
        return new String(buffer, 0, lastNewlineIndex);
    }

    private int findLastNewlineIndex(byte[] buffer, int length) {
        for (int i = length - 1; i >= 0; i--) {
            if (buffer[i] == '\n') {
                return i;
            }
        }
        return -1;
    }
}

class CityStatsProcessor implements Callable<Map<String, CityTemperatureStats>> {
    private final String chunk;

    public CityStatsProcessor(String chunk) {
        this.chunk = chunk;
    }

    @Override
    public Map<String, CityTemperatureStats> call() {
        // System.out.println("Processing chunk with " + chunk.length() + " bytes");

        Map<String, CityTemperatureStats> stats = new HashMap<>();

        for (String line : chunk.split("\n")) {
            // System.out.println("Processing line: " + line);
            String[] parts = line.split(";");
            if (parts.length != 2) {
                System.err.println("Invalid line: " + line);
                continue;
            }

            try {
                stats.computeIfAbsent(parts[0], k -> new CityTemperatureStats())
                        .accept(Double.parseDouble(parts[1]));
            }
            catch (NumberFormatException e) {
                System.err.println("Invalid temperature value: " + parts[1]);
            }
        }

        // System.out.println("Processed chunk with " + chunk.split("\n").length + " lines");

        return stats;
    }
}

class CityTemperatureStats {
    private double sum = 0;
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;
    private int count = 0;

    public void accept(double value) {
        sum += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
        count++;
    }

    public void combine(CityTemperatureStats other) {
        sum += other.sum;
        min = Math.min(min, other.min);
        max = Math.max(max, other.max);
        count += other.count;
    }

    public double getAverage() {
        return sum / count;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }
}

class CityStats {
    private final Map<String, CityTemperatureStats> stats;

    public CityStats() {
        this.stats = new TreeMap<>();
    }

    public void combine(Map<String, CityTemperatureStats> otherStats) {
        for (Map.Entry<String, CityTemperatureStats> entry : otherStats.entrySet()) {
            String city = entry.getKey();
            CityTemperatureStats otherCityStats = entry.getValue();

            stats.computeIfAbsent(city, k -> new CityTemperatureStats())
                    .combine(otherCityStats);
        }
    }

    public void print() {
        StringBuilder output = new StringBuilder("{");
        for (Map.Entry<String, CityTemperatureStats> entry : stats.entrySet()) {
            String city = entry.getKey();
            CityTemperatureStats cityStats = entry.getValue();

            output.append(String.format("%s=%.1f/%.1f/%.1f, ",
                    city, cityStats.getMin(), cityStats.getAverage(), cityStats.getMax()));
        }
        if (output.length() > 1) {
            output.setLength(output.length() - 2);
        }
        output.append("}");

        System.out.println(output.toString());
    }
}