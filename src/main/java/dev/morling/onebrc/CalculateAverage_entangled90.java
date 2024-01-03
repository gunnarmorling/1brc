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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.Math.round;

public class CalculateAverage_entangled90 {

    private static final String FILE = "./measurements.txt";

    public static final boolean DEBUG = false;

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner();
        long start = System.currentTimeMillis();
        PooledChunkProcessor chunkProcessor = new PooledChunkProcessor(Runtime.getRuntime().availableProcessors());
        scanner.scan(FILE, chunkProcessor);
        long finish = System.currentTimeMillis();
        var map = chunkProcessor.result();
        map.printResults();
        if (DEBUG) {
            System.out.println("Took: " + TimeUnit.MILLISECONDS.toSeconds(finish - start) + "seconds");
        }
    }
}

class AggregatedProcessor {
    public double max = 0D;
    public double min = 0D;
    private double sum = 0D;
    private long count = 0L;

    public void addMeasure(double value) {
        max = Math.max(max, value);
        min = Math.min(min, value);
        sum += value;
        count++;
    }

    public void combine(AggregatedProcessor processor) {
        max = Math.max(max, processor.max);
        min = Math.min(min, processor.min);
        sum += processor.sum;
        count += processor.count;
    }

    public double mean() {
        return sum / count;
    }

    @Override
    public String toString() {
        return round(min) + "/" + round(mean()) + "/" + round(max);
    }
}

class ProcessorMap {
    Map<String, AggregatedProcessor> processors = new HashMap<>(1024);

    public void printResults() {
        System.out.print("{");
        System.out.print(
                processors.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    public void addMeasure(String city, double value) {
        var processor = processors.get(city);
        if (processor == null) {
            processor = new AggregatedProcessor();
            processors.put(city, processor);
        }
        processor.addMeasure(value);
    }

    private void combine(String city, AggregatedProcessor processor) {
        var thisProcessor = processors.get(city);
        if (thisProcessor == null) {
            processors.put(city, processor);
        }
        else {
            thisProcessor.combine(processor);
        }
    }

    public static ProcessorMap combineAll(ProcessorMap... processors) {
        var result = new ProcessorMap();
        for (var p : processors) {
            for (var entry : p.processors.entrySet()) {
                result.combine(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}

class PooledChunkProcessor implements Consumer<ByteBuffer> {
    private final ArrayBlockingQueue<ByteBuffer> queue;
    private final ArrayList<Thread> threads;
    private final ProcessorMap[] results;
    private final CountDownLatch latch;

    private volatile boolean queueClosed = false;

    public PooledChunkProcessor(int n) {
        queue = new ArrayBlockingQueue<>(4 * 1024);
        results = new ProcessorMap[n];
        threads = new ArrayList<>(n);
        latch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int finalI = i;
            var t = new Thread(() -> {
                var processor = new ChunkProcessor();
                while (!Thread.interrupted()) {
                    try {
                        var element = queue.poll(10, TimeUnit.MILLISECONDS);
                        if (element != null)
                            processor.processChunk(element);
                        else if (queueClosed) {
                            if (CalculateAverage_entangled90.DEBUG) {
                                System.out.println("Queue closed, thread #" + finalI + " stopping");
                            }
                            break;
                        }
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
                results[finalI] = processor.processors;
                latch.countDown();
            });
            threads.add(t);
            t.start();
        }
    }

    @Override
    public void accept(ByteBuffer byteBuffer) {
        queue.offer(byteBuffer);
    }

    public ProcessorMap result() {
        try {
            queueClosed = true;
            latch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ProcessorMap.combineAll(results);
    }
}

class ChunkProcessor implements Consumer<ByteBuffer> {
    ProcessorMap processors = new ProcessorMap();

    public void processChunk(ByteBuffer bb) {
        while (processRow(bb)) {
        }
    }

    @Override
    public void accept(ByteBuffer byteBuffer) {
        processChunk(byteBuffer);
    }

    // true if you can continue
    // false if input is missing
    public boolean processRow(ByteBuffer bb) {
        int cityLimit = findChar(bb, (byte) ';');
        if (cityLimit < 0) {
            return false;
        }
        String city = stringFromBB(bb, cityLimit - bb.position()).trim();
        bb.position(cityLimit + 1);

        int valueLimit = findChar(bb, (byte) '\n');
        if (valueLimit < 0) {
            return false;
        }
        double value = Double.parseDouble(stringFromBB(bb, valueLimit - cityLimit));
        // double value = parseFromBB(bb, valueLimit - cityLimit);
        bb.position(valueLimit + 1);
        // System.out.println("Read: " + city + "=" + value);
        processors.addMeasure(city, value);

        return true;
    }

    private String stringFromBB(ByteBuffer bb, int length) {
        var bytes = new byte[length];
        bb.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private int findChar(ByteBuffer bb, byte c) {
        for (int i = bb.position(); i < bb.limit(); i++) {
            if (bb.get(i) == c)
                return i;
        }
        return -1;
    }
}

class Scanner {
    private static final int MAX_MAPPED_MEMORY = 4 * 1024 * 1024;

    public void scan(String fileName, Consumer<ByteBuffer> consumer) throws IOException {
        try (var file = new RandomAccessFile(fileName, "r"); FileChannel channel = file.getChannel()) {
            int chunkId = 0;
            // file pointer
            long fp = 0L;
            long fileLen = file.length();

            while (true) {
                if (fp > 0) {
                    file.seek(fp);
                }
                MappedByteBuffer bb = channel.map(FileChannel.MapMode.READ_ONLY, fp, Math.min(MAX_MAPPED_MEMORY, fileLen - fp));
                bb.load();
                var limitIdx = findLastNewLine(bb);
                bb.limit(limitIdx);
                // get last newline from the end
                consumer.accept(bb);
                chunkId++;
                fp += offset;
                if (CalculateAverage_entangled90.DEBUG && chunkId % 10 == 0) {
                    System.out.println(" read chunk " + chunkId + " at pointer " + fp + "(" + ((int) (fp * 100 / fileLen)) + "%)");
                }

                if (fp >= fileLen - 1) {
                    break;
                }
            }
        }
    }

    public int findLastNewLine(ByteBuffer bb) {
        for (int i = bb.limit() - 1; i > bb.position(); i--) {
            if (bb.get(i) == '\n') {
                return i;
            }
        }
        return -1;
    }
}