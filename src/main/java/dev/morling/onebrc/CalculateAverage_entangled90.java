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
import java.util.*;
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
        scanner.scan(FILE, chunkProcessor, 0L, -1);
        long finish = System.currentTimeMillis();
        var map = chunkProcessor.result();
        map.printResults();
        if (DEBUG) {
            System.out.println("Took: " + TimeUnit.MILLISECONDS.toSeconds(finish - start) + "seconds");
        }
    }
}

class AggregatedProcessor {
    public double max = Double.NEGATIVE_INFINITY;
    public double min = Double.POSITIVE_INFINITY;
    private double sum = 0L;
    public long count = 0L;

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

    private static double round(double d) {
        return Math.round(d * 10.0) / 10.0;
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "%.1f/%.1f/%.1f", round(min), round(mean()), round(max));
    }
}

class ProcessorMap {
    Map<BytesWrapper, AggregatedProcessor> processors = new HashMap<>(1024);

    public void printResults() {
        // System.out.println("Processed: " + processors.entrySet().stream().mapToLong(e -> e.getValue().count).sum());
        System.out.print("{");
        System.out.print(
                processors.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey()).map(Object::toString).collect(Collectors.joining(", ")));
        System.out.println("}");
    }

    public void addMeasure(BytesWrapper city, double value) {
        var processor = processors.get(city);
        if (processor == null) {
            processor = new AggregatedProcessor();
            processors.put(city, processor);
        }
        processor.addMeasure(value);
    }

    private void combine(BytesWrapper city, AggregatedProcessor processor) {
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
    private final ProcessorMap[] results;
    private final CountDownLatch latch;

    private volatile boolean queueClosed = false;

    public PooledChunkProcessor(int n) {
        queue = new ArrayBlockingQueue<>(4 * 1024);
        results = new ProcessorMap[n];
        latch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int finalI = i;
            var t = new Thread(() -> {
                var processor = new ChunkProcessor();
                while (!Thread.interrupted()) {
                    try {
                        var element = queue.poll(1, TimeUnit.MILLISECONDS);
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
            t.start();
        }
    }

    @Override
    public void accept(ByteBuffer byteBuffer) {
        try {
            queue.put(byteBuffer);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
        int colonIdx = findSemiColon(bb);
        if (colonIdx < 0) {
            return false;
        }
        while (bb.get(bb.position()) == '\n') {
            bb.get();
        }
        var wrapper = wrapperFromBB(bb, colonIdx - bb.position());
        bb.position(colonIdx + 1);
        double value = parseDoubleNewLine(bb);
        processors.addMeasure(wrapper, value);
        return true;
    }

    private static BytesWrapper wrapperFromBB(ByteBuffer bb, int length) {
        var bytes = new byte[length];
        bb.get(bytes);
        return new BytesWrapper(bytes);
    }

    // don't advance bb
    private int findSemiColon(ByteBuffer bb) {
        for (int i = bb.position(); i < bb.limit(); i++) {
            if (bb.get(i) == (byte) ';')
                return i;
        }
        return -1;
    }

    // parses double until new line and advances buffer
    private static double parseDoubleNewLine(ByteBuffer bb) {
        int result = 0;
        int sign = 1;
        byte c;
        do {
            c = bb.get();
            switch (c) {
                case '-':
                    sign = -1;
                    break;
                case '.', ',', '\r', '\n':
                    break;
                default:
                    result = result * 10 + (c - '0');
            }
        } while (c != '\n' && bb.position() < bb.limit());
        return result * sign / 10.0;
    }
}

class Scanner {
    private static final int MAX_MAPPED_MEMORY = 32 * 1024 * 1024;

    public void scan(String fileName, Consumer<ByteBuffer> consumer, long offset, long len) throws IOException {
        try (var file = new RandomAccessFile(fileName, "r"); FileChannel channel = file.getChannel()) {
            int chunkId = 0;
            if (len < 0) {
                len = file.length();
            }

            while (true) {
                if (offset > 0) {
                    file.seek(offset);
                }
                MappedByteBuffer bb = channel.map(FileChannel.MapMode.READ_ONLY, offset, Math.min(MAX_MAPPED_MEMORY, len - offset));
                var limitIdx = findLastNewLine(bb);
                bb.limit(limitIdx);
                // get last newline from the end
                consumer.accept(bb);
                chunkId++;
                offset += limitIdx;
                if (CalculateAverage_entangled90.DEBUG && chunkId % 10 == 0) {
                    System.out.println(" read chunk " + chunkId + " at pointer " + offset + "(" + ((int) (offset * 100 / len)) + "%)");
                }

                if (offset >= len - 1) {
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

class BytesWrapper implements Comparable<BytesWrapper> {
    private final byte[] bytes;

    public BytesWrapper(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesWrapper) {
            return Arrays.equals(bytes, ((BytesWrapper) obj).bytes);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return new String(bytes);
    }

    @Override
    public int compareTo(BytesWrapper bytesWrapper) {
        return this.toString().compareTo(bytesWrapper.toString());
    }
}
