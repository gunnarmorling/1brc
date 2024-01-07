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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_mariuslarsen {
    private static final Byte DELIMITER = ';';
    private static final Byte NEWLINE = '\n';
    private static final int N_THREADS = 8;
    private static final int MAX_NUMBER_OF_DESTINATIONS = 1024;
    private static final int BLOCK_SIZE = 2048;

    public static void main(String[] args) throws IOException {
        Path path = Path.of("./measurements100m.txt");

        if (args.length > 0) {
            path = Path.of(args[0]);
        }

        long start = System.currentTimeMillis();
        readMeasurements(path);
        long end = System.currentTimeMillis();
        System.out.printf("Time: %f", (end - start) / 1000.0);
    }

    private static void readMeasurements(Path path) {
        int numBuffers = 2 * N_THREADS;
        BlockingQueue<ByteBuffer> fullBuffers = new ArrayBlockingQueue<>(numBuffers);
        BlockingQueue<ByteBuffer> emptyBuffers = new ArrayBlockingQueue<>(numBuffers);
        for (int i = 0; i < numBuffers; i++) {
            emptyBuffers.offer(ByteBuffer.allocate(BLOCK_SIZE));
        }
        AtomicBoolean isDone = new AtomicBoolean(false);

        try (ExecutorService ex = Executors.newFixedThreadPool(N_THREADS)) {
            ex.submit(new ReaderThread(fullBuffers, emptyBuffers, path, isDone));
            var tasks = IntStream.range(0, N_THREADS - 1)
                    .mapToObj(i -> new ParserThread(fullBuffers, emptyBuffers, isDone))
                    .toList();
            var res = ex.invokeAll(tasks).stream()
                    .map(Future::resultNow)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(s -> new String(s.city), Function.identity(), Stats::join, TreeMap::new));
            System.out.println(res);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class ReaderThread implements Runnable {

        BlockingQueue<ByteBuffer> fullBuffers;
        BlockingQueue<ByteBuffer> emptyBuffers;
        Path path;
        AtomicBoolean isDone;

        public ReaderThread(BlockingQueue<ByteBuffer> fullBuffers, BlockingQueue<ByteBuffer> emptyBuffers, Path path, AtomicBoolean isdone) {
            this.fullBuffers = fullBuffers;
            this.emptyBuffers = emptyBuffers;
            this.path = path;
            this.isDone = isdone;
        }

        @Override
        public void run() {
            try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ)) {
                List<MappedByteBuffer> mappedByteBuffers = createBuffers(chan);
                for (var mappedByteBuffer : mappedByteBuffers) {
                    int size = mappedByteBuffer.limit();
                    int processed = 0;
                    while (processed < size) {
                        ByteBuffer buffer = emptyBuffers.take();
                        int max = Math.min(buffer.capacity(), mappedByteBuffer.remaining() - processed);
                        mappedByteBuffer.get(processed, buffer.array(), 0, max);
                        while (buffer.get(--max) != NEWLINE)
                            ;
                        buffer.limit(max + 1);
                        processed += max + 1;
                        fullBuffers.put(buffer);
                    }
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            isDone.set(true);
        }
    }

    static class ParserThread implements Callable<Collection<Stats>> {
        BlockingQueue<ByteBuffer> fullBuffers;
        BlockingQueue<ByteBuffer> emptyBuffers;
        AtomicBoolean isDone;

        public ParserThread(BlockingQueue<ByteBuffer> fullBuffers, BlockingQueue<ByteBuffer> emptyBuffers, AtomicBoolean isDone) {
            this.fullBuffers = fullBuffers;
            this.emptyBuffers = emptyBuffers;
            this.isDone = isDone;
        }

        @Override
        public Collection<Stats> call() {
            Map<Integer, Stats> destinations = new HashMap<>(MAX_NUMBER_OF_DESTINATIONS);
            ByteBuffer buffer;
            while (!isDone.get() || !fullBuffers.isEmpty()) {
                buffer = fullBuffers.poll();
                if (buffer == null)
                    continue;
                Stats currentStats;
                int size;
                int hashCode;
                int negative;
                int temperature;
                byte current;
                int start;
                while (buffer.hasRemaining()) {
                    start = buffer.position();
                    hashCode = 1;
                    while ((current = buffer.get()) != DELIMITER) {
                        hashCode = 31 * hashCode + current;
                    }
                    size = buffer.position() - start - 1;
                    if ((currentStats = destinations.get(hashCode)) == null) {
                        currentStats = createStats(buffer, hashCode, start, size);
                        destinations.put(hashCode, currentStats);
                    }
                    temperature = 0;
                    negative = 1;
                    while ((current = buffer.get()) != NEWLINE) {
                        if (current == '-') {
                            negative = -1;
                        } else if (current != '.') {
                            temperature = temperature * 10 + current - '0';
                        }
                    }
                    double temp = negative * temperature / 10.0;
                    currentStats.update(temp);
                }
                buffer.clear();
                try {
                    emptyBuffers.put(buffer);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return destinations.values();
        }
    }

    private static List<MappedByteBuffer> createBuffers(FileChannel channel) throws IOException {
        int taskCount = (int) (channel.size() / Integer.MAX_VALUE) + 1;
        int blockSize = (int) (channel.size() / taskCount);
        List<MappedByteBuffer> blocks = new ArrayList<>(taskCount);
        long pos = 0;
        for (int i = 1; i <= taskCount; i++) {
            int size = (int) (i * blockSize - pos);
            if (i == taskCount) {
                size = (int) (channel.size() - pos);
            }
            MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
            while (b.get(--size) != '\n')
                ;
            b.limit(size + 1);
            blocks.add(b);
            pos += size + 1;
        }
        return blocks;
    }

    private static Stats createStats(ByteBuffer buffer, int hashCode, int offset, int size) {
        Stats currentStats;
        byte[] dest = new byte[size];
        System.arraycopy(buffer.array(), offset, dest, 0, size);
        currentStats = new Stats(dest, hashCode);
        return currentStats;
    }

    static class Stats {
        int count;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum;
        byte[] city;
        int hashCode;

        public Stats(byte[] city, int hashCode) {
            this.city = city;
            this.hashCode = hashCode;
        }

        void update(double temp) {
            count += 1;
            sum += temp;
            min = Math.min(min, temp);
            max = Math.max(max, temp);
        }

        Stats join(Stats s) {
            count += s.count;
            sum += s.sum;
            min = Math.min(min, s.min);
            max = Math.max(max, s.max);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return Arrays.equals(city, ((Stats) (o)).city);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }
    }
}
