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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CalculateAverage_mariuslarsen_mapped {
    private static final Byte DELIMITER = ';';
    private static final Byte NEWLINE = '\n';
    private static final int N_THREADS = 8;
    private static final int MAX_NUMBER_OF_DESTINATIONS = 1024;

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
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            try (ExecutorService ex = Executors.newFixedThreadPool(N_THREADS)) {
                var tasks = createBuffers(channel).stream()
                        .map(b -> (Callable<Collection<Stats>>) (() -> parseBuffer(b))).toList();
                var res = ex.invokeAll(tasks).stream()
                        .map(Future::resultNow)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toMap(s -> new String(s.city), Function.identity(), Stats::join, TreeMap::new));
                System.out.println(res);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<MappedByteBuffer> createBuffers(FileChannel channel) throws IOException {
        int taskCount = Math.max(N_THREADS, (int) (channel.size() / Integer.MAX_VALUE) + 1);
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

    private static Collection<Stats> parseBuffer(ByteBuffer buffer) {
        Map<Integer, Stats> destinations = new HashMap<>(MAX_NUMBER_OF_DESTINATIONS);
        Stats currentStats;
        int size;
        int hashCode;
        int negative;
        int temperature;
        byte current;
        int offset;
        while (buffer.hasRemaining()) {
            hashCode = 1;
            offset = buffer.position();
            while ((current = buffer.get()) != DELIMITER) {
                hashCode = 31 * hashCode + current;
            }
            size = buffer.position() - offset - 1;
            if ((currentStats = destinations.get(hashCode)) == null) {
                byte[] newDestination = new byte[size];
                buffer.get(offset, newDestination, 0, size);
                currentStats = new Stats(newDestination, hashCode);
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
            currentStats.update(negative * temperature / 10.0);
        }
        return destinations.values();
    }

    private static Stats createStats(ByteBuffer destination, int hashCode, int offset, int size) {
        byte[] dest = new byte[size];
        destination.get(offset, dest, 0, size);
        return new Stats(dest, hashCode);
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
