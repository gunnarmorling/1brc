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

public class CalculateAverage_mariuslarsen {
    private static final Path path = Path.of("./measurements.txt");
    private static final Byte DELIMITER = ';';
    private static final Byte NEWLINE = '\n';
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int MAX_BYTES_IN_MEASUREMENT = 5;
    private static final int MAX_BYTES_IN_DESTINATION = 4 * 100;
    private static final int MAX_NUMBER_OF_DESTINATIONS = 10000;
    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(THREAD_COUNT);

    public static void main(String[] args) throws IOException {
        readMeasurements();
    }

    private static void readMeasurements() {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            MappedByteBuffer[] blocks = createBuffers(channel);
            var tasks = Arrays.stream(blocks)
                    .map(buffer -> (Callable<Collection<Stats>>) (() -> parseBuffer(buffer)))
                    .toList();

            Map<String, Stats> res = THREAD_POOL.invokeAll(tasks).stream()
                    .map(Future::resultNow)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(s -> s.city, Function.identity(), Stats::join, TreeMap::new));

            System.out.println(res);
            THREAD_POOL.shutdown();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static MappedByteBuffer[] createBuffers(FileChannel channel) throws IOException {
        int taskCount = Math.max(THREAD_COUNT, (int) (channel.size() / Integer.MAX_VALUE));
        int blockSize = (int) (channel.size() / taskCount);
        MappedByteBuffer[] blocks = new MappedByteBuffer[taskCount];
        long pos = 0;
        for (int i = 1; i <= taskCount; i++) {
            int size = (int) (i * blockSize - pos);
            if (i == taskCount) {
                size = (int) (channel.size() - pos);
            }
            MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
            int j = size;
            while (b.get(--j) != '\n')
                ;
            b.limit(j + 1);
            pos += j + 1;
            blocks[i - 1] = b;
        }
        return blocks;
    }

    private static Collection<Stats> parseBuffer(MappedByteBuffer buffer) {
        ByteBuffer destination = ByteBuffer.allocate(MAX_BYTES_IN_DESTINATION);
        ByteBuffer measurement = ByteBuffer.allocate(MAX_BYTES_IN_MEASUREMENT);
        ByteBuffer currentBuffer = destination;
        Map<Integer, Stats> destinations = HashMap.newHashMap(MAX_NUMBER_OF_DESTINATIONS);
        int hashCode = 0;
        int destinationHash = 0;
        int negative = 1;
        int temp = 0;

        byte current;
        while (buffer.hasRemaining()) {
            current = buffer.get();
            if (current == DELIMITER) {
                destinationHash = hashCode;
                destinations.computeIfAbsent(destinationHash, k -> new Stats(new String(destination.array(), 0, destination.position())));
                currentBuffer.flip();
                currentBuffer.clear();
                currentBuffer = measurement;
            } else if (current == NEWLINE) {
                currentBuffer.flip();
                while (currentBuffer.hasRemaining()) {
                    byte d = currentBuffer.get();
                    if (d == '-') {
                        negative = -1;
                    } else if (d != '.') {
                        temp = temp * 10 + d - '0';
                    }
                }
                destinations.get(destinationHash).update(negative * temp / 10.0);
                currentBuffer.clear();
                currentBuffer = destination;
                negative = 1;
                destinationHash = 0;
                temp = 0;
                hashCode = 0;
            } else {
                currentBuffer.put(current);
                hashCode = 31 * hashCode + current;
            }
        }
        return destinations.values();
    }
}

class Stats {
    int count;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    double sum;
    String city;

    public Stats(String city) {
        this.city = city;
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

    private double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    @Override
    public String toString() {
        return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
    }
}
