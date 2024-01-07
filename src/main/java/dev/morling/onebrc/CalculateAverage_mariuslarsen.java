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
import java.util.function.Function;
import java.util.stream.Collectors;

public class CalculateAverage_mariuslarsen {
    private static final Byte DELIMITER = ';';
    private static final Byte NEWLINE = '\n';
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int MAX_BYTES_IN_MEASUREMENT = 5;
    private static final int MAX_BYTES_IN_DESTINATION = 4 * 100;
    private static final int MAX_NUMBER_OF_DESTINATIONS = 512;

    public static void main(String[] args) throws IOException {
        Path path = Path.of("./measurements.txt");

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
            Map<String, Stats> res = createBuffers(channel).parallelStream()
                    .map(CalculateAverage_mariuslarsen::parseBuffer)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(s -> s.city, Function.identity(), Stats::join, TreeMap::new));
            System.out.println(res);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<MappedByteBuffer> createBuffers(FileChannel channel) throws IOException {
        int taskCount = Math.max(N_THREADS, (int) (channel.size() / Integer.MAX_VALUE));
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

    private static Collection<Stats> parseBuffer(MappedByteBuffer buffer) {
        ByteBuffer destination = ByteBuffer.allocate(MAX_BYTES_IN_DESTINATION);
        ByteBuffer measurement = ByteBuffer.allocate(MAX_BYTES_IN_MEASUREMENT);
        ByteBuffer currentBuffer = destination;
        Map<Integer, Stats> destinations = new HashMap<>(MAX_NUMBER_OF_DESTINATIONS);
        int hashCode = 0;
        int destinationHash;
        int negative;
        int temperature;
        byte digit;
        byte current;
        Stats currentStats = null;
        while (buffer.hasRemaining()) {
            current = buffer.get();
            if (current == DELIMITER) {
                destinationHash = hashCode;
                if ((currentStats = destinations.get(destinationHash)) == null) {
                    currentStats = new Stats(new String(destination.array(), 0, destination.position()));
                    destinations.put(destinationHash, currentStats);
                }
                currentBuffer.clear();
                currentBuffer = measurement;
            }
            else if (current == NEWLINE) {
                currentBuffer.flip();
                negative = 1;
                temperature = 0;
                while (currentBuffer.hasRemaining()) {
                    digit = currentBuffer.get();
                    if (digit == '-') {
                        negative = -1;
                    }
                    else if (digit != '.') {
                        temperature = temperature * 10 + digit - '0';
                    }
                }
                currentStats.update(negative * temperature / 10.0);
                currentBuffer.clear();
                currentBuffer = destination;
                hashCode = 0;
            }
            else {
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
        return round(min) + "/" + round(sum / count) + "/" + round(max);
    }
}
