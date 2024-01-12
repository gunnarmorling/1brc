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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_thanhtrinity {

    private static final String FILE = "./measurements.txt";
    private static final int TOTAL_PROCESSOR = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException, InterruptedException {

        // System.out.println("Num Of Processor:" + TOTAL_PROCESSOR);
        var threads = new ArrayList<Thread>();

        var fileChannel = FileChannel.open(Path.of(FILE), READ);
        long fullSize = fileChannel.size();
        // System.out.println("FullSize:" + fullSize);

        var standardChunkSize = fullSize / TOTAL_PROCESSOR;
        // System.out.println("StandardChunkSize:" + standardChunkSize);
        var chunkDataList = new ChunkData[TOTAL_PROCESSOR];

        var start = 0L;
        var end = standardChunkSize;
        for (int index = 0; index < TOTAL_PROCESSOR; index++) {
            long newStart = start;
            end = adjustBreakLinePosition(start + standardChunkSize) + 1;
            end = end >= fullSize ? fullSize : end;
            var chunkSize = end - start;

            // Have checked with virtual thread but it slower than normal thread
            int taskIdx = index;
            var thread = new Thread(() -> {
                try (var file = new RandomAccessFile(FILE, "r");
                        var fc = file.getChannel()) {
                    var buffer = fc.map(READ_ONLY, newStart, chunkSize);
                    chunkDataList[taskIdx] = processBufferData(buffer, taskIdx + 1);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            threads.add(thread);
            if (end == fullSize) {
                break;
            }
            start = end;
        }

        for (var thread : threads) {
            thread.join();
        }
        consolidateData(chunkDataList);
    }

    private static long adjustBreakLinePosition(long position) throws IOException {
        try (var file = new RandomAccessFile(FILE, "r")) {
            file.seek(position);
            while (file.read() != '\n' && position < file.length()) {
                position++;
            }
        }
        return position;

    }

    private static ChunkData processBufferData(MappedByteBuffer buffer, long taskIdx) {

        int currentIdx = 0;
        int breakLineIndex = 0;

        final int capacity = 100000;
        var cities = new City[capacity];
        var isProcessKey = true;
        var hashKey = 0;
        double result = 0;
        int integerPart = 0;
        double fractionalPart = 0;
        boolean isFractional = false;
        double divisorForFraction = 1;
        boolean isNegative = false;
        for (int i = 0; i < buffer.limit(); i++) {
            var b = buffer.get();
            var position = i + 1;
            if (isProcessKey) {
                if (b == ';') {
                    hashKey = hashKey % capacity;
                    currentIdx = hashKey;
                    var name = new byte[position - breakLineIndex];
                    buffer.get(breakLineIndex, name, 0, position - breakLineIndex - 1);
                    if (cities[currentIdx] == null) {
                        cities[currentIdx] = new City(name);
                    }
                    else if (!Arrays.equals(cities[currentIdx].getName(), name)) {
                        while (cities[currentIdx] != null) { // Continue probing until empty slot
                            currentIdx = (currentIdx + 1) % capacity;
                        }
                        cities[currentIdx] = new City(name);
                    }
                    hashKey = 0;
                    isProcessKey = false;
                }
                else {
                    hashKey = (31 * hashKey + b) & 0x7FFFFFFF;
                }
            }
            else {
                if (b == '\n') {
                    fractionalPart /= divisorForFraction;
                    result = integerPart + fractionalPart;
                    if (isNegative) {
                        result *= -1;
                    }
                    cities[currentIdx].updateTemp(result);

                    breakLineIndex = position;
                    // reset parameter
                    hashKey = 0;
                    result = 0;
                    integerPart = 0;
                    fractionalPart = 0;
                    isFractional = false;
                    divisorForFraction = 1;
                    isNegative = false;
                    isProcessKey = true;
                }
                else {
                    switch (b) {
                        case '-':
                            isNegative = true;
                            break;
                        case '.':
                            isFractional = true;
                            break;
                        default:
                            if (!isFractional) {
                                integerPart = integerPart * 10 + (b - '0');
                            }
                            else {
                                divisorForFraction *= 10;
                                fractionalPart = fractionalPart * 10 + (b - '0');
                            }
                            break;
                    }
                }
            }

        }
        buffer = null;
        System.gc();
        var citiesList = Arrays.stream(cities).filter(Objects::nonNull).toList();
        return new ChunkData(citiesList);
    }

    private static void consolidateData(ChunkData[] citiesTempChunk) {

        var cities = Arrays.stream(citiesTempChunk).filter(Objects::nonNull)
                .flatMap(chunkData -> chunkData.cities().stream())
                .collect(
                        Collectors.toMap(
                                City::getKey,
                                city -> city,
                                City::combine));
        System.out.println(new TreeMap<>(cities));
    }

    record ChunkData(List<City> cities) {
    }
}

class City {
    private byte[] name;
    private String key;
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;
    private double sum = 0L;
    private int count = 0;

    public City() {
    }

    public City(byte[] name) {
        this.name = name;
    }

    public void updateTemp(double temp) {
        min = min(min, temp);
        max = max(max, temp);
        sum += temp;
        count++;
    }

    public String getKey() {
        int i = 0;
        while (i < name.length && name[i] != 0) {
            i++;
        }
        key = new String(name, 0, i, UTF_8);
        return key;
    }

    public static City combine(City t1, City t2) {
        City result = new City();
        result.min = min(t1.min, t2.min);
        result.max = max(t1.max, t2.max);
        result.sum = t1.sum + t2.sum;
        result.count = t1.count + t2.count;
        return result;
    }

    @Override
    public String toString() {
        return roundNumber(min) + "/" + roundNumber(sum / count) + "/" + roundNumber(max);
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte[] getName() {
        return this.name;
    }

    private static double roundNumber(double value) {
        return round(value * 10.0) / 10.0;
    }
}