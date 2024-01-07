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
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_thanhtrinity {

    private static final String FILE = "./measurements.txt";
    private static final int TOTAL_PROCCESSOR = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Num Of Proccessor:" + TOTAL_PROCCESSOR);
        var threads = new Thread[TOTAL_PROCCESSOR];

        var fileChannel = FileChannel.open(Path.of(FILE), READ);
        long fullSize = fileChannel.size();
        System.out.println("FullSize:" + fullSize);

        long standardChunkSize = fullSize / TOTAL_PROCCESSOR;
        System.out.println("StandardChunkSize:" + standardChunkSize);

        var CitiesTempChunk = new CitiesTempChunk[TOTAL_PROCCESSOR];
        for (int index = 0; index < TOTAL_PROCCESSOR; index++) {
            var pIndex = index;
            var start = pIndex * standardChunkSize;
            // The last chunk will be the remaining
            var end = (pIndex == TOTAL_PROCCESSOR - 1) ? fullSize : start + standardChunkSize;
            var chunkSize = end - start;

            // Haved check with virtual thread but it slower than normal thread
            var thread = new Thread(() -> {
                try {
                    var buffer = fileChannel.map(READ_ONLY, start, chunkSize);
                    CitiesTempChunk[pIndex] = processBufferData(buffer, pIndex);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            threads[pIndex] = thread;
        }

        for (var thread : threads) {
            thread.join();
        }

        consolidateStats(CitiesTempChunk);
    }

    private static void consolidateStats(CitiesTempChunk[] CitiesTempChunk) {

        var citiesList = Arrays.stream(CitiesTempChunk)
                .flatMap(cs -> Arrays.stream(cs.cities).filter(city -> city != null))
                .toList();

        System.out.println("Count:" + citiesList.size());

        var cities = citiesList.stream().collect(
                Collectors.toMap(
                        city -> new String(city.getName(), StandardCharsets.UTF_8),
                        city -> city,
                        City::combine));

        // Append Header And Footer
        for (int i = 0; i < CitiesTempChunk.length; i++) {
            if (i > 0) {
                var footer = CitiesTempChunk[i - 1].footer;
                var header = CitiesTempChunk[i].header;

                var footerSize = footer != null ? footer.length : 0;
                var headerSize = header != null ? header.length : 0;

                var buffer = ByteBuffer.allocate(footerSize + headerSize);
                if (footer != null) {
                    buffer.put(footer);
                }
                if (header != null) {
                    buffer.put(header);
                }

                var data = new String(buffer.array(), StandardCharsets.UTF_8).split(";");

                var city = new City(data[0].getBytes());
                city.updateTempurature(Double.valueOf(data[1]));
                cities.merge(data[0], city, City::combine);
            }
        }

        System.out.println(new TreeMap<>(cities));
    }

    private static CitiesTempChunk processBufferData(MappedByteBuffer buffer, int taskIdx) {

        var breakLineIndex = 0;
        var semicolonIndex = 0;

        var cities = new City[1000];
        City city = null;
        var isProcessKey = true;
        var hashKey = 0;
        var firstBreakLineIndex = 0;
        var pro = new DataProcessor();
        while (buffer.hasRemaining()) {
            var b = buffer.get();
            var position = buffer.position();

            if (b == '\n') {
                breakLineIndex = position;
                if (firstBreakLineIndex == 0) {
                    firstBreakLineIndex = position;
                }
                hashKey = 0;
            }

            if (isProcessKey) {
                if (b == ';') {
                    semicolonIndex = position;
                    int cIdx = Math.abs(hashKey % 1000);
                    city = cities[cIdx];
                    if (city == null) {
                        var name = new byte[semicolonIndex - breakLineIndex];
                        buffer.get(breakLineIndex, name, 0, semicolonIndex - breakLineIndex - 1);
                        cities[cIdx] = city = new City(name);
                    }
                    hashKey = 0;
                    isProcessKey = false;
                } else {
                    hashKey = 31 * hashKey + b;
                }
            } else {
                if (b == '\n') {
                    isProcessKey = true;
                    var tem = pro.calculateTempurature();
                    city.updateTempurature(tem);
                    // reset parameter
                    pro.resetParsingParams();
                } else {
                    pro.updateParsingParams(b);
                }
            }
        }

        byte[] byteHeader = null;
        byte[] byteFooter = null;
        // Get Header And Footer byte[]
        if (taskIdx != 0) {
            byteHeader = new byte[firstBreakLineIndex];
            buffer.get(0, byteHeader, 0, firstBreakLineIndex - 1);
        }

        if (buffer.capacity() > breakLineIndex) {
            byteFooter = new byte[buffer.capacity() - breakLineIndex];
            buffer.get(breakLineIndex, byteFooter, 0, buffer.capacity() -
                    breakLineIndex);
        }

        return new CitiesTempChunk(cities, byteHeader, byteFooter);
    }

    record CitiesTempChunk(City[] cities, byte[] header, byte[] footer) {
    }

}

class DataProcessor {
    // Tempurature Parsing Param
    private double result = 0;
    private int integerPart = 0;
    private double fractionalPart = 0;
    private boolean isFractional = false;
    private double divisorForFraction = 1;
    private boolean isNegative = false;

    public double calculateTempurature() {
        fractionalPart /= divisorForFraction;
        result = integerPart + fractionalPart;
        if (isNegative) {
            result *= -1;
        }
        return result;
    }

    public void updateParsingParams(byte b) {
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
                } else {
                    divisorForFraction *= 10;
                    fractionalPart = fractionalPart * 10 + (b - '0');
                }
                break;
        }

    }

    public void resetParsingParams() {
        result = 0;
        integerPart = 0;
        fractionalPart = 0;
        isFractional = false;
        divisorForFraction = 1;
        isNegative = false;
    }
}

class City {
    private byte[] name;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0L;
    private int count = 0;

    public City() {
    }

    public City(byte[] name) {
        this.name = name;
    }

    public void updateTempurature(double temp) {
        min = min(min, temp);
        max = max(max, temp);
        sum += temp;
        count++;
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
        return roundNumber(min) + "/" + roundNumber((sum) / count) + "/" + roundNumber(max);
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte[] getName() {
        return this.name;
    }

    private double roundNumber(double value) {
        return round(value * 10.0) / 10.0;
    }
}