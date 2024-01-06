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

// There are 1B record need to read so we cannot create 1B of Objects to process --> out of memory
// 1. There are around 10k+ cities over the world --> max city record is 10K Objects
// 2. Init Max 10K citis at boot times. --> partions them for processors

//--------------------------Process Chunk----------------------------
// 1. Analys problem when split file to multiple pieces
// First Task --> Concat last Line
// Middle Task --> Concat First and Last Line
// Last Task --> Concat Lirst Line
// 2. How to know First Line and Last Line
// Fist Line from 0 to the first \n
// Last Line from last \n to the end of byte capacity
public class CalculateAverage_thanhtrinity {

    private static final String FILE = "./measurements.txt";
    private static final int TOTAL_PROCCESSOR = max(16, Runtime.getRuntime().availableProcessors());

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Num Of Proccessor:" + TOTAL_PROCCESSOR);
        var threads = new Thread[TOTAL_PROCCESSOR];

        var fileChannel = FileChannel.open(Path.of(FILE), READ);
        long fullSize = fileChannel.size();
        System.out.println("FullSize:" + fullSize);

        long standardChunkSize = fullSize / TOTAL_PROCCESSOR;
        System.out.println("standardChunkSize:" + standardChunkSize);

        var cityStats = new CityStats[TOTAL_PROCCESSOR];
        for (int index = 0; index < TOTAL_PROCCESSOR; index++) {
            var pIndex = index;
            var start = pIndex * standardChunkSize;
            // The last chunk will be the remaining
            var end = (pIndex == TOTAL_PROCCESSOR - 1) ? fullSize : start + standardChunkSize;
            var regionSize = end - start;

            // Haved check with virtual thread but it slower than normal thread
            var thread = new Thread(() -> {
                try {
                    var buffer = fileChannel.map(READ_ONLY, start, regionSize);
                    cityStats[pIndex] = processBufferData(buffer, pIndex);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            threads[pIndex] = thread;
        }

        for (var thread : threads) {
            thread.join();
        }

        consolidateStats(cityStats);
    }

    private static void consolidateStats(CityStats[] cityStats) {

        var citiesList = Arrays.stream(cityStats).flatMap(cs -> Arrays.stream(cs.cities).filter(city -> city != null))
                .toList();
        var cities = citiesList.stream().collect(
                Collectors.toMap(
                        city -> new String(city.getName(), StandardCharsets.UTF_8),
                        city -> city,
                        City::combine));

        // Append Header And Footer
        for (int i = 0; i < cityStats.length; i++) {
            if (i > 0) {
                var footer = cityStats[i - 1].footer;
                var header = cityStats[i].header;

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

                var city = new City();
                city.setName(data[0].getBytes());
                city.update(Double.valueOf(data[1]));
                cities.merge(data[0], city, City::combine);
            }
        }

        System.out.println(new TreeMap<>(cities));
    }

    private static CityStats processBufferData(MappedByteBuffer buffer, int taskIdx) {

        var breakLineIndex = 0;
        var semicolonIndex = 0;

        var cities = new City[1000];
        City city = null;
        var isProcessKey = true;
        var hashKey = 0;
        int firstBreakLineIndex = 0;

        double result = 0;
        int integerPart = 0;
        double fractionalPart = 0;
        boolean isFractional = false;
        double divisorForFraction = 1;
        boolean isNegative = false;
        while (buffer.hasRemaining()) {
            var b = buffer.get();

            if (isProcessKey) {
                if (b == ';') {
                    semicolonIndex = buffer.position();
                    int cIdx = Math.abs(hashKey % 1000);
                    isProcessKey = !isProcessKey;
                    city = cities[cIdx];
                    if (city == null) {
                        var name = new byte[semicolonIndex - breakLineIndex];
                        buffer.get(breakLineIndex, name, 0, semicolonIndex - breakLineIndex - 1);
                        cities[cIdx] = city = new City();
                        city.setName(name);
                    }
                    hashKey = 0;
                }
                else if (b == '\n') {
                    hashKey = 0;
                    breakLineIndex = buffer.position();
                    if (firstBreakLineIndex == 0) {
                        firstBreakLineIndex = buffer.position();
                    }
                }
                else {
                    hashKey = 31 * hashKey + b;
                }
            }
            else {
                if (b == '\n') {
                    fractionalPart /= divisorForFraction;
                    result = integerPart + fractionalPart;
                    if (isNegative) {
                        result *= -1;
                    }
                    city.update(result);

                    if (firstBreakLineIndex == 0) {
                        firstBreakLineIndex = buffer.position();
                    }

                    isProcessKey = !isProcessKey;
                    breakLineIndex = buffer.position();
                    hashKey = 0;

                    // reset parameter
                    result = 0;
                    integerPart = 0;
                    fractionalPart = 0;
                    isFractional = false;
                    divisorForFraction = 1;
                    isNegative = false;
                }
                else {
                    if (b == '-') {
                        isNegative = true;
                        continue;
                    }
                    else if (b == '.') {
                        isFractional = true;
                        continue;
                    }

                    if (!isFractional) {
                        integerPart = integerPart * 10 + (b - '0');
                    }
                    else {
                        divisorForFraction *= 10;
                        fractionalPart = fractionalPart * 10 + (b - '0');
                    }
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
            buffer.get(breakLineIndex, byteFooter, 0, buffer.capacity() - breakLineIndex);
        }

        return new CityStats(cities, byteHeader, byteFooter);
    }

    record CityStats(City[] cities, byte[] header, byte[] footer) {
    }

}

class City {
    private byte[] name;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0L;
    private int count = 0;

    public void update(double temp) {
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