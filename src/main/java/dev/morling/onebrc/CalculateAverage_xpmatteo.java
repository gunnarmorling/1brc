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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

@SuppressWarnings({ "ReassignedVariable", "StatementWithEmptyBody" })
public class CalculateAverage_xpmatteo {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException, InterruptedException {
        var fileName = dataFileName(args);

        try (
                var file = new RandomAccessFile(new File(fileName), "r");
                var channel = file.getChannel()) {
            var numCpus = Runtime.getRuntime().availableProcessors();
            var threads = split(channel, numCpus).stream()
                    .map(Worker::new)
                    .toList();
            threads.forEach(Thread::start);
            for (Worker thread : threads) {
                thread.join();
            }
            var results = threads.stream().map(Worker::getResults)
                    .reduce(CalculateAverage_xpmatteo::merge)
                    .orElseThrow();
            printCities(results);
        }
    }

    public static class Worker extends Thread {
        private final ByteBuffer buffer;
        private Results results;

        public Worker(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void run() {
            this.results = parseData(this.buffer);
        }

        public Results getResults() {
            return results;
        }
    }

    protected static List<ByteBuffer> split(FileChannel channel, int numCpus) throws IOException {
        if (channel.size() < 10_000) {
            return List.of(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()));
        }

        long[] increments = new long[numCpus + 1];
        for (int i = 0; i < numCpus; i++) {
            increments[i] = i * channel.size() / numCpus;
            // adjust the increments so that they start on the beginning of a city
            while (increments[i] > 0 && byteAt(channel, increments[i] - 1) != '\n') {
                increments[i]--;
            }
        }
        increments[numCpus] = channel.size();

        List<ByteBuffer> result = new ArrayList<>(numCpus);
        for (int i = 0; i < numCpus; i++) {
            long from = increments[i];
            long to = increments[i + 1];
            result.add(channel.map(FileChannel.MapMode.READ_ONLY, from, to - from));
        }
        return result;
    }

    private static byte byteAt(FileChannel channel, long offset) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        channel.position(offset);
        channel.read(buf);
        buf.flip();
        var bytes = new byte[1];
        buf.get(bytes);
        return bytes[0];
    }

    public static String dataFileName(String[] args) {
        if (args.length == 1) {
            return args[0];
        }
        return FILE;
    }

    protected static byte[] readAllData(String fileName) throws IOException {
        return Files.readAllBytes(Path.of(fileName));
    }

    protected static ByteBuffer memoryMap(String fileName) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(new File(fileName), "r")) {
            // Get file channel in read-only mode
            FileChannel fileChannel = file.getChannel();

            return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        }
    }

    protected enum State {
        PARSING_CITY_NAME,
        SKIPPING_SEMICOLON,
        PARSING_TEMPERATURE
    }

    protected static Results parseData(ByteBuffer data) {
        var results = new Results();
        var state = State.PARSING_CITY_NAME;
        int cityStartOffset = 0, cityEndOffset = 0;
        int temp = 0, sign = 0;

        for (int i = 0; i < data.limit(); i++) {
            byte currentChar = data.get();
            if (state == State.PARSING_CITY_NAME && currentChar == ';') {
                state = State.SKIPPING_SEMICOLON;
                cityEndOffset = i;
            }
            else if (state == State.PARSING_CITY_NAME) {
                // do nothing
            }
            else if (state == State.SKIPPING_SEMICOLON && currentChar == '-') {
                state = State.PARSING_TEMPERATURE;
                temp = 0;
                sign = -1;
            }
            else if (state == State.SKIPPING_SEMICOLON && currentChar >= '0' && currentChar <= '9') {
                state = State.PARSING_TEMPERATURE;
                temp = currentChar - '0';
                sign = 1;
            }
            else if (state == State.PARSING_TEMPERATURE && currentChar >= '0' && currentChar <= '9') {
                temp = temp * 10 + currentChar - '0';
            }
            else if (state == State.PARSING_TEMPERATURE && currentChar == '.') {
                // do nothing
            }
            else if (state == State.PARSING_TEMPERATURE && currentChar == '\n') {
                byte[] bytes = new byte[cityEndOffset - cityStartOffset];
                data.get(cityStartOffset, bytes);
                var cityName = new String(bytes);
                accumulate(results, cityName, temp * sign);
                state = State.PARSING_CITY_NAME;
                cityStartOffset = i + 1;
            }
        }

        return results;
    }

    private static void accumulate(Results results, String cityName, int tempTimesTen) {
        var existing = results.get(cityName);
        if (existing == null) {
            results.put(cityName, new CityData(tempTimesTen, tempTimesTen, tempTimesTen, 1));
        }
        else {
            existing.min = Math.min(existing.min, tempTimesTen);
            existing.sum = existing.sum + tempTimesTen;
            existing.max = Math.max(existing.max, tempTimesTen);
            existing.count++;
        }
    }

    protected static Results merge(Results a, Results b) {
        for (var entry : b.entrySet()) {
            CityData valueInA = a.get(entry.getKey());
            if (null == valueInA) {
                a.put(entry.getKey(), entry.getValue());
            }
            else {
                var valueInB = entry.getValue();
                valueInA.min = Math.min(valueInA.min, valueInB.min);
                valueInA.sum += valueInB.sum;
                valueInA.max = Math.max(valueInA.max, valueInB.max);
                valueInA.count += valueInB.count;
            }
        }

        return a;
    }

    protected static class Results extends TreeMap<String, CityData> {

    }

    protected static class CityData {
        int min, sum, max, count;

        public CityData(int min, int sum, int max, int count) {
            this.min = min;
            this.sum = sum;
            this.max = max;
            this.count = count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CityData cityData = (CityData) o;
            return min == cityData.min && sum == cityData.sum && max == cityData.max && count == cityData.count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, sum, max, count);
        }

        @Override
        public String toString() {
            return STR."CityData{min=\{min}, sum=\{sum}, max=\{max}, count=\{count}\{'}'}";
        }
    }

    protected static void printCities(Results cities) {
        System.out.print("{");
        for (String city : cities.keySet()) {
            CityData data = cities.get(city);
            var min = data.min / 10.0;
            var mean = (data.sum * 10.0 / data.count) / 100.0;
            var max = data.max / 10.0;
            System.out.printf(
                    "%s=%.1f/%.1f/%.1f, ",
                    city,
                    min,
                    mean,
                    max);
        }
        System.out.print("}");
    }
}
