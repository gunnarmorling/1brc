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

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class CalculateAverage_mahadev_k {

    private static final String FILE = "./measurements.txt";

    private static Map<String, MeasurementAggregator> stationMap = new ConcurrentSkipListMap<>();

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static class MeasurementAggregator {
        double minima = Double.POSITIVE_INFINITY, maxima = Double.NEGATIVE_INFINITY, total = 0, count = 0;

        public synchronized void accept(double value) {
            if (minima > value)
                minima = value;
            if (maxima < value)
                maxima = value;
            total += value;
            count++;
        }

        public double min() {
            return round(minima);
        }

        public double max() {
            return round(maxima);
        }

        public double avg() {
            return round((Math.round(total * 10.0) / 10.0) / count);
        }
    }

    public static void main(String[] args) throws IOException {
        int chunkSize = args.length == 1 ? Integer.parseInt(args[0]) : 1_000_000;
        readAndProcess(chunkSize);
        print();
    }

    public static void readAndProcess(int chunkSize) {
        final ThreadFactory factory = Thread.ofVirtual().name("routine-", 0).factory();

        try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {
            try (var executor = Executors.newThreadPerTaskExecutor(factory)) {

                var channel = file.getChannel();
                var size = channel.size();
                long start = 0;
                while (start <= size) {
                    long end = start + chunkSize;
                    String letter = "";
                    do {
                        end--;
                        ByteBuffer buffer = ByteBuffer.allocate(1);
                        channel.read(buffer, end);
                        buffer.flip();
                        letter = StandardCharsets.UTF_8.decode(buffer).toString();
                    } while (!letter.equals("\n"));

                    if (end < start)
                        end = start + chunkSize;

                    final long currentStart = start;
                    final long currentEnd = end;
                    executor.submit(() -> {
                        ByteBuffer buffer = ByteBuffer.allocate((int) (currentEnd - currentStart + 1));
                        try {
                            channel.read(buffer, currentStart);
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                        buffer.flip();
                        String data = StandardCharsets.UTF_8.decode(buffer).toString();
                        processData(data);
                    });
                    start = end + 1;
                }
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void processData(String dataBlock) {
        StringTokenizer tokenizer = new StringTokenizer(dataBlock, "\n");
        while (tokenizer.hasMoreElements()) {
            StringTokenizer tokens = new StringTokenizer(tokenizer.nextToken(), ";");
            String station = tokens.nextToken();
            double value = Double.parseDouble(tokens.nextToken());
            processMinMaxMean(station, value);
        }
    }

    private static void processMinMaxMean(String station, double temp) {
        var values = stationMap.get(station);
        if (values == null) {
            values = new MeasurementAggregator();
            stationMap.putIfAbsent(station, values);
        }
        values = stationMap.get(station);
        values.accept(temp);
    }

    public static void print() throws UnsupportedEncodingException {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.out.print("{");
        int i = stationMap.size();
        for (var kv : stationMap.entrySet()) {
            System.out.printf("%s=%s/%s/%s", kv.getKey(), kv.getValue().min(), kv.getValue().avg(), kv.getValue().max());
            if (i > 1)
                System.out.print(", ");
            i--;
        }
        System.out.println("}");
    }
}