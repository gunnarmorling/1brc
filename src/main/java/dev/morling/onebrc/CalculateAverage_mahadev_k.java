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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

class ConcurrentTreeMap<K, V> extends ConcurrentSkipListMap<K, V> {
    @Override
    public synchronized V putIfAbsent(K key, V value) {
        return super.putIfAbsent(key, value);
    }

    // @Override
    // public synchronized V get(Object key) {
    // return super.get(key);
    // }
}

public class CalculateAverage_mahadev_k {

    private static final String FILE = "./measurements.txt";

    private static List<Map<String, MeasurementAggregator>> intermediateList = new Vector<>();

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static class MeasurementAggregator {
        double minima = Double.POSITIVE_INFINITY, maxima = Double.NEGATIVE_INFINITY, total = 0, count = 0;

        public MeasurementAggregator(double temp) {
            accept(temp);
        }

        private void accept(double value) {
            minima = Math.min(minima, value);
            maxima = Math.max(maxima, value);
            total += value;
            count++;
        }

        public void merge(MeasurementAggregator agg) {
            minima = Math.min(minima, agg.minima);
            maxima = Math.max(maxima, agg.maxima);
            total += agg.total;
            count += agg.count;
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
                        ByteBuffer buffer = ByteBuffer.allocate((int) (currentEnd - currentStart));
                        try {
                            channel.read(buffer, currentStart);
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                        buffer.flip();
                        String data = StandardCharsets.UTF_8.decode(buffer).toString();
                        executor.submit(() -> processData(data));
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
        Map<String, MeasurementAggregator> intermediateAgg = new HashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(dataBlock, "\n");
        while (tokenizer.hasMoreElements()) {
            StringTokenizer tokens = new StringTokenizer(tokenizer.nextToken(), ";");
            String station = tokens.nextToken();
            double temp = Double.parseDouble(tokens.nextToken());
            // processMinMaxMean(station, temp);
            var agg = new MeasurementAggregator(temp);
            var value = intermediateAgg.getOrDefault(station, agg);
            if (value != agg) {
                value.merge(agg);
            }
            intermediateAgg.put(station, value);
        }
        intermediateList.add(intermediateAgg);
    }

    // private static void processMinMaxMean(String station, double temp) {
    // if (!stationMap.containsKey(station)) {
    // stationMap.putIfAbsent(station, new MeasurementAggregator(temp));
    // }
    // var values = stationMap.get(station);
    // values.accept(temp);
    // }

    public static void print() throws UnsupportedEncodingException {
        Map<String, MeasurementAggregator> aggMap = new TreeMap<>();
        for (var intermediateAgg : intermediateList) {
            for (var kv : intermediateAgg.entrySet()) {
                var aggVal = aggMap.getOrDefault(kv.getKey(), kv.getValue());
                if (aggVal != kv.getValue()) {
                    aggVal.merge(kv.getValue());
                }
                aggMap.put(kv.getKey(), aggVal);
            }
        }
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.out.print("{");
        int i = aggMap.size();
        for (var kv : aggMap.entrySet()) {
            System.out.printf("%s=%s/%s/%s", kv.getKey(), kv.getValue().min(), kv.getValue().avg(), kv.getValue().max());
            if (i > 1)
                System.out.print(", ");
            i--;
        }
        System.out.printf("}\n");
    }
}
