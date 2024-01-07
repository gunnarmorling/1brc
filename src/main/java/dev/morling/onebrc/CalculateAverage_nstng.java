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

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_nstng {

    private static final File FILE = new File("./measurements.txt");

    private static class MeasurementAggregator {
        private double min;
        private double max;
        private long sum;
        private long count;

        public MeasurementAggregator(double min, double max, long sum, long count) {

            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        @Override
        public String toString() {
            return "%s/%s/%s".formatted(min, round(sum / 10.0 / count), max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        HashMap<BytesKey, MeasurementAggregator>[] maps;
        Thread[] threads;
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            // calc num threads to use
            int availCpus = Runtime.getRuntime().availableProcessors();
            // make sure to use at least num of threads so that byte chunks fit into an array
            long fileLength = raf.length();
            int minThreads = (int) Math.ceil(fileLength / (1.0 * Integer.MAX_VALUE));
            int numThreads = Math.max(minThreads, availCpus);

            // create threads - each thread processes a chunk of the file
            // the chunks have all equal size (modulo take care that a chunk does not
            // start/end in the middle of a line)
            threads = new Thread[numThreads];
            maps = new HashMap[numThreads];
            long chunkSize = fileLength / numThreads;
            long lastChunkEnd = 0;
            for (int i = 0; i < numThreads; i++) {
                raf.seek(Math.min(lastChunkEnd + chunkSize, fileLength));
                readUntilLineBreak(raf);
                long chunkEnd = raf.getFilePointer();
                long finalLastChunkEnd = lastChunkEnd;
                // 413 possible cities
                maps[i] = new HashMap<>(413);
                HashMap<BytesKey, MeasurementAggregator> finalMap = maps[i];
                threads[i] = new Thread(() -> {
                    try {
                        // do not work on empty chunks
                        if (chunkEnd - finalLastChunkEnd <= 0)
                            return;
                        // -1: left from found line break
                        processChunk(finalLastChunkEnd, chunkEnd - 1, finalMap);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                lastChunkEnd = chunkEnd;
            }

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        // run it ...
        for (Thread thread : threads) {
            thread.start();
        }
        // ... and wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // merge results
        TreeMap<String, MeasurementAggregator> resultMap = new TreeMap<>();
        for (HashMap<BytesKey, MeasurementAggregator> map : maps) {
            for (Map.Entry<BytesKey, MeasurementAggregator> entry : map.entrySet()) {
                String stringKey = entry.getKey().asString();
                MeasurementAggregator agg = resultMap.get(stringKey);
                if (agg == null) {
                    resultMap.put(stringKey, entry.getValue());
                }
                else {
                    agg.min = Math.min(entry.getValue().min, agg.min);
                    agg.max = Math.max(entry.getValue().max, agg.max);
                    agg.count = entry.getValue().count + agg.count;
                    agg.sum = entry.getValue().sum + agg.sum;
                }
            }
        }
        System.out.println(resultMap);
    }

    private static void processChunk(long fromPos, long toPos, HashMap<BytesKey, MeasurementAggregator> map)
            throws IOException {

        byte[] byteChunk = new byte[(int) (toPos - fromPos)];
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            raf.seek(fromPos);
            raf.read(byteChunk);
        }
        IndexedByteArray chunk = new IndexedByteArray(byteChunk);

        // reads one line from right to left in each iteration
        while (chunk.pos > 0) {
            double mes = getMeasurement(chunk);
            BytesKey cityKey = getCityKey(chunk);
            addOrUpdateEntry(map, cityKey, mes);
        }
    }

    private static void addOrUpdateEntry(HashMap<BytesKey, MeasurementAggregator> map, BytesKey cityKey, double mes) {
        MeasurementAggregator agg = map.get(cityKey);
        if (agg == null) {
            map.put(cityKey, new MeasurementAggregator(mes, mes, (long) (mes * 10), 1));
        }
        else {
            agg.min = Math.min(agg.min, mes);
            agg.max = Math.max(agg.max, mes);
            agg.sum += (long) (mes * 10);
            agg.count++;
        }
    }

    // did not know how much of a bottleneck new String(byteArray) can be :)
    // use the byte array representation of a city as a HashMap key
    // needs to be wrapped so that hashCode/equals can be defined
    private static BytesKey getCityKey(IndexedByteArray chunk) {
        // city name length >=3 known
        int cityLength = 3;
        chunk.dec(3);
        while (chunk.pos >= 0 && chunk.getCurrent() != '\n') {
            chunk.dec();
            cityLength++;
        }
        byte[] cityBytes = new byte[cityLength];
        // we are on '\n' or out of bounds by -1 -> srcPos is chunk.pos + 1
        System.arraycopy(chunk.array, chunk.pos + 1, cityBytes, 0, cityLength);
        chunk.dec(); // move away from '\n' (or further out of bounds)

        return new BytesKey(cityBytes);
    }

    // did not know how much of a bottleneck Double.parseDouble(s) can be :)
    // parses a double value from right to left
    // massively uses that a measurement value is very restricted
    // (<100, >-100, exactly one decimal place)
    private static double getMeasurement(IndexedByteArray chunk) {
        int asciiNumbersOffset = 48;
        double mes = (chunk.getCurrent() - asciiNumbersOffset) / 10.0; // 10^-1 place
        chunk.dec(2); // jump over '.'
        mes += chunk.getCurrent() - asciiNumbersOffset; // 10^0 place
        chunk.dec();
        switch (chunk.getCurrent()) {
            case '-' -> {
                mes *= -1; // <10 and negative
                chunk.dec();
            }
            case ';' -> {
                // do nothing - we will move to the left at the end
            }
            default -> {
                mes += (chunk.getCurrent() - asciiNumbersOffset) * 10; // 10^1 place
                chunk.dec();
            }
        }
        if (chunk.getCurrent() == '-') {
            mes *= -1; // >=10 and negative
            chunk.dec();
        }
        chunk.dec(); // move away from ';'
        return mes;
    }

    // just a thin wrapper around a byte array - makes handling return values and
    // position updates more easy
    private static class IndexedByteArray {
        private final byte[] array;
        private int pos;

        public IndexedByteArray(byte[] array) {

            this.array = array;
            this.pos = array.length - 1;
        }

        public void dec(int by) {
            pos -= by;
        }

        public void dec() {
            pos--;
        }

        public int getCurrent() {
            return array[pos];
        }
    }

    private static void readUntilLineBreak(RandomAccessFile raf) throws IOException {
        boolean eor = false;

        while (!eor) {
            int c = raf.read();
            if (c == -1 || c == '\n') {
                eor = true;
            }
        }
    }

    // inspired by https://www.baeldung.com/java-map-key-byte-array
    private static class BytesKey {
        private final byte[] array;
        private int hash;

        public BytesKey(byte[] array) {
            this.array = array;
            // pre-calculated value with perfect hash function (by trial and error :P)
            // -> i.e., can be used also for equals
            this.hash = 1;
            for (byte b : array)
                this.hash = 11 * this.hash + b;
        }

        @Override
        public boolean equals(Object o) {
            return hash == o.hashCode();
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public String asString() {
            return new String(array);
        }
    }
}
