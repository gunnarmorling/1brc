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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.LongStream;

public class CalculateAverage_lawrey {

    // Path to the file containing temperature measurements.
    private static final String FILE = "./measurements.txt";

    // Inner class representing a measurement with min, max, and average calculations.
    static class Measurement {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0.0;
        long count = 0;

        // Default constructor for Measurement.
        public Measurement() {
        }

        // Adds a new temperature sample and updates min, max, and average.
        public void sample(double temp) {
            min = Math.min(min, temp);
            max = Math.max(max, temp);
            sum += temp;
            count++;
        }

        // Returns a formatted string representing min, average, and max.
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        // Helper method to round a double value to one decimal place.
        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // Merges this Measurement with another Measurement.
        public Measurement merge(Measurement m2) {
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
            count += m2.count;
            return this;
        }
    }

    // Inner class representing a key for measurements.
    static class Key {
        final byte[] data = new byte[32];
        int hash = 0, length = 0;

        // Override of hashCode using the hash field.
        @Override
        public int hashCode() {
            return hash;
        }

        // Custom equals method to compare two Key objects.
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key k) {
                if (length != k.length || hash != k.hash)
                    return false;
                return Arrays.equals(data, 0, length, k.data, 0, length);
            }
            return false;
        }

        // Converts the key's data to a String.
        @Override
        public String toString() {
            return new String(data, 0, length, StandardCharsets.UTF_8);
        }

        // Appends a byte to the key and updates its hash.
        public void append(byte b) {
            data[length++] = b;
            hash = hash * 10191 + b;
        }

        // Resets the key to its initial state.
        public void clear() {
            length = hash = 0;
        }
    }

    public static void main(String[] args) throws IOException {
        // Open the file for reading.
        File file = new File(FILE);
        long length = file.length();
        long chunk = 1 << 28; // Size of the chunk to be processed.
        RandomAccessFile raf = new RandomAccessFile(file, "r");

        // Process the file in chunks and merge the results.
        Map<Key, Measurement> allMeasurementsMap = LongStream.range(0, length / chunk + 1)
                .parallel()
                .mapToObj(i -> extractMeasurementsFromChunk(i, chunk, length, raf))
                .reduce((a, b) -> mergeMeasurementMaps(a, b))
                .orElseGet(Collections::emptyMap);

        // Sort the measurements and print them.
        Map<String, Measurement> sortedMeasurementsMap = new TreeMap<>();
        allMeasurementsMap.forEach((k, m) -> sortedMeasurementsMap.put(k.toString(), m));
        System.out.println(sortedMeasurementsMap);
    }

    // Merges two measurement maps.
    private static Map<Key, Measurement> mergeMeasurementMaps(Map<Key, Measurement> a, Map<Key, Measurement> b) {
        a.forEach((k, m) -> {
            Measurement m2 = b.get(k);
            if (m2 != null)
                m.merge(m2);
        });
        b.forEach(a::putIfAbsent);
        return a;
    }

    // Extracts measurements from a chunk of the file.
    private static Map<Key, Measurement> extractMeasurementsFromChunk(long i, long chunk, long length, RandomAccessFile raf) {
        long start = i * chunk;
        long size = Math.min(length, start + chunk + 64 * 1024) - start;
        Map<Key, Measurement> map = new HashMap<>(1024);
        try {
            MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
            if (i > 0)
                skipToFirstLine(mbb);
            Key key = new Key();
            while (mbb.remaining() > 0 && mbb.position() <= chunk) {
                key.clear();
                readKey(mbb, key);
                int temp = readTemperatureFromBuffer(mbb);
                Measurement m = map.computeIfAbsent(key, n -> new Measurement());
                if (m.count == 0)
                    key = new Key();
                m.sample(temp / 10.0);
            }

        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return map;
    }

    // Reads a temperature value from the buffer.
    private static int readTemperatureFromBuffer(MappedByteBuffer mbb) {
        int temp = 0;
        boolean negative = false;
        outer: while (mbb.remaining() > 0) {
            byte b = mbb.get();
            switch (b) {
                case '-':
                    negative = true;
                    break;
                case '.':
                    break;
                case '\r':
                case '\n':
                    break outer;
                default:
                    temp = 10 * temp + (b - '0');
                    break;
            }
        }
        if (mbb.remaining() > 0) {
            byte b = mbb.get(mbb.position());
            if (b == '\n')
                mbb.get();
        }
        if (negative)
            temp = -temp;
        return temp;
    }

    // Reads a key from the buffer.
    private static void readKey(MappedByteBuffer mbb, Key key) {
        while (mbb.remaining() > 0) {
            byte b = mbb.get();
            if (b == ';' || b == '\r' || b == '\n')
                break;
            key.append(b);
        }
    }

    // Skips to the first line in the buffer, used for chunk processing.
    private static void skipToFirstLine(MappedByteBuffer mbb) {
        while ((mbb.get() & 0xFF) >= ' ') {
            // Skip bytes until reaching the start of a line.
        }
    }
}
