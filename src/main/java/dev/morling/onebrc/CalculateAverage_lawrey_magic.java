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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;

public class CalculateAverage_lawrey_magic {

    // Path to the file containing temperature measurements.
    private static final String FILE = "./measurements.txt";
    private static final String[] stations = new String[6122];

    public static void main(String[] args) throws IOException {
        // Open the file for reading.
        File file = new File(FILE);
        long length = file.length();
        long chunk = 1 << 28; // Size of the chunk to be processed.
        RandomAccessFile raf = new RandomAccessFile(file, "r");

        // Process the file in chunks and merge the results.
        Measurement[] allMeasurementsMap = LongStream.range(0, length / chunk + 1)
                .parallel()
                .mapToObj(i -> extractMeasurementsFromChunk(i, chunk, length, raf))
                .reduce((a, b) -> mergeMeasurementMaps(a, b))
                .orElseThrow();

        // Sort the measurements and print them.
        Map<String, Measurement> sortedMeasurementsMap = new TreeMap<>();
        for (int i = 0; i < stations.length; i++) {
            String station = stations[i];
            if (station != null) // most are null
                sortedMeasurementsMap.put(station, allMeasurementsMap[i]);
        }
        System.out.print('{');
        String sep = "";
        for (Map.Entry<String, Measurement> entry : sortedMeasurementsMap.entrySet()) {
            System.out.print(sep);
            System.out.print(entry);
            sep = ", ";
        }
        System.out.println("}");
    }

    // Merges two measurement maps.
    private static Measurement[] mergeMeasurementMaps(Measurement[] a, Measurement[] b) {
        for (int i = 0; i < a.length; i++) {
            if (a[i] != null)
                a[i].merge(b[i]);
            else
                a[i] = b[i];
        }
        return a;
    }

    // Extracts measurements from a chunk of the file.
    private static Measurement[] extractMeasurementsFromChunk(long i, long chunk, long length, RandomAccessFile raf) {
        long start = i * chunk;
        long size = Math.min(length, start + chunk + 64 * 1024) - start;
        Measurement[] array = new Measurement[6122];
        try {
            MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
            mbb.order(ByteOrder.nativeOrder());
            if (i > 0)
                skipToFirstLine(mbb);
            while (mbb.remaining() > 0 && mbb.position() <= chunk) {
                int pos = mbb.position();
                int key = readKey(mbb);
                Measurement m = array[key];
                if (m == null) {
                    m = array[key] = new Measurement();
                    if (stations[key] == null) {
                        // need to read it again the first time
                        int len = mbb.position() - pos - 1;
                        byte[] bytes = new byte[len];
                        mbb.position(pos);
                        mbb.get(bytes, 0, len);
                        mbb.get(); // ';'
                        stations[key] = new String(bytes, StandardCharsets.UTF_8);
                    }
                }
                int temp = readTemperatureFromBuffer(mbb);
                m.sample(temp / 10.0);
            }

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return array;
    }

    // Reads a temperature value from the buffer.
    private static int readTemperatureFromBuffer(MappedByteBuffer mbb) {
        int temp = 0;
        boolean negative = false;
        outer:
        while (mbb.remaining() > 0) {
            int b = mbb.get();
            switch (b) {
                case '-':
                    negative = true;
                    break;
                default:
                    temp = 10 * temp + (b - '0');
                    break;
                case '.':
                    b = mbb.get();
                    temp = 10 * temp + (b - '0');
                case '\r':
                    mbb.get();
                case '\n':
                    break outer;
            }
        }
        if (negative)
            temp = -temp;
        return temp;
    }

    // Skips to the first line in the buffer, used for chunk processing.
    private static void skipToFirstLine(MappedByteBuffer mbb) {
        while ((mbb.get() & 0xFF) >= ' ') {
            // Skip bytes until reaching the start of a line.
        }
    }

    static int readKey(MappedByteBuffer mbb) {
        long hash = mbb.getInt();
        int rewind = -1;

        if ((hash & 0xFF000000) != (';' << 24)) {
            do {
                int s = mbb.getInt();
                if ((s & 0xFF) == ';') {
                    rewind = 3;
                    s = ';';
                } else if ((s & 0xFF00) == (';' << 8)) {
                    rewind = 2;
                    s &= 0xFFFF;
                } else if ((s & 0xFF0000) == (';' << 16)) {
                    rewind = 1;
                    s &= 0xFFFFFF;
                } else if ((s & 0xFF000000) == (';' << 24)) {
                    rewind = 0;
                }
                hash = hash * 21503 + s;
            } while (rewind == -1);
        }
        hash += hash >>> 1;
        mbb.position(mbb.position() - rewind);

        int abs = (int) Math.abs(hash % 6121);
        return abs;
    }

    static String peek(MappedByteBuffer mbb) {
        int pos = mbb.position();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 16; i++)
            sb.append((char) (mbb.get(pos + i) & 0xff));
        return sb.toString();
    }

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
}
