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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingByConcurrent;

public class CalculateAverage_PawelAdamski {

    private static final long READ_SIZE = 100_000_000;
    private static final String FILE = "./measurements.txt";

    private static record ResultRow(double min, double mean, double max) {

        public ResultRow(MeasurementAggregator ma) {
            this(ma.min / 10.0, ((Math.round(ma.sum * 100.0) / 100.0) / (double) ma.count) / 10.0, ma.max / 10.0);
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class Station {
        byte[] bytes;
        int hash;

        public Station(byte[] station) {
            this.bytes = station;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return Arrays.equals(bytes, ((Station) o).bytes);
        }

    }

    private static class MeasurementAggregator {
        private long min;
        private long max;
        private long sum;
        private long count;

        public MeasurementAggregator(long temp) {
            min = temp;
            max = temp;
            sum = temp;
            count = 1;
        }

        public MeasurementAggregator() {
            min = Long.MAX_VALUE;
            max = Long.MIN_VALUE;
            sum = 0;
            count = 0;
        }

        public MeasurementAggregator merge(MeasurementAggregator measurement) {
            MeasurementAggregator ma = new MeasurementAggregator();
            ma.min = Math.min(min, measurement.min);
            ma.max = Math.max(max, measurement.max);
            ma.sum = sum + measurement.sum;
            ma.count = count + measurement.count;
            return ma;
        }
    }

    public static void main(String[] args) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            List<FilePart> parts = splitFileIntoParts(raf);
            Map<Station, MeasurementAggregator> rr = calculateTemperatureStats(parts, raf);
            Map<String, ResultRow> results = prepareResults(rr);
            System.out.println(results);
        }
    }

    private static Map<String, ResultRow> prepareResults(Map<Station, MeasurementAggregator> rr) {
        Map<String, ResultRow> measurements = new TreeMap<>();
        rr.forEach((k, v) -> measurements.put(new String(k.bytes, UTF_8), new ResultRow(v)));
        return measurements;
    }

    private static Map<Station, MeasurementAggregator> calculateTemperatureStats(List<FilePart> parts, RandomAccessFile raf) {
        return parts.parallelStream()
                .map(filePart -> parse(filePart, raf))
                .flatMap(m -> m.entrySet().stream())
                .collect(groupingByConcurrent(
                        Map.Entry::getKey,
                        Collectors.reducing(
                                new MeasurementAggregator(),
                                Map.Entry::getValue,
                                MeasurementAggregator::merge)));
    }

    private static ArrayList<FilePart> splitFileIntoParts(RandomAccessFile raf) throws IOException {
        ArrayList<FilePart> parts = new ArrayList<>((int) (raf.length() / READ_SIZE));
        long pointer = 0;
        long nextPointer = 0;
        long fileLength = raf.length();
        while (pointer < fileLength) {
            if (pointer + READ_SIZE > fileLength) {
                nextPointer = fileLength;
            }
            else {
                nextPointer = findNextLine(raf, pointer + READ_SIZE);
            }
            parts.add(new FilePart(pointer, nextPointer - pointer));
            pointer = nextPointer;
        }
        return parts;
    }

    private static Map<Station, MeasurementAggregator> parse(FilePart filePart, RandomAccessFile raf) {
        try {
            byte[] bytes = readBytesFromFile(filePart, raf);
            return parseBytesIntoStationsMap(bytes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static HashMap<Station, MeasurementAggregator> parseBytesIntoStationsMap(byte[] bytes) {
        HashMap<Station, MeasurementAggregator> measurementAggregator = new HashMap<>(500);
        int semicolonIndex = 0;
        int newLineIndex = -1;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == ';') {
                semicolonIndex = i;
            }
            else if (bytes[i] == '\n') {
                byte[] station = Arrays.copyOfRange(bytes, newLineIndex + 1, semicolonIndex);
                long temp = parseDouble(bytes, semicolonIndex + 1, i);
                MeasurementAggregator measurement = new MeasurementAggregator(temp);
                measurementAggregator.compute(new Station(station), (k, prevV) -> prevV == null ? measurement : prevV.merge(measurement));
                newLineIndex = i;
            }
        }
        return measurementAggregator;
    }

    private static byte[] readBytesFromFile(FilePart filePart, RandomAccessFile raf) throws IOException {
        var bb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, filePart.start(), filePart.len());
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
    }

    private static long parseDouble(byte[] text, int start, int end) {
        boolean negative = false;
        int result = 0;
        for (int i = start; i < end; i++) {
            byte c = text[i];
            if (c == '-') {
                negative = true;
            }
            else if (c != '.') {
                result *= 10;
                result += c - '0';
            }
        }
        if (negative) {
            return -result;
        }
        else {
            return result;
        }
    }

    private static long findNextLine(RandomAccessFile raf, long currentPosition) throws IOException {
        raf.seek(currentPosition);
        while (raf.readByte() != '\n')
            ;
        return raf.getFilePointer();
    }

    record FilePart(long start, long len) {
    }
}
