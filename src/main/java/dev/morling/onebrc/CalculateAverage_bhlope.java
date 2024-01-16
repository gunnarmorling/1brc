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
import java.io.UnsupportedEncodingException;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_bhlope {
    private static final int PAGE_SIZE = 4096;
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {

        try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {
            List<FileSector> sectors = getSectors(file);

            sectors
                    .stream()
                    .parallel()
                    .forEach(CalculateAverage_bhlope::readSector);

            Map<String, WeatherRecord> finalMap = new TreeMap<>();

            for (FileSector sector : sectors) {

                for (Map.Entry<String, WeatherRecord> entry : sector.listener.getRecordMap().entrySet()) {
                    WeatherRecord wr = finalMap.putIfAbsent(entry.getKey(), entry.getValue());

                    if (wr != null) {
                        wr.addRecord(entry.getValue());
                    }
                }
                sector.listener.getRecordMap().clear();
            }
            System.out.println(finalMap);
        }
    }

    private static List<FileSector> getSectors(RandomAccessFile file) throws IOException {
        long fileSize = file.getChannel().size();

        int numberOfSectors;
        long mapLength = Integer.MAX_VALUE;

        if (fileSize > Integer.MAX_VALUE) {
            numberOfSectors = (int) (fileSize / Integer.MAX_VALUE);
        }
        else {
            numberOfSectors = 1;
            mapLength = (int) (fileSize / numberOfSectors);
        }

        if (mapLength * numberOfSectors < fileSize) {
            numberOfSectors += 1;
        }
        List<FileSector> sectors = new ArrayList<>(numberOfSectors);

        long start = 0;

        for (int i = 0; i < numberOfSectors; i++) {
            long remaining = fileSize - start;

            mapLength = Math.min(mapLength, remaining);

            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, mapLength);

            for (; mapLength > 0; mapLength--) {
                if (buffer.get((int) mapLength - 1) == '\n') {
                    break;
                }
            }
            mapLength--;
            sectors.add(new FileSector(start, mapLength, file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, mapLength)));

            start += mapLength + 1;
            mapLength = Integer.MAX_VALUE;
        }
        return sectors;
    }

    private static int readSector(FileSector sector) {
        int count = 0;
        long length = PAGE_SIZE;

        while (sector.buffer.hasRemaining()) {

            length = Math.min(length, sector.length - sector.buffer.position());

            String entry = readLine(sector.buffer);

            WeatherRecord weatherRecord = WeatherRecord.fromEntry(entry);

            if (weatherRecord != null) {
                count++;
                sector.listener.onNewRecord(weatherRecord);
            }
        }
        sector.buffer = null;

        return count;
    }

    private static String readLine(MappedByteBuffer buffer) {

        int size = 0;
        int maxLength = buffer.limit() - 1;

        while (buffer.get(buffer.position() + size) != '\n' && (buffer.position() + size) < maxLength) {
            size++;
        }
        size += 1;

        byte[] data = new byte[size];

        buffer.get(data, 0, size);

        return new String(data, StandardCharsets.UTF_8);
    }

    private static class FileSector {
        private long start;
        private long length;
        private MappedByteBuffer buffer;
        private RecordListener listener;

        public FileSector(long start, long length, MappedByteBuffer buffer) {
            this.start = start;
            this.length = length;
            this.buffer = buffer;
            listener = new WeatherRecordListener();
        }
    }

    private static class WeatherRecord {
        private long count;
        private double sum;
        private final String city;
        private double max = Double.NEGATIVE_INFINITY;
        private double min = Double.POSITIVE_INFINITY;

        public WeatherRecord(String city, double temperature) {
            this.city = city;
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
            sum += temperature;
            count = 1;
        }

        public void addRecord(WeatherRecord weatherRecord) {
            min = Math.min(min, weatherRecord.min);
            max = Math.max(max, weatherRecord.max);
            sum += weatherRecord.sum;
            count++;
        }

        public String getCity() {
            return city;
        }

        @Override
        public String toString() {
            double mean = sum / count;

            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        public static WeatherRecord fromEntry(String entry) {
            entry = entry.trim();

            String[] values = entry.split(";");

            if (values.length == 2) {
                try {
                    return new WeatherRecord(values[0], Double.parseDouble(values[1]));
                }
                catch (NumberFormatException ne) {
                    return null;
                }
            }
            return null;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static interface RecordListener {
        void onNewRecord(WeatherRecord weatherRecord);

        Map<String, WeatherRecord> getRecordMap();
    }

    private static class WeatherRecordListener implements RecordListener {
        private final Map<String, WeatherRecord> recordMap = new HashMap<>();

        @Override
        public void onNewRecord(WeatherRecord weatherRecord) {
            WeatherRecord wr = recordMap.putIfAbsent(weatherRecord.getCity(), weatherRecord);

            if (wr != null) {
                wr.addRecord(weatherRecord);
            }
        }

        @Override
        public Map<String, WeatherRecord> getRecordMap() {
            return recordMap;
        }
    }
}
