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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CalculateAverage_plbpietrz {

    private static final String FILE = "./measurements.txt";
    private static final int READ_SIZE = 1024;
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static class TemperatureStats {
        double min = 999, max = -999d;
        double accumulated;
        int count;

        public void update(double temp) {
            this.min = Math.min(this.min, temp);
            this.max = Math.max(this.max, temp);
            this.accumulated += temp;
            this.count++;
        }
    }

    private record FilePart(long pos, long size) {
    }

    private static class WeatherStation {
        private int length;
        private int nameHash;
        private byte[] nameBytes;
        private String string;

        public WeatherStation() {
            nameBytes = new byte[128];
        }

        public WeatherStation(WeatherStation station) {
            this.nameBytes = Arrays.copyOf(station.nameBytes, station.length);
            this.length = station.length;
            this.nameHash = station.nameHash;
        }

        @Override
        public int hashCode() {
            return nameHash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o instanceof WeatherStation s) {
                return this.nameHash == s.nameHash && Arrays.equals(this.nameBytes, 0, this.length, s.nameBytes, 0, s.length);
            }
            return false;
        }

        @Override
        public String toString() {
            if (string == null)
                string = new String(nameBytes, 0, length, Charset.defaultCharset());
            return string;
        }

        public void appendByte(byte b) {
            string = null;
            nameBytes[length++] = b;
            nameHash = nameHash * 31 + b;
        }

        public void clear() {
            this.length = 0;
            this.nameHash = 0;
            this.string = null;
        }

    }

    public static void main(String[] args) throws IOException {
        Path inputFilePath = Path.of(FILE);
        Map<WeatherStation, TemperatureStats> results;
        try (RandomAccessFile inputFile = new RandomAccessFile(inputFilePath.toFile(), "r")) {
            var parsedBuffers = partitionInput(inputFile)
                    .stream()
                    .parallel()
                    .map(fp -> getMappedByteBuffer(fp, inputFile))
                    .map(CalculateAverage_plbpietrz::parseBuffer);
            results = parsedBuffers.flatMap(m -> m.entrySet().stream())
                    .collect(
                            Collectors.groupingBy(
                                    Map.Entry::getKey,
                                    Collectors.reducing(
                                            new TemperatureStats(),
                                            Map.Entry::getValue,
                                            CalculateAverage_plbpietrz::mergeTemperatureStats)));
            try (PrintWriter pw = new PrintWriter(new BufferedOutputStream(System.out))) {
                formatResults(pw, results);
            }
        }
    }

    private static List<FilePart> partitionInput(RandomAccessFile inputFile) throws IOException {
        List<FilePart> fileParts = new ArrayList<>();
        long fileLength = inputFile.length();

        long blockSize = Math.min(fileLength, Math.max(READ_SIZE, fileLength / CPU_COUNT));

        for (long start = 0, end; start < fileLength; start = end) {
            end = findMinBlockOffset(inputFile, start, blockSize);
            fileParts.add(new FilePart(start, end - start));
        }
        return fileParts;
    }

    private static long findMinBlockOffset(RandomAccessFile file, long startPosition, long minBlockSize) throws IOException {
        long length = file.length();
        if (startPosition + minBlockSize < length) {
            file.seek(startPosition + minBlockSize);
            while (file.readByte() != '\n') {
            }
            return file.getFilePointer();
        }
        else {
            return length;
        }
    }

    private static MappedByteBuffer getMappedByteBuffer(FilePart fp, RandomAccessFile inputFile) {
        try {
            return inputFile.getChannel().map(FileChannel.MapMode.READ_ONLY, fp.pos, fp.size);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<WeatherStation, TemperatureStats> parseBuffer(MappedByteBuffer buffer) {
        byte[] readLong = new byte[READ_SIZE];
        byte[] temperature = new byte[32];
        int temperatureLineLenght = 0;

        int limit = buffer.limit();
        boolean readingName = true;
        Map<WeatherStation, TemperatureStats> temperatures = new HashMap<>();
        WeatherStation station = new WeatherStation();

        int bytesToRead = Math.min(READ_SIZE, limit - buffer.position());
        while (bytesToRead > 0) {
            if (bytesToRead == READ_SIZE) {
                buffer.get(readLong);
            }
            else {
                for (int j = 0; j < bytesToRead; ++j)
                    readLong[j] = buffer.get();
            }

            for (int i = 0; i < bytesToRead; ++i) {
                byte aChar = readLong[i];
                if (readingName) {
                    if (aChar != ';') {
                        if (aChar != '\n') {
                            station.appendByte(aChar);
                        }
                    }
                    else {
                        readingName = false;
                    }
                }
                else {
                    if (aChar != '\n') {
                        temperature[temperatureLineLenght++] = aChar;
                    }
                    else {
                        double temp = parseTemperature(temperature, temperatureLineLenght);

                        if (!temperatures.containsKey(station)) {
                            temperatures.put(new WeatherStation(station), new TemperatureStats());
                        }
                        TemperatureStats weatherStats = temperatures.get(station);
                        weatherStats.update(temp);

                        station.clear();
                        temperatureLineLenght = 0;
                        readingName = true;
                    }
                }
            }

            bytesToRead = Math.min(READ_SIZE, limit - buffer.position());
        }
        return temperatures;
    }

    private static double parseTemperature(byte[] temperature, int temperatureSize) {
        double sign = 1;
        double manitssa = 0;
        double exponent = 1;
        for (int i = 0; i < temperatureSize; ++i) {
            byte c = temperature[i];
            switch (c) {
                case '-':
                    sign = -1;
                    break;
                case '.':
                    for (int j = i; j < temperatureSize - 1; ++j)
                        exponent *= 0.1;
                    break;
                default:
                    manitssa = manitssa * 10 + (c - 48);
            }
        }
        return sign * manitssa * exponent;
    }

    private static TemperatureStats mergeTemperatureStats(TemperatureStats v1, TemperatureStats v2) {
        TemperatureStats acc = new TemperatureStats();
        acc.min = Math.min(v1.min, v2.min);
        acc.max = Math.max(v1.max, v2.max);
        acc.accumulated = v1.accumulated + v2.accumulated;
        acc.count = v1.count + v2.count;
        return acc;
    }

    private static void formatResults(PrintWriter pw, Map<WeatherStation, TemperatureStats> resultsMap) {
        pw.print('{');
        var results = new ArrayList<>(resultsMap.entrySet());
        results.sort(Comparator.comparing(e -> e.getKey().toString()));
        var iterator = results.iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            TemperatureStats stats = entry.getValue();
            pw.printf("%s=%.1f/%.1f/%.1f",
                    entry.getKey(),
                    stats.min,
                    stats.accumulated / stats.count,
                    stats.max);
            if ((iterator.hasNext()))
                pw.print(", ");
        }
        pw.println('}');
    }

}
