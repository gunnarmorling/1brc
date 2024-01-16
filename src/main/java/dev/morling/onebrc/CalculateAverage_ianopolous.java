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
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.*;
import java.util.stream.*;
import java.util.*;

/* A simple implementation aiming for readability.
 * Features:
 * * memory mapped file
 * * read chunks in parallel
 * * minimise allocation
 * * no unsafe
 *
 * Timings on 4 core i7-7500U CPU @ 2.70GHz:
 * average_baseline: 4m48s
 * ianopolous:         36s
*/
public class CalculateAverage_ianopolous {

    public static final int MAX_LINE_LENGTH = 107;
    public static final int MAX_STATIONS = 10_000;

    public static void main(String[] args) throws Exception {
        File input = new File("./measurements.txt");
        long filesize = input.length();
        // keep chunk size between 256 MB and 1G (1 chunk for files < 256MB)
        long chunkSize = Math.min(Math.max(filesize / 32, 256 * 1024 * 1024), 1024 * 1024 * 1024L);
        int nChunks = (int) ((filesize + chunkSize - 1) / chunkSize);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<List<List<Stat>>>> allResults = IntStream.range(0, nChunks)
                .mapToObj(i -> pool.submit(() -> parseStats(i * chunkSize, Math.min((i + 1) * chunkSize, filesize))))
                .toList();

        TreeMap<String, Stat> merged = allResults.stream()
                .parallel()
                .flatMap(f -> {
                    try {
                        return f.get().stream().filter(Objects::nonNull).flatMap(Collection::stream);
                    }
                    catch (Exception e) {
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toMap(s -> s.name(), s -> s, (a, b) -> a.merge(b), TreeMap::new));
        System.out.println(merged);
    }

    public static boolean matchingStationBytes(int start, int end, MappedByteBuffer buffer, Stat existing) {
        for (int i = start; i < end; i++) {
            if (existing.name[i - start] != buffer.get(i))
                return false;
        }
        return true;
    }

    public static Stat parseStation(int start, int end, int hash, MappedByteBuffer buffer, List<List<Stat>> stations) {
        int index = Math.floorMod(hash, MAX_STATIONS);
        List<Stat> matches = stations.get(index);
        if (matches == null) {
            List<Stat> value = new ArrayList<>();
            byte[] stationBuffer = new byte[end - start];
            buffer.position(start);
            buffer.get(stationBuffer);
            Stat res = new Stat(stationBuffer);
            value.add(res);
            stations.set(index, value);
            return res;
        }
        else {
            for (int i = 0; i < matches.size(); i++) {
                Stat s = matches.get(i);
                if (matchingStationBytes(start, end, buffer, s))
                    return s;
            }
            byte[] stationBuffer = new byte[end - start];
            buffer.position(start);
            buffer.get(stationBuffer);
            Stat res = new Stat(stationBuffer);
            matches.add(res);
            return res;
        }
    }

    public static List<List<Stat>> parseStats(long startByte, long endByte) {
        try {
            RandomAccessFile file = new RandomAccessFile("./measurements.txt", "r");
            long maxEnd = Math.min(file.length(), endByte + MAX_LINE_LENGTH);
            long len = maxEnd - startByte;
            if (len > Integer.MAX_VALUE)
                throw new RuntimeException("Segment size must fit into an int");
            int maxDone = (int) (endByte - startByte);
            MappedByteBuffer buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, startByte, len);
            int done = 0;
            // read first partial line
            if (startByte > 0) {
                for (int i = 0; i < MAX_LINE_LENGTH; i++) {
                    byte b = buffer.get(i);
                    if (b == '\n') {
                        done = i + 1;
                        break;
                    }
                }
            }

            List<List<Stat>> stations = new ArrayList<>(MAX_STATIONS);
            for (int i = 0; i < MAX_STATIONS; i++)
                stations.add(null);
            int lineStart = done;
            int lineSplit = 0;
            short temperature = 0;
            int hash = 1;
            boolean negative = false;
            while (done < maxDone) {
                Stat station = null;
                for (int i = done; i < done + MAX_LINE_LENGTH && i < maxEnd; i++) {
                    byte b = buffer.get(i);
                    if (b == '\n') {
                        done = i + 1;
                        temperature = negative ? (short) -temperature : temperature;
                        station.add(temperature);
                        lineStart = done;
                        station = null;
                        hash = 1;
                        break;
                    }
                    else if (b == ';') {
                        lineSplit = i;
                        station = parseStation(lineStart, lineSplit, hash, buffer, stations);
                        temperature = 0;
                        negative = false;
                    }
                    else if (station == null) {
                        hash = 31 * hash + b;
                    }
                    else if (b == '-') {
                        negative = true;
                    }
                    else if (b != '.') {
                        temperature = (short) (temperature * 10 + (b - 0x30));
                    }
                }
            }
            return stations;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Stat {
        final byte[] name;
        int count = 0;
        short min = Short.MAX_VALUE, max = Short.MIN_VALUE;
        long total = 0;

        public Stat(byte[] name) {
            this.name = name;
        }

        public void add(short value) {
            if (value < min)
                min = value;
            if (value > max)
                max = value;
            total += value;
            count++;
        }

        public Stat merge(Stat value) {
            if (value.min < min)
                min = value.min;
            if (value.max > max)
                max = value.max;
            total += value.total;
            count += value.count;
            return this;
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }

        public String name() {
            return new String(name);
        }

        public String toString() {
            return round((double) min) + "/" + round(((double) total) / count) + "/" + round((double) max);
        }
    }
}
