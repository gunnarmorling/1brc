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
import java.util.stream.*;
import java.util.*;

/* A simple implementation that memory maps the file, reads chunks in parallel and minimises allocation without any unsafe.
 *
 * Timings on 4 core i7-7500U CPU @ 2.70GHz:
 * average_baseline: 4m48s
 * ianopolous:         48s
*/
public class CalculateAverage_ianopolous {

    public static final int MAX_LINE_LENGTH = 107;
    public static final int MAX_STATIONS = 10000;

    public static void main(String[] args) {
        File input = new File("./measurements.txt");
        long filesize = input.length();
        long chunkSize = 256 * 1024 * 1024;
        int nChunks = (int) ((filesize + chunkSize - 1) / chunkSize);
        List<HashMap<String, Stat>> allResults = IntStream.range(0, nChunks).mapToObj(i -> {
            HashMap<String, Stat> results = new HashMap(512);
            parseStats(i * chunkSize, Math.min((i + 1) * chunkSize, filesize), results);
            return results;
        }).parallel().toList();
        HashMap<String, Stat> result = allResults.getFirst();
        for (int i = 1; i < allResults.size(); ++i) {
            for (Map.Entry<String, Stat> entry : allResults.get(i).entrySet()) {
                Stat current = result.putIfAbsent(entry.getKey(), entry.getValue());
                if (current != null) {
                    current.merge(entry.getValue());
                }
            }
        }

        System.out.println(new TreeMap<>(result));
    }

    public record Station(String name, ByteBuffer buf) {
    }

    public static boolean matchingStationBytes(int start, int end, MappedByteBuffer buffer, Station existing) {
        buffer.position(start);
        for (int i = start; i < end; i++) {
            if (existing.buf.get(i - start) != buffer.get(i))
                return false;
        }
        return true;
    }

    public static Station parseStation(int start, int end, int hash, MappedByteBuffer buffer, List<List<Station>> stations) {
        int index = Math.floorMod(hash, MAX_STATIONS);
        List<Station> matches = stations.get(index);
        if (matches == null) {
            List<Station> value = new ArrayList<>();
            byte[] stationBuffer = new byte[end - start];
            buffer.position(start);
            buffer.get(stationBuffer);
            String name = new String(stationBuffer);
            Station res = new Station(name, ByteBuffer.wrap(stationBuffer));
            value.add(res);
            stations.set(index, value);
            return res;
        }
        else {
            for (int i = 0; i < matches.size(); i++) {
                Station s = matches.get(i);
                if (matchingStationBytes(start, end, buffer, s))
                    return s;
            }
            byte[] stationBuffer = new byte[end - start];
            buffer.position(start);
            buffer.get(stationBuffer);
            Station res = new Station(new String(stationBuffer), ByteBuffer.wrap(stationBuffer));
            matches.add(res);
            return res;
        }
    }

    public static void parseStats(long startByte, long endByte, Map<String, Stat> results) {
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

            List<List<Station>> stations = new ArrayList<>(MAX_STATIONS);
            for (int i = 0; i < MAX_STATIONS; i++)
                stations.add(null);
            int lineStart = done;
            int lineSplit = 0;
            long temperature = 0;
            int hash = 1;
            boolean negative = false;
            while (done < maxDone) {
                Station station = null;
                for (int i = done; i < done + MAX_LINE_LENGTH && i < maxEnd; i++) {
                    byte b = buffer.get(i);
                    if (b == '\n') {
                        done = i + 1;
                        Stat res = results.get(station.name);
                        temperature = negative ? -temperature : temperature;
                        if (res != null) {
                            res.add(temperature);
                        }
                        else {
                            res = new Stat();
                            res.add(temperature);
                            results.put(station.name, res);
                        }
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
                    else if (b == '-' && station != null) {
                        negative = true;
                    }
                    else if (b != '.' && station != null) {
                        temperature = temperature * 10 + (b - 0x30);
                    }
                    else {
                        hash = 31 * hash + b;
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Stat {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE, total = 0, count = 0;

        public void add(long value) {
            if (value < min)
                min = value;
            if (value > max)
                max = value;
            total += value;
            count++;
        }

        public void merge(Stat value) {
            if (value.min < min)
                min = value.min;
            if (value.max > max)
                max = value.max;
            total += value.total;
            count += value.count;
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }

        public String toString() {
            return round((double) min) + "/" + round(((double) total) / count) + "/" + round((double) max);
        }
    }
}
