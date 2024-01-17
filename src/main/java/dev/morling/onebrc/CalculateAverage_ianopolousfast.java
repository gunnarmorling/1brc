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

/* A fast implementation with no unsafe.
 * Features:
 * * memory mapped file
 * * read chunks in parallel
 * * minimise allocation
 * * no unsafe
 *
 * Timings on 4 core i7-7500U CPU @ 2.70GHz:
 * average_baseline: 4m48s
 * ianopolous:         19s
*/
public class CalculateAverage_ianopolousfast {

    public static final int MAX_LINE_LENGTH = 107;
    public static final int MAX_STATIONS = 10_000;

    public static void main(String[] args) throws Exception {
        File input = new File("./measurements.txt");
        long filesize = input.length();
        // keep chunk size between 256 MB and 1G (1 chunk for files < 256MB)
        long chunkSize = Math.min(Math.max((filesize + 31) / 32, 256 * 1024 * 1024), 1024 * 1024 * 1024L);
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
                        e.printStackTrace();
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toMap(s -> s.name(), s -> s, (a, b) -> a.merge(b), TreeMap::new));
        System.out.println(merged);
    }

    public static boolean matchingStationBytes(int start, int end, ByteBuffer buffer, Stat existing) {
        if (end - start != existing.name.length)
            return false;
        for (int i = start; i < end; i++) {
            if (existing.name[i - start] != buffer.get(i))
                return false;
        }
        return true;
    }

    public static Stat dedupeStation(int start, int end, long hash, ByteBuffer buffer, List<List<Stat>> stations) {
        int index = Math.floorMod(hash ^ (hash >> 32), MAX_STATIONS);
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

    public static int getSemicolon(long d) {
        // from Hacker's Delight page 92
        d = d ^ 0x3b3b3b3b3b3b3b3bL;
        long y = (d & 0x7f7f7f7f7f7f7f7fL) + 0x7f7f7f7f7f7f7f7fL;
        y = ~(y | d | 0x7f7f7f7f7f7f7f7fL);
        return Long.numberOfLeadingZeros(y) >> 3;
    }

    public static long updateHash(long hash, long x) {
        return ((hash << 5) ^ x) * 0x517cc1b727220a95L; // fxHash
    }

    public static Stat parseStation(int lineStart, ByteBuffer buffer, List<List<Stat>> stations) {
        // find semicolon and update hash as we go, reading a long at a time
        long d = buffer.getLong(lineStart);

        int semiIndex = getSemicolon(d);
        int index = 0;
        long hash = 0;
        while (semiIndex == 8) {
            hash = updateHash(hash, d);
            index += 8;
            d = buffer.getLong(lineStart + index);
            semiIndex = getSemicolon(d);
        }
        // mask extra bytes off last long
        d = d & (-1L << ((8 - semiIndex) * 8));
        if (semiIndex > 0) {
            hash = updateHash(hash, d);
        }
        return dedupeStation(lineStart, lineStart + index + semiIndex, hash, buffer, stations);
    }

    public static int processTemperature(int lineSplit, MappedByteBuffer buffer, Stat station) {
        short temperature;
        boolean negative = false;
        byte b = buffer.get(lineSplit++);
        if (b == '-') {
            negative = true;
            b = buffer.get(lineSplit++);
        }
        temperature = (short) (b - 0x30);
        b = buffer.get(lineSplit++);
        if (b == '.') {
            b = buffer.get(lineSplit++);
            temperature = (short) (temperature * 10 + (b - 0x30));
        }
        else {
            temperature = (short) (temperature * 10 + (b - 0x30));
            lineSplit++;
            b = buffer.get(lineSplit++);
            temperature = (short) (temperature * 10 + (b - 0x30));
        }
        temperature = negative ? (short) -temperature : temperature;
        station.add(temperature);
        return lineSplit + 1;
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

            // Handle reading the very last line in the file
            // this allows us to not worry about reading a long beyond the end
            // in the inner loop (reducing branches)
            // We only need to read one because the min record size is 6 bytes
            // so 2nd last record must be > 8 from end
            if (endByte == file.length()) {
                int offset = (int) (file.length() - startByte - 1);
                while (buffer.get(offset) != '\n') // final new line
                    offset--;
                offset--;
                while (offset > 0 && buffer.get(offset) != '\n') // end of second last line
                    offset--;
                maxDone = offset;
                if (offset > 0)
                    offset++;
                // copy into a 8n sized buffer to avoid reading off end
                int roundedSize = (int) (file.length() - startByte) - offset;
                roundedSize = (roundedSize + 7) / 8 * 8;
                byte[] end = new byte[roundedSize];
                for (int i = offset; i < (int) (file.length() - startByte); i++)
                    end[i - offset] = buffer.get(i);
                Stat station = parseStation(0, ByteBuffer.wrap(end), stations);
                processTemperature(offset + station.name.length + 1, buffer, station);
            }

            int lineStart = done;
            while (lineStart < maxDone) {
                Stat station = parseStation(lineStart, buffer, stations);
                lineStart = processTemperature(lineStart + station.name.length + 1, buffer, station);
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
