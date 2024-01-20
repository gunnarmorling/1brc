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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.*;
import java.util.*;

import static java.lang.foreign.ValueLayout.*;

/* A fast implementation with no unsafe.
 * Features:
 * * memory mapped file using preview Arena FFI
 * * semicolon finding using incubator vector api
 * * read chunks in parallel
 * * minimise allocation
 * * no unsafe
 *
 * Timings on 4 core i7-7500U CPU @ 2.70GHz:
 * average_baseline: 4m48s
 * ianopolous:         15s
*/
public class CalculateAverage_ianopolousfast {

    public static final int MAX_LINE_LENGTH = 107;
    public static final int MAX_STATIONS = 1 << 14;
    private static final OfLong LONG_LAYOUT = JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 32
            ? ByteVector.SPECIES_256
            : ByteVector.SPECIES_128;

    public static void main(String[] args) throws Exception {
        Arena arena = Arena.global();
        Path input = Path.of("measurements.txt");
        FileChannel channel = (FileChannel) Files.newByteChannel(input, StandardOpenOption.READ);
        long filesize = Files.size(input);
        MemorySegment mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, filesize, arena);
        int nChunks = filesize < 4 * 1024 * 1024 ? 1 : Runtime.getRuntime().availableProcessors();
        long chunkSize = (filesize + nChunks - 1) / nChunks;
        List<List<List<Stat>>> allResults = IntStream.range(0, nChunks)
                .parallel()
                .mapToObj(i -> parseStats(i * chunkSize, Math.min((i + 1) * chunkSize, filesize), mmap))
                .toList();

        TreeMap<String, Stat> merged = allResults.stream()
                .parallel()
                .flatMap(f -> {
                    try {
                        return f.stream().filter(Objects::nonNull).flatMap(Collection::stream);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toMap(s -> s.name(), s -> s, (a, b) -> a.merge(b), TreeMap::new));
        System.out.println(merged);
    }

    public static boolean matchingStationBytes(long start, long end, int offset, MemorySegment buffer, Stat existing) {
        int len = (int) (end - start);
        if (len != existing.name.length)
            return false;
        for (int i = offset; i < len; i++) {
            if (existing.name[i] != buffer.get(JAVA_BYTE, offset + start++))
                return false;
        }
        return true;
    }

    private static int hashToIndex(long hash, int len) {
        // From Thomas Wuerthinger's entry
        int hashAsInt = (int) (hash ^ (hash >>> 28));
        int finalHash = (hashAsInt ^ (hashAsInt >>> 15));
        return (finalHash & (len - 1));
    }

    public static Stat parseStation(long start, long end, long first8, long second8,
                                    MemorySegment buffer) {
        byte[] stationBuffer = new byte[(int) (end - start)];
        for (long off = start; off < end; off++)
            stationBuffer[(int) (off - start)] = buffer.get(JAVA_BYTE, off);
        return new Stat(stationBuffer, first8, second8);
    }

    public static Stat dedupeStation(long start, long end, long hash, long first8, long second8,
                                     MemorySegment buffer, List<List<Stat>> stations) {
        int index = hashToIndex(hash, MAX_STATIONS);
        List<Stat> matches = stations.get(index);
        if (matches == null) {
            List<Stat> value = new ArrayList<>();
            Stat res = parseStation(start, end, first8, second8, buffer);
            value.add(res);
            stations.set(index, value);
            return res;
        }
        else {
            for (int i = 0; i < matches.size(); i++) {
                Stat s = matches.get(i);
                if (first8 == s.first8 && second8 == s.second8 && matchingStationBytes(start, end, 16, buffer, s))
                    return s;
            }
            Stat res = parseStation(start, end, first8, second8, buffer);
            matches.add(res);
            return res;
        }
    }

    public static Stat dedupeStation8(long start, long end, long hash, long first8, MemorySegment buffer, List<List<Stat>> stations) {
        int index = hashToIndex(hash, MAX_STATIONS);
        List<Stat> matches = stations.get(index);
        if (matches == null) {
            List<Stat> value = new ArrayList<>();
            Stat station = parseStation(start, end, first8, 0, buffer);
            value.add(station);
            stations.set(index, value);
            return station;
        }
        else {
            for (int i = 0; i < matches.size(); i++) {
                Stat s = matches.get(i);
                if (first8 == s.first8 && s.name.length <= 8)
                    return s;
            }
            Stat station = parseStation(start, end, first8, 0, buffer);
            matches.add(station);
            return station;
        }
    }

    public static Stat dedupeStation16(long start, long end, long hash, long first8, long second8, MemorySegment buffer, List<List<Stat>> stations) {
        int index = hashToIndex(hash, MAX_STATIONS);
        List<Stat> matches = stations.get(index);
        if (matches == null) {
            List<Stat> value = new ArrayList<>();
            Stat res = parseStation(start, end, first8, second8, buffer);
            value.add(res);
            stations.set(index, value);
            return res;
        }
        else {
            for (int i = 0; i < matches.size(); i++) {
                Stat s = matches.get(i);
                if (first8 == s.first8 && second8 == s.second8 && s.name.length <= 16)
                    return s;
            }
            Stat res = parseStation(start, end, first8, second8, buffer);
            matches.add(res);
            return res;
        }
    }

    static long maskHighBytes(long d, int nbytes) {
        return d & (-1L << ((8 - nbytes) * 8));
    }

    public static Stat parseStation(long lineStart, MemorySegment buffer, List<List<Stat>> stations) {
        ByteVector line = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart, ByteOrder.nativeOrder());
        int keySize = line.compare(VectorOperators.EQ, ';').firstTrue();

        if (keySize == BYTE_SPECIES.vectorByteSize()) {
            while (buffer.get(JAVA_BYTE, lineStart + keySize) != ';') {
                keySize++;
            }
            long first8 = buffer.get(LONG_LAYOUT, lineStart);
            if (keySize < 8)
                return dedupeStation8(lineStart, lineStart + keySize, first8, first8, buffer, stations);
            long second8 = buffer.get(LONG_LAYOUT, lineStart + 8);
            if (keySize < 16)
                return dedupeStation16(lineStart, lineStart + keySize, first8 ^ second8, first8, second8, buffer, stations);
            long hash = first8 ^ second8; // todo include other bytes
            return dedupeStation(lineStart, lineStart + keySize, hash, first8, second8, buffer, stations);
        }

        long first8 = buffer.get(LONG_LAYOUT, lineStart);
        if (keySize <= 8) {
            first8 = maskHighBytes(first8, keySize & 0x07);
            return dedupeStation8(lineStart, lineStart + keySize, first8, first8, buffer, stations);
        }
        long second8 = buffer.get(LONG_LAYOUT, lineStart + 8);
        if (keySize < 16) {
            second8 = maskHighBytes(second8, keySize & 0x07);
            return dedupeStation16(lineStart, lineStart + keySize, first8 ^ second8, first8, second8, buffer, stations);
        }
        long hash = first8 ^ second8; // todo include later bytes
        return dedupeStation(lineStart, lineStart + keySize, hash, first8, second8, buffer, stations);
    }

    public static int getDot(long d) {
        // from Hacker's Delight page 92
        d = d ^ 0x2e2e2e2e2e2e2e2eL;
        long y = (d & 0x7f7f7f7f7f7f7f7fL) + 0x7f7f7f7f7f7f7f7fL;
        y = ~(y | d | 0x7f7f7f7f7f7f7f7fL);
        return Long.numberOfLeadingZeros(y) >> 3;
    }

    public static short getMinus(long d) {
        d = d & 0xff00000000000000L;
        d = d ^ 0x2d2d2d2d2d2d2d2dL;
        long y = (d & 0x7f7f7f7f7f7f7f7fL) + 0x7f7f7f7f7f7f7f7fL;
        y = ~(y | d | 0x7f7f7f7f7f7f7f7fL);
        return (short) ((Long.numberOfLeadingZeros(y) >> 6) - 1);
    }

    public static long processTemperature(long lineSplit, MemorySegment buffer, Stat station) {
        long d = buffer.get(LONG_LAYOUT, lineSplit);
        // negative is either 0 or -1
        short negative = getMinus(d);
        d = d << (negative * -8);
        int dotIndex = getDot(d);
        d = (d >> 8) | 0x30000000_00000000L; // add a leading 0 digit
        d = d >> 8 * (5 - dotIndex);
        short temperature = (short) ((byte) d - '0' +
                10 * (((byte) (d >> 16)) - '0') +
                100 * (((byte) (d >> 24)) - '0'));
        temperature = (short) ((temperature ^ negative) - negative); // negative treatment inspired by merkitty
        station.add(temperature);
        return lineSplit - negative + dotIndex + 3;
    }

    public static List<List<Stat>> parseStats(long startByte, long endByte, MemorySegment buffer) {
        // read first partial line
        if (startByte > 0) {
            for (int i = 0; i < MAX_LINE_LENGTH; i++) {
                byte b = buffer.get(JAVA_BYTE, startByte++);
                if (b == '\n') {
                    break;
                }
            }
        }

        List<List<Stat>> stations = new ArrayList<>(MAX_STATIONS);
        for (int i = 0; i < MAX_STATIONS; i++)
            stations.add(null);

        // Handle reading the very last few lines in the file
        // this allows us to not worry about reading beyond the end
        // in the inner loop (reducing branches)
        // We need at least the vector lane size bytes back
        if (endByte == buffer.byteSize()) {
            endByte -= 1; // skip final new line
            // reverse at least vector lane width
            while (endByte > 0 && buffer.byteSize() - endByte < BYTE_SPECIES.vectorByteSize()) {
                endByte--;
                while (endByte > 0 && buffer.get(JAVA_BYTE, endByte) != '\n')
                    endByte--;
            }

            if (endByte > 0)
                endByte++;
            // copy into a larger buffer to avoid reading off end
            MemorySegment end = Arena.global().allocate(MAX_LINE_LENGTH + BYTE_SPECIES.vectorByteSize());
            for (long i = endByte; i < buffer.byteSize(); i++)
                end.set(JAVA_BYTE, i - endByte, buffer.get(JAVA_BYTE, i));
            int index = 0;
            while (endByte + index < buffer.byteSize()) {
                Stat station = parseStation(index, end, stations);
                index = (int) processTemperature(index + station.name.length + 1, end, station);
            }
        }

        while (startByte < endByte) {
            Stat station = parseStation(startByte, buffer, stations);
            startByte = processTemperature(startByte + station.name.length + 1, buffer, station);
        }
        return stations;
    }

    public static class Stat {
        final byte[] name;
        int count = 0;
        short min = Short.MAX_VALUE, max = Short.MIN_VALUE;
        long total = 0;
        final long first8, second8;

        public Stat(byte[] name, long first8, long second8) {
            this.name = name;
            this.first8 = first8;
            this.second8 = second8;
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