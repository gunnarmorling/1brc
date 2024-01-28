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

import java.io.IOException;
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
 * * semicolon finding and name comparison using incubator vector api
 * * read chunks in parallel
 * * minimise allocation
 * * no unsafe
 *
 * Timings on 4 core i7-7500U CPU @ 2.70GHz:
 * average_baseline: 4m48s
 * ianopolous:         13.8s
*/
public class CalculateAverage_ianopolousfast {

    public static final int MAX_LINE_LENGTH = 107;
    public static final int MAX_STATIONS = 1 << 14;
    private static final OfLong LONG_LAYOUT = JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 16
            ? ByteVector.SPECIES_128
            : ByteVector.SPECIES_64;

    public static void main(String[] args) throws Exception {
        Arena arena = Arena.global();
        Path input = Path.of("measurements.txt");
        FileChannel channel = (FileChannel) Files.newByteChannel(input, StandardOpenOption.READ);
        long filesize = Files.size(input);
        MemorySegment mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, filesize, arena);
        int nChunks = filesize < 4 * 1024 * 1024 ? 1 : Runtime.getRuntime().availableProcessors();
        long chunkSize = (filesize + nChunks - 1) / nChunks;
        List<Stat[]> allResults = IntStream.range(0, nChunks)
                .parallel()
                .mapToObj(i -> parseStats(i * chunkSize, Math.min((i + 1) * chunkSize, filesize), mmap))
                .toList();

        TreeMap<String, Stat> merged = allResults.stream()
                .parallel()
                .flatMap(f -> {
                    try {
                        return Arrays.stream(f).filter(Objects::nonNull);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toMap(s -> s.name(), s -> s, (a, b) -> a.merge(b), TreeMap::new));
        System.out.println(merged);
    }

    public static boolean matchingStationBytes(long start, long end, MemorySegment buffer, Stat existing) {
        for (int index = 0; index < end - start; index += BYTE_SPECIES.vectorByteSize()) {
            ByteVector line = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, start + index, ByteOrder.nativeOrder(), BYTE_SPECIES.indexInRange(start + index, end));
            ByteVector found = ByteVector.fromArray(BYTE_SPECIES, existing.name, index);
            if (!found.eq(line).allTrue())
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

    public static Stat createStation(long start, long end, MemorySegment buffer) {
        byte[] stationBuffer = new byte[(int) (end - start)];
        for (long off = start; off < end; off++)
            stationBuffer[(int) (off - start)] = buffer.get(JAVA_BYTE, off);
        return new Stat(stationBuffer);
    }

    public static Stat dedupeStation(long start, long end, long hash, MemorySegment buffer, Stat[] stations) {
        int index = hashToIndex(hash, MAX_STATIONS);
        Stat match = stations[index];
        while (match != null) {
            if (matchingStationBytes(start, end, buffer, match))
                return match;
            index = (index + 1) % stations.length;
            match = stations[index];
        }
        Stat res = createStation(start, end, buffer);
        stations[index] = res;
        return res;
    }

    static long maskHighBytes(long d, int nbytes) {
        return d & (-1L << ((8 - nbytes) * 8));
    }

    public static Stat parseStation(long lineStart, MemorySegment buffer, Stat[] stations) {
        ByteVector line = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart, ByteOrder.nativeOrder());
        int keySize = line.compare(VectorOperators.EQ, ';').firstTrue();

        long first8 = buffer.get(LONG_LAYOUT, lineStart);
        long second8 = 0;
        if (keySize <= 8) {
            first8 = maskHighBytes(first8, keySize & 0x07);
        }
        else if (keySize < 16) {
            second8 = maskHighBytes(buffer.get(LONG_LAYOUT, lineStart + 8), keySize & 0x07);
        }
        else if (keySize == BYTE_SPECIES.vectorByteSize()) {
            while (buffer.get(JAVA_BYTE, lineStart + keySize) != ';') {
                keySize++;
            }
            second8 = maskHighBytes(buffer.get(LONG_LAYOUT, lineStart + 8), keySize & 0x07);
        }
        long hash = first8 ^ second8; // todo include later bytes
        return dedupeStation(lineStart, lineStart + keySize, hash, buffer, stations);
    }

    public static short getMinus(long d) {
        return ((d & 0xff00000000000000L) ^ 0x2d00000000000000L) != 0 ? 0 : (short) -1;
    }

    public static long processTemperature(long lineSplit, int size, MemorySegment buffer, Stat station) {
        long d = buffer.get(LONG_LAYOUT, lineSplit);
        // negative is either 0 or -1
        short negative = getMinus(d);
        d = d << (negative * -8);
        int dotIndex = size - 2 + negative;
        d = (d >> 8) | 0x30000000_00000000L; // add a leading 0 digit
        d = d >> 8 * (5 - dotIndex);
        short temperature = (short) ((byte) d - '0' +
                10 * (((byte) (d >> 16)) - '0') +
                100 * (((byte) (d >> 24)) - '0'));
        temperature = (short) ((temperature ^ negative) - negative); // negative treatment inspired by merkitty
        station.add(temperature);
        return lineSplit + size + 1;
    }

    private static long parseLine(long lineStart, MemorySegment buffer, Stat[] stations) {
        ByteVector line = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart, ByteOrder.nativeOrder());
        int lineSize = line.compare(VectorOperators.EQ, '\n').firstTrue();
        int index = lineSize;
        while (index == BYTE_SPECIES.vectorByteSize()) {
            index = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart + lineSize,
                    ByteOrder.nativeOrder()).compare(VectorOperators.EQ, '\n').firstTrue();
            lineSize += index;
        }
        int keySize = lineSize - 6 + ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart + lineSize - 6,
                ByteOrder.nativeOrder()).compare(VectorOperators.EQ, ';').firstTrue();

        long first8 = buffer.get(LONG_LAYOUT, lineStart);
        long second8 = 0;
        if (keySize <= 8) {
            first8 = maskHighBytes(first8, keySize & 0x07);
        }
        else if (keySize < 16) {
            second8 = maskHighBytes(buffer.get(LONG_LAYOUT, lineStart + 8), keySize & 0x07);
        }
        else if (keySize == BYTE_SPECIES.vectorByteSize()) {
            while (buffer.get(JAVA_BYTE, lineStart + keySize) != ';') {
                keySize++;
            }
            second8 = maskHighBytes(buffer.get(LONG_LAYOUT, lineStart + 8), keySize & 0x07);
        }
        long hash = first8 ^ second8; // todo include later bytes
        Stat station = dedupeStation(lineStart, lineStart + keySize, hash, buffer, stations);
        return processTemperature(lineStart + keySize + 1, lineSize - keySize - 1, buffer, station);
    }

    public static Stat[] parseStats(long startByte, long endByte, MemorySegment buffer) {
        // read first partial line
        if (startByte > 0) {
            for (int i = 0; i < MAX_LINE_LENGTH; i++) {
                byte b = buffer.get(JAVA_BYTE, startByte++);
                if (b == '\n') {
                    break;
                }
            }
        }

        Stat[] stations = new Stat[MAX_STATIONS];

        // Handle reading the very last few lines in the file
        // this allows us to not worry about reading beyond the end
        // in the inner loop (reducing branches)
        // We need at least the vector lane size bytes back
        if (endByte == buffer.byteSize()) {
            // reverse at least vector lane width
            endByte = Math.max(buffer.byteSize() - BYTE_SPECIES.vectorByteSize(), 0);
            while (endByte > 0 && buffer.get(JAVA_BYTE, endByte) != '\n')
                endByte--;

            if (endByte > 0)
                endByte++;
            // copy into a larger buffer to avoid reading off end
            MemorySegment end = Arena.global().allocate(MAX_LINE_LENGTH + BYTE_SPECIES.vectorByteSize());
            for (long i = endByte; i < buffer.byteSize(); i++)
                end.set(JAVA_BYTE, i - endByte, buffer.get(JAVA_BYTE, i));
            int index = 0;
            while (endByte + index < buffer.byteSize()) {
                Stat station = parseStation(index, end, stations);
                int tempSize = 3;
                if (end.get(JAVA_BYTE, index + station.namelen + 5) == '\n')
                    tempSize = 4;
                if (end.get(JAVA_BYTE, index + station.namelen + 6) == '\n')
                    tempSize = 5;
                index = (int) processTemperature(index + station.namelen + 1, tempSize, end, station);
            }
        }

        innerloop(startByte, endByte, buffer, stations);
        return stations;
    }

    private static void innerloop(long startByte, long endByte, MemorySegment buffer, Stat[] stations) {
        while (startByte < endByte) {
            startByte = parseLine(startByte, buffer, stations);
        }
    }

    public static class Stat {
        final byte[] name;
        final int namelen;
        int count = 0;
        short min = Short.MAX_VALUE, max = Short.MIN_VALUE;
        long total = 0;

        public Stat(byte[] name) {
            int vecSize = BYTE_SPECIES.vectorByteSize();
            int arrayLen = (name.length + vecSize - 1) / vecSize * vecSize;
            this.name = Arrays.copyOfRange(name, 0, arrayLen);
            this.namelen = name.length;
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
            return new String(Arrays.copyOfRange(name, 0, namelen));
        }

        public String toString() {
            return round((double) min) + "/" + round(((double) total) / count) + "/" + round((double) max);
        }
    }
}
