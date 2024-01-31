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
 * * semicolon finding and name comparison using incubator vector api
 * * read chunks in parallel
 * * minimise allocation
 * * no unsafe
 * * process multiple lines in each thread for better ILP
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

    private static final int GOLDEN_RATIO = 0x9E3779B9;
    private static final int HASH_LROTATE = 5;

    // hash from giovannicuccu
    private static int hash(MemorySegment memorySegment, long start, int len) {
        int x;
        int y;
        if (len >= Integer.BYTES) {
            x = memorySegment.get(JAVA_INT_UNALIGNED, start);
            y = memorySegment.get(JAVA_INT_UNALIGNED, start + len - Integer.BYTES);
        }
        else {
            x = memorySegment.get(JAVA_BYTE, start);
            y = memorySegment.get(JAVA_BYTE, start + len - Byte.BYTES);
        }
        return (Integer.rotateLeft(x * GOLDEN_RATIO, HASH_LROTATE) ^ y) * GOLDEN_RATIO;
    }

    public static Stat createStation(long start, long end, MemorySegment buffer) {
        byte[] stationBuffer = new byte[(int) (end - start)];
        for (long off = start; off < end; off++)
            stationBuffer[(int) (off - start)] = buffer.get(JAVA_BYTE, off);
        return new Stat(stationBuffer);
    }

    public static Stat dedupeStation(long start, long end, MemorySegment buffer, Stat[] stations) {
        int hash = hash(buffer, start, (int) (end - start));
        int index = hash & (MAX_STATIONS - 1);
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

    public static short getMinus(long d) {
        return ((d & 0xff00000000000000L) ^ 0x2d00000000000000L) != 0 ? 0 : (short) -1;
    }

    public static void processTemperature(long lineSplit, int size, MemorySegment buffer, Stat station) {
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
    }

    private static int lineSize(long lineStart, MemorySegment buffer) {
        ByteVector line = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart, ByteOrder.nativeOrder());
        int lineSize = line.compare(VectorOperators.EQ, '\n').firstTrue();
        int index = lineSize;
        while (index == BYTE_SPECIES.vectorByteSize()) {
            index = ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart + lineSize,
                    ByteOrder.nativeOrder()).compare(VectorOperators.EQ, '\n').firstTrue();
            lineSize += index;
        }
        return lineSize;
    }

    private static int keySize(int lineSize, long lineStart, MemorySegment buffer) {
        return lineSize - 6 + ByteVector.fromMemorySegment(BYTE_SPECIES, buffer, lineStart + lineSize - 6,
                ByteOrder.nativeOrder()).compare(VectorOperators.EQ, ';').firstTrue();
    }

    public static Stat[] parseStats(long start1, long end2, MemorySegment buffer) {
        // read first partial line
        if (start1 > 0) {
            for (int i = 0; i < MAX_LINE_LENGTH; i++) {
                byte b = buffer.get(JAVA_BYTE, start1++);
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
        if (end2 == buffer.byteSize()) {
            // reverse at least vector lane width
            end2 = Math.max(buffer.byteSize() - 2 * BYTE_SPECIES.vectorByteSize(), 0);
            while (end2 > 0 && buffer.get(JAVA_BYTE, end2) != '\n')
                end2--;

            if (end2 > 0)
                end2++;
            // copy into a larger buffer to avoid reading off end
            MemorySegment end = Arena.global().allocate(MAX_LINE_LENGTH + 2 * BYTE_SPECIES.vectorByteSize());
            for (long i = end2; i < buffer.byteSize(); i++)
                end.set(JAVA_BYTE, i - end2, buffer.get(JAVA_BYTE, i));
            int index = 0;
            while (end2 + index < buffer.byteSize()) {
                int lineSize1 = lineSize(index, end);
                int semiSearchStart = index + Math.max(0, lineSize1 - 6);
                int keySize1 = semiSearchStart - index + ByteVector.fromMemorySegment(BYTE_SPECIES, end, semiSearchStart,
                        ByteOrder.nativeOrder()).compare(VectorOperators.EQ, ';').firstTrue();
                Stat station1 = dedupeStation(index, index + keySize1, end, stations);
                processTemperature(index + keySize1 + 1, lineSize1 - keySize1 - 1, end, station1);
                index += lineSize1 + 1;
            }
        }

        while (start1 < end2) {
            int lineSize1 = lineSize(start1, buffer);
            long start2 = start1 + lineSize1 + 1;
            int lineSize2 = start2 < end2 ? lineSize(start2, buffer) : 0;
            int keySize1 = keySize(lineSize1, start1, buffer);
            int keySize2 = keySize(lineSize2, start2, buffer);
            Stat station1 = dedupeStation(start1, start1 + keySize1, buffer, stations);
            processTemperature(start1 + keySize1 + 1, lineSize1 - keySize1 - 1, buffer, station1);
            if (start2 < end2) {
                Stat station2 = dedupeStation(start2, start2 + keySize2, buffer, stations);
                processTemperature(start2 + keySize2 + 1, lineSize2 - keySize2 - 1, buffer, station2);
                start1 = start2 + lineSize2 + 1;
            }
            else
                start1 += lineSize1 + 1;
        }
        return stations;
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
