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

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_abhinavupadhyay {

    private static final String FILE_NAME = "./measurements.txt";

    private static Unsafe initUnsafe() {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final static Unsafe UNSAFE = initUnsafe();

    public static final long HASSEMI = 0x3B3B3B3B3B3B3B3BL;

    static long has0(long x) {
        return (x - 0x0101010101010101L) & (~x) & 0x8080808080808080L;
    }

    private static class Table {
        private static final int TABLE_SIZE = 1 << 21; // 0x8000; // collisions with table smaller than this. Need a better hash function
        private static final int TABLE_MASK = TABLE_SIZE - 1;
        Row[] table = new Row[TABLE_SIZE];
        byte[] array = new byte[256];

        public void put(long cityStartOffset, long cityLength, long nameHash, int temperature) {
            final long uhash = nameHash ^ (nameHash >> 29);
            int index = (int) uhash & TABLE_MASK;
            Row row = table[index];
            if (row == null) {
                int i = 0;
                for (; i < cityLength - 1;) {
                    array[i++] = UNSAFE.getByte(cityStartOffset++);
                    array[i++] = UNSAFE.getByte(cityStartOffset++);
                }
                for (; i < cityLength; i++) {
                    array[i] = UNSAFE.getByte(cityStartOffset++);
                }
                table[index] = new Row(new String(array, 0, i), temperature, temperature, 1, temperature, uhash);
                return;
            }

            while (row.hash != uhash) {
                index = (int) ((index + (uhash)) & TABLE_MASK);
                int i = 0;
                for (; i < cityLength - 1;) {
                    array[i++] = UNSAFE.getByte(cityStartOffset++);
                    array[i++] = UNSAFE.getByte(cityStartOffset++);
                }
                for (; i < cityLength; i++) {
                    array[i] = UNSAFE.getByte(cityStartOffset++);
                }
                table[index] = new Row(new String(array, 0, i), temperature, temperature, 1, temperature, uhash);
                return;
            }

            row.update(temperature);

        }

    }

    private static final class Row {
        private final String name;
        private int minTemp;
        private int maxTemp;
        private int count;
        private int sum;
        private final long hash;

        public Row(String name, int minTemp, int maxTemp, int count, int sum, long hash) {
            this.name = name;
            this.minTemp = minTemp;
            this.maxTemp = maxTemp;
            this.count = count;
            this.sum = sum;
            this.hash = hash;
        }

        void update(int temperature) {
            this.count++;
            this.sum += temperature;
            if (temperature < minTemp) {
                this.minTemp = temperature;
                return;
            }
            if (temperature > maxTemp) {
                this.maxTemp = temperature;
            }
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", this.minTemp / 10.0, this.sum / (count * 10.0), maxTemp / 10.0);
        }

        public Row update(Row value) {
            this.minTemp = Integer.min(this.minTemp, value.minTemp);
            this.maxTemp = Integer.max(this.maxTemp, value.maxTemp);
            this.count += value.count;
            this.sum += value.sum;
            return this;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            return this.hash == ((Row) obj).hash;
        }

        @Override
        public int hashCode() {
            return (int) this.hash;
        }

        private static int max(int a, int b) {
            a -= b;
            a &= (~a) >> 31;
            return a + b;
        }

        private static int min(int a, int b) {
            a -= b;
            a &= a >> 31;
            return a + b;
        }

    }

    private static Table readFile(long startAddress, long endAddress) {
        Table table = new Table();
        long currentOffset = startAddress;
        long nameHash = 1;
        while (currentOffset < endAddress) {
            long cityStart = currentOffset;
            long word = UNSAFE.getLong(currentOffset);
            long hasSemi = has0(word ^ HASSEMI);
            while (hasSemi == 0) {
                currentOffset += 8;
                for (int i = 0; i < 8; i++) {
                    byte b = (byte) ((word >> (i * 8)) & 0xff);
                    nameHash = nameHash * 31 + b;
                    nameHash += (nameHash << 10);
                    nameHash ^= (nameHash >> 6);
                }
                word = UNSAFE.getLong(currentOffset);
                hasSemi = has0(word ^ HASSEMI);
            }
            int trailingZeros = Long.numberOfTrailingZeros(hasSemi);
            int semiColonIndex = trailingZeros >> 3;
            if (trailingZeros >= 8) {
                int nonZeroBits = 64 - trailingZeros;
                nameHash ^= ((word << nonZeroBits) >> nonZeroBits);
                nameHash += (nameHash << 10);
                nameHash ^= (nameHash >> 6);
                currentOffset += semiColonIndex;
            }
            long cityLength = currentOffset - cityStart;
            currentOffset++; // skip ;
            int temperature = 0;
            int scale = 1;
            int isNegative = 1;
            byte b = UNSAFE.getByte(currentOffset++);
            if (b == '-') {
                isNegative = -1;
            }
            else {
                temperature = b - '0';
                scale = 10;
            }
            while ((b = UNSAFE.getByte(currentOffset++)) != '\n') {
                if (b == '.') {
                    continue;
                }
                temperature = temperature * scale + b - '0';
                scale = 10;
            }
            temperature *= isNegative;
            table.put(cityStart, cityLength, nameHash, temperature);
            nameHash = 1;
        }
        return table;

    }

    public static void main(String[] args) throws IOException {
        String filename = args.length > 0 ? args[0] : FILE_NAME;
        FileChannel fc = FileChannel.open(Paths.get(filename), StandardOpenOption.READ);
        final long fileSize = fc.size();
        final long startAddress = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
        final long endAddress = startAddress + fileSize;
        final long[][] segments = findSegments(startAddress, endAddress, fileSize, fileSize > 1024 * 1024 * 1024 ? 12 : 1);
        final List<Table> collect = Arrays.stream(segments).parallel().map(s -> readFile(s[0], s[1])).toList();
        Map<String, Row> finalMap = new TreeMap<>();
        for (final Table t : collect) {
            for (int j = 0; j < Table.TABLE_SIZE; j++) {
                final Row row = t.table[j];
                if (row == null) {
                    continue;
                }
                finalMap.compute(row.name, (_, v) -> v == null ? row : v.update(row));
            }
        }
        System.out.println(finalMap);
    }

    private static long[][] findSegments(long startAddress, long endAddress, long size, int segmentCount) {
        if (segmentCount == 1) {
            return new long[][]{ { startAddress, endAddress } };
        }
        long[][] segments = new long[segmentCount][2];
        long segmentSize = size / segmentCount + 1;
        int i = 0;
        long currentOffset = startAddress;
        while (currentOffset < endAddress) {
            segments[i][0] = currentOffset;
            currentOffset += segmentSize;
            currentOffset = Math.min(currentOffset, endAddress);
            if (currentOffset >= endAddress) {
                segments[i][1] = endAddress;
                break;
            }
            while (UNSAFE.getByte(currentOffset) != '\n') {
                currentOffset++;
                // align to newline boundary
            }
            segments[i++][1] = currentOffset++;
        }
        return segments;
    }
}
