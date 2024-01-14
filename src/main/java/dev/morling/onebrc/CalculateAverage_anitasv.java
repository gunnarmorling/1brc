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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_anitasv {
    private static final String FILE = "./measurements.txt";

    private static class LongHashEntry<T> {
        long key;
        T value;
        int next;
    }

    // Stolen from java.util.HashMap
    static final int MAXIMUM_CAPACITY = 1 << 30;

    static int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    private static class LongHashMap<T> {
        private final LongHashEntry<T>[] entries;
        private int next = -1;

        @SuppressWarnings("unchecked")
        private LongHashMap(int capacity) {
            this.entries = (LongHashEntry<T>[]) new LongHashEntry[tableSizeFor(capacity)];
            for (int i = 0; i < entries.length; i++) {
                this.entries[i] = new LongHashEntry<>();
            }
        }

        public LongHashEntry<T> find(long key) {
            int start = Long.hashCode(key) & (entries.length - 1);
            int index = start;
            do {
                LongHashEntry<T> entry = entries[index];
                if (entry.key == key) {
                    return entry;
                }
                else if (entry.value == null) {
                    entry.key = key;
                    entry.next = next;
                    next = index;
                    return entry;
                }
                index++;
                if (index == entries.length) {
                    index = 0;
                }
            } while (index != start);
            return null;
        }

        public List<T> values() {
            List<T> values = new ArrayList<>();
            int scan = next;
            while (scan != -1) {
                LongHashEntry<T> entry = entries[scan];
                values.add(entry.value);
                scan = entry.next;
            }
            return values;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            int scan = next;
            while (scan != -1) {
                LongHashEntry<T> entry = entries[scan];
                builder.append(entry.key).append(",").append(entry.value).append("\n");
                scan = entry.next;
            }
            return builder.toString();
        }
    }

    private record Shard(MemorySegment mmapMemory,
                         long chunkStart, long chunkEnd) {

        byte getByte(long address) {
            return mmapMemory.get(ValueLayout.JAVA_BYTE, address);
        }

        long getAlignedLong(long address) {
            return mmapMemory.get(ValueLayout.JAVA_LONG, address);
        }

        long getSlowLong(long address) {
            return mmapMemory.get(ValueLayout.JAVA_LONG_UNALIGNED, address);
        }

        long indexOf(long position, byte ch) {
            ByteBuffer buf = mmapMemory.asSlice(position,
                            Math.min(128, mmapMemory.byteSize() - position))
                    .asByteBuffer();
            while (buf.hasRemaining()) {
                if (buf.get() == ch) {
                    return position + buf.position() - 1;
                }
            }
            return -1;
        }

        byte[] getRange(long start, long end) {
            return mmapMemory.asSlice(start, end - start).toArray(ValueLayout.JAVA_BYTE);
        }

        int parseDouble(long start, long end) {
            int normalized = 0;
            boolean sign = true;
            long index = start;
            if (getByte(index) == '-') {
                index++;
                sign = false;
            }
            boolean hasDot = false;
            for (; index < end; index++) {
                byte ch = getByte(index);
                if (ch != '.') {
                    normalized = normalized * 10 + (ch - '0');
                } else {
                    hasDot = true;
                }
            }
            if (!hasDot) {
                normalized *= 10;
            }
            if (!sign) {
                normalized = -normalized;
            }
            return normalized;
        }

        public long computeHash(long position, long stationEnd) {
            ByteBuffer buf2 = mmapMemory.asSlice(position, stationEnd - position)
                    .asByteBuffer();
            return buf2.hashCode();
        }

        public boolean matches(byte[] existingStation, long start, long end) {
            ByteBuffer buf1 = ByteBuffer.wrap(existingStation);
            ByteBuffer buf2 = mmapMemory.asSlice(start, end - start).asByteBuffer();
            return buf1.equals(buf2);
        }
    }

    private record ResultRow(byte[] station, IntSummaryStatistics statistics) {

        public String toString() {
            return STR."\{new String(station, StandardCharsets.UTF_8)} : \{statToString(statistics)}";
        }
    }

    private static Map<String, IntSummaryStatistics> process(Shard shard) {
        LongHashMap<List<ResultRow>> result = new LongHashMap<>(1 << 14);

        boolean skip = shard.chunkStart != 0;
        for (long position = shard.chunkStart; position < shard.chunkEnd; position++) {
            if (skip) {
                position = shard.indexOf(position, (byte) '\n');
                skip = false;
            }
            else {
                long stationEnd = shard.indexOf(position, (byte) ';');
                long hash = shard.computeHash(position, stationEnd);

                long temperatureEnd = shard.indexOf(stationEnd + 1, (byte) '\n');
                int temperature = shard.parseDouble(stationEnd + 1, temperatureEnd);

                LongHashEntry<List<ResultRow>> entry = result.find(hash);
                if (entry == null) {
                    throw new IllegalStateException("Not enough space in hashmap.");
                }
                List<ResultRow> collisions = entry.value;
                if (collisions == null) {
                    collisions = new ArrayList<>();
                    entry.value = collisions;
                }

                boolean found = false;
                for (ResultRow existing : collisions) {
                    byte[] existingStation = existing.station();
                    if (shard.matches(existingStation, position, stationEnd)) {
                        existing.statistics.accept(temperature);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    IntSummaryStatistics stats = new IntSummaryStatistics();
                    stats.accept(temperature);
                    ResultRow rr = new ResultRow(shard.getRange(position, stationEnd), stats);
                    collisions.add(rr);
                }
                position = temperatureEnd;
            }
        }

        return result.values()
                .stream()
                .flatMap(Collection::stream)
                .map(rr -> new AbstractMap.SimpleImmutableEntry<>(
                        new String(rr.station, StandardCharsets.UTF_8),
                        rr.statistics))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, IntSummaryStatistics> combineResults(List<Map<String, IntSummaryStatistics>> list) {

        Map<String, IntSummaryStatistics> output = HashMap.newHashMap(1024);
        for (Map<String, IntSummaryStatistics> map : list) {
            for (Map.Entry<String, IntSummaryStatistics> entry : map.entrySet()) {
                output.compute(entry.getKey(), (ignore, val) -> {
                    if (val == null) {
                        return entry.getValue();
                    }
                    else {
                        val.combine(entry.getValue());
                        return val;
                    }
                });
            }
        }

        return output;
    }

    private static Map<String, IntSummaryStatistics> master(MemorySegment mmapMemory) {
        long totalBytes = mmapMemory.byteSize();
        int numWorkers = Runtime.getRuntime().availableProcessors();
        long chunkSize = Math.ceilDiv(totalBytes, numWorkers);
        return combineResults(IntStream.range(0, numWorkers)
                .parallel()
                .mapToObj(workerId -> {
                    long chunkStart = workerId * chunkSize;
                    long chunkEnd = Math.min(chunkStart + chunkSize + 1, totalBytes);
                    return new Shard(mmapMemory, chunkStart, chunkEnd);
                })
                .map(CalculateAverage_anitasv::process)
                .toList());
    }

    public static Map<String, IntSummaryStatistics> start() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(FILE),
                StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            MemorySegment mmapMemory = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0, fileSize, Arena.global());
            return master(mmapMemory);
        }
    }

    private static Map<String, String> toPrintMap(Map<String, IntSummaryStatistics> output) {
        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<String, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(entry.getKey(), statToString(stat));
        }
        return outputStr;
    }

    private static String statToString(IntSummaryStatistics stat) {
        return STR."\{stat.getMin() / 10.0}/\{Math.round(stat.getAverage()) / 10.0}/\{stat.getMax() / 10.0}";
    }

    public static void main(String[] args) throws IOException {
        System.out.println(toPrintMap(start()));
    }
}
