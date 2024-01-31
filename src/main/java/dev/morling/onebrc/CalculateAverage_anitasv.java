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
import java.util.stream.IntStream;

public class CalculateAverage_anitasv {
    private static final String FILE = "./measurements.txt";

    private record Shard(MemorySegment mmapMemory,
                         long chunkStart, long chunkEnd) {

        byte getByte(long address) {
            return mmapMemory.get(ValueLayout.JAVA_BYTE, address);
        }

        long indexOf(long position, byte ch) {
            ByteBuffer buf = mmapMemory.asSlice(position,
                            Math.min(128, mmapMemory.byteSize() - position))
                    .asByteBuffer();
            while (buf.hasRemaining()) {
                if (buf.get() == ch) {
                    return position + (buf.position() - 1);
                }
            }
            return -1;
        }

        MemorySegment getRange(long start, long end) {
            return mmapMemory.asSlice(start, end - start);
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

        public int computeHash(long position, long stationEnd) {
            ByteBuffer buf2 = mmapMemory.asSlice(position, stationEnd - position)
                    .asByteBuffer();
            return buf2.hashCode();
        }

        public long truncate(long index) {
            return Math.min(index, mmapMemory.byteSize());
        }

        public long getLong(long position) {
            return mmapMemory.get(ValueLayout.JAVA_LONG_UNALIGNED, position);
        }
    }

    private record ResultRow(IntSummaryStatistics statistics, int keyLength, int next) {
    }

    private static class FastHashMap {
        private final byte[] keys;
        private final ResultRow[] values;

        private final int capacityMinusOne;

        private final MemorySegment keySegment;

        private int next = -1;

        private FastHashMap(int capacity) {
            this.capacityMinusOne = capacity - 1;
            this.keys = new byte[capacity << 7];
            this.keySegment = MemorySegment.ofArray(keys);
            this.values = new ResultRow[capacity];
        }

        IntSummaryStatistics find(int hash, Shard shard, long stationStart, long stationEnd) {
            int initialIndex = hash & capacityMinusOne;
            int lookupLength = (int) (stationEnd - stationStart);
            int lookupAligned = ((lookupLength + 7) & (-8));
            int i = initialIndex;

            lookupAligned = (int) (shard.truncate(stationStart + lookupAligned) - stationStart) - 7;

            do {
                int keyIndex = i << 7;

                if (keys[keyIndex] != 0 && keys[keyIndex + lookupLength] == 0) {

                    int mismatch = -1, j;
                    for (j = 0; j < lookupAligned; j += 8) {
                        long entryLong = keySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyIndex + j);
                        long lookupLong = shard.getLong(stationStart + j);
                        if (entryLong != lookupLong) {
                            int diff = Long.numberOfTrailingZeros(entryLong ^ lookupLong);
                            mismatch = j + (diff >> 3);
                            break;
                        }
                    }
                    if (mismatch == -1) {
                        for (; j < lookupLength; j++) {
                            byte entryByte = keys[keyIndex + j];
                            byte lookupByte = shard.getByte(stationStart + j);
                            if (entryByte != lookupByte) {
                                mismatch = j;
                                break;
                            }
                        }
                    }
                    if (mismatch == -1 || mismatch >= lookupLength) {
                        return this.values[i].statistics;
                    }
                }
                if (keys[keyIndex] == 0) {
                    MemorySegment fullLookup = shard.getRange(stationStart, stationEnd);

                    keySegment.asSlice(keyIndex, lookupLength)
                            .copyFrom(fullLookup);

                    keys[keyIndex + lookupLength] = 0;
                    IntSummaryStatistics stats = new IntSummaryStatistics();
                    ResultRow resultRow = new ResultRow(stats, lookupLength, this.next);
                    this.next = i;
                    this.values[i] = resultRow;
                    return stats;
                }

                if (i == capacityMinusOne) {
                    i = 0;
                }
                else {
                    i++;
                }
            } while (i != initialIndex);
            throw new IllegalStateException("Hash size too small");
        }

        Iterable<Map.Entry<String, IntSummaryStatistics>> values() {
            return () -> new Iterator<>() {

                int scan = FastHashMap.this.next;

                @Override
                public boolean hasNext() {
                    return scan != -1;
                }

                @Override
                public Map.Entry<String, IntSummaryStatistics> next() {
                    ResultRow resultRow = values[scan];
                    IntSummaryStatistics stats = resultRow.statistics;
                    String key = new String(keys, scan << 7, resultRow.keyLength,
                            StandardCharsets.UTF_8);
                    scan = resultRow.next;
                    return new AbstractMap.SimpleEntry<>(key, stats);
                }
            };
        }

    }

    private static Iterable<Map.Entry<String, IntSummaryStatistics>> process(Shard shard) {
        FastHashMap result = new FastHashMap(1 << 14);

        boolean skip = shard.chunkStart != 0;
        for (long position = shard.chunkStart; position < shard.chunkEnd; position++) {
            if (skip) {
                position = shard.indexOf(position, (byte) '\n');
                skip = false;
            }
            else {
                long stationEnd = shard.indexOf(position, (byte) ';');
                int hash = shard.computeHash(position, stationEnd);

                long temperatureEnd = shard.indexOf(stationEnd + 1, (byte) '\n');
                int temperature = shard.parseDouble(stationEnd + 1, temperatureEnd);

                IntSummaryStatistics stats = result.find(hash, shard, position, stationEnd);
                stats.accept(temperature);
                position = temperatureEnd;
            }
        }

        return result.values();
    }

    private static Map<String, IntSummaryStatistics> combineResults(List<Iterable<Map.Entry<String, IntSummaryStatistics>>> list) {
        Map<String, IntSummaryStatistics> output = HashMap.newHashMap(1024);
        for (Iterable<Map.Entry<String, IntSummaryStatistics>> map : list) {
            for (Map.Entry<String, IntSummaryStatistics> entry : map) {
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
