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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Runtime.getRuntime;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class CalculateAverage_anestoruk {

    private static final String path = "./measurements.txt";
    private static final int cpus = getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException {
        List<SegmentRange> rangeList = new ArrayList<>();
        MemorySegment segment;

        try (FileChannel channel = FileChannel.open(Path.of(path))) {
            final long fileSize = channel.size();
            final long chunkSize = fileSize > 10_000 ? min(Integer.MAX_VALUE - 256, fileSize / cpus) : fileSize;
            final int chunks = (int) ceil((double) fileSize / chunkSize);
            segment = channel.map(READ_ONLY, 0, fileSize, Arena.global());
            long startOffset = 0;
            long size = chunkSize;
            for (int i = 0; i < chunks && size > 0; i++) {
                long endOffset = startOffset + size;
                while (endOffset < fileSize && segment.get(JAVA_BYTE, endOffset) != '\n') {
                    endOffset++;
                }
                rangeList.add(new SegmentRange(startOffset, endOffset));
                startOffset = endOffset + 1;
                size = min(chunkSize, fileSize - startOffset);
            }
        }

        TreeMap<String, Record> result = new TreeMap<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(cpus)) {
            List<CompletableFuture<Record[]>> futures = new ArrayList<>();
            for (SegmentRange range : rangeList) {
                futures.add(supplyAsync(() -> process(range, segment), executor));
            }
            for (CompletableFuture<Record[]> future : futures) {
                try {
                    Record[] partialResult = future.get();
                    mergeResult(result, partialResult);
                }
                catch (InterruptedException | ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        System.out.println(result);
    }

    private static Record[] process(SegmentRange range, MemorySegment segment) {
        Record[] records = new Record[1024 * 100];
        byte[] cityBuffer = new byte[100];
        long offset = range.startOffset;
        byte b;
        while (offset < range.endOffset) {
            int cityLength = 0;
            int hash = 0;
            while ((b = segment.get(JAVA_BYTE, offset++)) != ';') {
                cityBuffer[cityLength++] = b;
                hash = hash * 31 + b;
            }
            hash = Math.abs(hash);
            int value = 0;
            boolean negative;
            if ((b = segment.get(JAVA_BYTE, offset++)) == '-') {
                negative = true;
            }
            else {
                negative = false;
                value = b - '0';
            }
            while ((b = segment.get(JAVA_BYTE, offset++)) != '\n') {
                if (b != '.') {
                    value = value * 10 + (b - '0');
                }
            }
            int temperature = negative ? -value : value;
            byte[] city = new byte[cityLength];
            System.arraycopy(cityBuffer, 0, city, 0, cityLength);
            addResult(records, hash, city, temperature);
        }
        return records;
    }

    private static void addResult(Record[] records, int hash, byte[] city, int temperature) {
        int idx = hash % records.length;
        Record record;
        while ((record = records[idx]) != null) {
            if (record.hash == hash && Arrays.equals(record.city, city)) {
                record.add(temperature);
                return;
            }
            idx = (idx + 1) % records.length;
        }
        records[idx] = new Record(hash, city, temperature);
    }

    private static void mergeResult(TreeMap<String, Record> result, Record[] partialResult) {
        for (Record partialRecord : partialResult) {
            if (partialRecord == null) {
                continue;
            }
            String cityName = new String(partialRecord.city, UTF_8);
            result.compute(cityName, (_, record) -> {
                if (record == null) {
                    return partialRecord;
                }
                record.merge(partialRecord);
                return record;
            });
        }
    }

    private record SegmentRange(long startOffset, long endOffset) {
    }

    private static class Record {

        private final int hash;
        private final byte[] city;
        private int min;
        private int max;
        private long sum;
        private int count;

        public Record(int hash, byte[] city, int temperature) {
            this.hash = hash;
            this.city = city;
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        public void add(int temperature) {
            min = min(min, temperature);
            max = max(max, temperature);
            sum += temperature;
            count++;
        }

        public void merge(Record other) {
            min = min(min, other.min);
            max = max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        @Override
        public String toString() {
            return "%.1f/%.1f/%.1f".formatted(
                    (min / 10.0),
                    ((double) sum / count / 10.0),
                    (max / 10.0));
        }
    }
}
