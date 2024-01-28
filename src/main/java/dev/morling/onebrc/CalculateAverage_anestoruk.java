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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            List<CompletableFuture<Map<ByteWrapper, Record>>> futures = new ArrayList<>();
            for (SegmentRange range : rangeList) {
                futures.add(supplyAsync(() -> process(range, segment), executor));
            }
            for (CompletableFuture<Map<ByteWrapper, Record>> future : futures) {
                try {
                    Map<ByteWrapper, Record> partialResult = future.get();
                    combine(result, partialResult);
                }
                catch (InterruptedException | ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        System.out.println(result);
    }

    private static Map<ByteWrapper, Record> process(SegmentRange range, MemorySegment segment) {
        Map<ByteWrapper, Record> partialResult = new HashMap<>(1_000);
        byte[] buffer = new byte[100];
        long offset = range.startOffset;
        byte b;
        while (offset < range.endOffset) {
            int cityIdx = 0;
            while ((b = segment.get(JAVA_BYTE, offset++)) != ';') {
                buffer[cityIdx++] = b;
            }
            byte[] city = new byte[cityIdx];
            System.arraycopy(buffer, 0, city, 0, cityIdx);
            ByteWrapper cityWrapper = new ByteWrapper(city);

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

            partialResult.compute(cityWrapper, (_, record) -> update(record, temperature));
        }
        return partialResult;
    }

    private record SegmentRange(long startOffset, long endOffset) {
    }

    private record ByteWrapper(byte[] bytes) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByteWrapper that = (ByteWrapper) o;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static class Record {

        private int min;
        private int max;
        private long sum;
        private int count;

        public Record(int temperature) {
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }

        @Override
        public String toString() {
            return "%.1f/%.1f/%.1f".formatted(
                    (min / 10.0),
                    ((double) sum / count / 10.0),
                    (max / 10.0));
        }
    }

    private static Record update(Record record, int temperature) {
        if (record == null) {
            return new Record(temperature);
        }
        record.min = min(record.min, temperature);
        record.max = max(record.max, temperature);
        record.sum += temperature;
        record.count++;
        return record;
    }

    private static void combine(TreeMap<String, Record> result, Map<ByteWrapper, Record> partialResult) {
        partialResult.forEach((wrapper, partialRecord) -> {
            String city = new String(wrapper.bytes, UTF_8);
            result.compute(city, (_, record) -> {
                if (record == null) {
                    return partialRecord;
                }
                record.min = min(record.min, partialRecord.min);
                record.max = max(record.max, partialRecord.max);
                record.sum += partialRecord.sum;
                record.count += partialRecord.count;
                return record;
            });
        });
    }
}
