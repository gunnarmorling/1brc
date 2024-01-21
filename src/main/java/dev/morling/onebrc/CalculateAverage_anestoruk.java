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
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

    private static final String path = "./measurements_1_000_000_000.txt";
    // private static final String path = "./measurements.txt";
    private static final DecimalFormat df = new DecimalFormat("0.0", new DecimalFormatSymbols(Locale.US));
    private static final int cpus = getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException {
        long startTimeMs = System.currentTimeMillis();
        df.setRoundingMode(RoundingMode.HALF_UP);

        List<MemoryRange> rangeList = new ArrayList<>();
        MemorySegment segment;
        try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
            final long fileSize = file.length();
            final long chunkSize = fileSize > 10_000 ? min(Integer.MAX_VALUE - 256, fileSize / cpus) : fileSize;
            final int chunks = (int) ceil((double) fileSize / chunkSize);

            segment = file.getChannel().map(READ_ONLY, 0, fileSize, Arena.ofShared());
            long position = 0;
            long size = chunkSize;
            for (int i = 0; i < chunks && size > 0; i++) {
                file.seek(position + size);
                while (file.getFilePointer() < fileSize && file.readByte() != '\n') {
                    size++;
                }
                rangeList.add(new MemoryRange(position, position + size));
                position += (size + 1);
                size = min(chunkSize, fileSize - position);
            }
        }

        List<Map<ByteWrapper, Record>> partialResults = new ArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(cpus)) {
            List<CompletableFuture<Map<ByteWrapper, Record>>> futures = new ArrayList<>();
            for (MemoryRange range : rangeList) {
                futures.add(supplyAsync(() -> process(range, segment), executor));
            }
            for (CompletableFuture<Map<ByteWrapper, Record>> future : futures) {
                try {
                    Map<ByteWrapper, Record> partialResult = future.get();
                    partialResults.add(partialResult);
                }
                catch (InterruptedException | ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        TreeMap<String, Record> result = partialResults.stream().collect(
                TreeMap::new, CalculateAverage_anestoruk::accumulate, TreeMap::putAll);
        System.out.println(result);
        System.out.printf("Execution took: %s ms%n", (System.currentTimeMillis() - startTimeMs));
    }

    private static Map<ByteWrapper, Record> process(MemoryRange range, MemorySegment segment) {
        try {
            Map<ByteWrapper, Record> partialResult = new HashMap<>(1_000);
            byte[] cityBuffer = new byte[100], tempBuffer = new byte[100];
            long i = range.startOffset;
            byte b;
            while (i < range.endOffset) {
                int cityIdx = 0, tempIdx = 0;
                while ((b = segment.get(JAVA_BYTE, i++)) != ';') {
                    cityBuffer[cityIdx++] = b;
                }
                while (i < range.endOffset && (b = segment.get(JAVA_BYTE, i++)) != '\n') {
                    if (b != '.') {
                        tempBuffer[tempIdx++] = b;
                    }
                }
                byte[] city = new byte[cityIdx];
                System.arraycopy(cityBuffer, 0, city, 0, cityIdx);
                ByteWrapper cityWrapper = new ByteWrapper(city);

                byte[] temp = new byte[tempIdx];
                System.arraycopy(tempBuffer, 0, temp, 0, tempIdx);
                int temperature = toInt(temp);

                partialResult.compute(cityWrapper, (_, record) -> update(record, temperature));
            }
            return partialResult;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private record MemoryRange(long startOffset, long endOffset) {
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

        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0;
        private int count = 0;

        @Override
        public String toString() {
            return "%s/%s/%s".formatted(
                    df.format(min / 10.0),
                    df.format((double) sum / count / 10.0),
                    df.format(max / 10.0));
        }
    }

    private static int toInt(byte[] arr) {
        int result = 0;
        boolean negative = false;
        for (byte b : arr) {
            if (b == '-') {
                negative = true;
                continue;
            }
            result = result * 10 + (b - '0');
        }
        return negative ? -result : result;
    }

    private static Record update(Record record, int temperature) {
        if (record == null) {
            record = new Record();
        }
        record.min = min(record.min, temperature);
        record.max = max(record.max, temperature);
        record.sum += temperature;
        record.count++;
        return record;
    }

    private static void accumulate(TreeMap<String, Record> result, Map<ByteWrapper, Record> partialResult) {
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
