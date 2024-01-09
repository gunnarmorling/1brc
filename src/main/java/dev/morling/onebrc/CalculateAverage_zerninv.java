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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_zerninv {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_MMAP_SIZE = Integer.MAX_VALUE;

    private static final char DELIMITER = ';';
    private static final char LINE_SEPARATOR = '\n';
    private static final char ZERO = '0';
    private static final char NINE = '9';
    private static final char MINUS = '-';

    public static void main(String[] args) throws IOException {
        var results = new HashMap<String, Result>();
        try (var raf = new RandomAccessFile(FILE, "r"); var channel = raf.getChannel()) {
            var executor = Executors.newFixedThreadPool(8);
            List<Future<Map<String, Result>>> fResults = new ArrayList<>();
            var chunks = splitByChunks(channel);
            for (int i = 1; i < chunks.size(); i++) {
                final long prev = chunks.get(i - 1);
                final long curr = chunks.get(i);
                fResults.add(executor.submit(() -> calcForChunk(channel, prev, curr)));
            }
            fResults.forEach(f -> {
                try {
                    f.get().forEach((key, value) -> {
                        if (results.containsKey(key)) {
                            results.get(key).merge(value);
                        }
                        else {
                            results.put(key, value);
                        }
                    });
                }
                catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
            executor.shutdown();
        }
        System.out.println(new TreeMap<>(results));
    }

    private static List<Long> splitByChunks(FileChannel channel) throws IOException {
        long size = channel.size();
        List<Long> result = new ArrayList<>();
        long current = 0;
        result.add(current);
        while (current < size) {
            var mbb = channel.map(FileChannel.MapMode.READ_ONLY, current, Math.min(size - current, MAX_MMAP_SIZE));
            int position = mbb.limit() - 1;
            while (mbb.get(position) != LINE_SEPARATOR) {
                position--;
            }
            current += position + 1;
            result.add(current);
        }
        return result;
    }

    private static Map<String, Result> calcForChunk(FileChannel channel, long begin, long end) throws IOException {
        var results = new HashMap<CityWrapper, Result>();
        var mbb = channel.map(FileChannel.MapMode.READ_ONLY, begin, end - begin);
        var byteBuf = ByteBuffer.allocate(5);
        int hashCode;
        byte b;

        while (mbb.position() < mbb.limit()) {
            int cityBegin = mbb.position();
            int size = 0;
            hashCode = 0;
            while ((b = mbb.get()) != DELIMITER) {
                hashCode = 31 * hashCode + b;
                size++;
            }

            while ((b = mbb.get()) != LINE_SEPARATOR) {
                byteBuf.put(b);
            }

            int value = 0;
            for (int j = 0; j < byteBuf.position(); j++) {
                byte c = byteBuf.get(j);
                if (c >= ZERO && c <= NINE) {
                    value *= 10;
                    value += (c - ZERO);
                }
            }
            if (byteBuf.get(0) == MINUS) {
                value *= -1;
            }
            byteBuf.clear();

            CityWrapper key = new CityWrapper(mbb, cityBegin, size, hashCode);
            if (!results.containsKey(key)) {
                results.put(key, new Result());
            }
            results.get(key).addPoint(value);
        }
        return results.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }

    private static final class Result {
        private long sum;
        private int count;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;

        public void addPoint(int temp) {
            sum += temp;
            count++;
            min = Math.min(temp, min);
            max = Math.max(temp, max);
        }

        public void merge(Result o) {
            if (o == null) {
                return;
            }
            sum += o.sum;
            count += o.count;
            min = Math.min(min, o.min);
            max = Math.max(max, o.max);
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10d, sum / 10d / count, max / 10d);
        }
    }

    public static final class CityWrapper {
        private final MappedByteBuffer mbb;
        private final int begin;
        private final int size;
        private final int hashCode;

        public CityWrapper(MappedByteBuffer mbb, int begin, int size, int hashCode) {
            this.mbb = mbb;
            this.begin = begin;
            this.size = size;
            this.hashCode = hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CityWrapper that = (CityWrapper) o;
            if (hashCode != that.hashCode || size != that.size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (mbb.get(begin + i) != mbb.get(that.begin + i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            byte[] arr = new byte[size];
            for (int i = 0; i < size; i++) {
                arr[i] = mbb.get(begin + i);
            }
            return new String(arr);
        }
    }
}
