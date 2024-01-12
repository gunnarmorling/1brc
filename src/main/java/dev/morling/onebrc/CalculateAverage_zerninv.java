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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_zerninv {
    private static final String FILE = "./measurements.txt";
    private static final int MIN_CHUNK_SIZE = 1024 * 1024 * 16;
    private static final char DELIMITER = ';';
    private static final char LINE_SEPARATOR = '\n';
    private static final char ZERO = '0';
    private static final char NINE = '9';
    private static final char MINUS = '-';

    private static final Unsafe UNSAFE = initUnsafe();

    public static void main(String[] args) throws IOException {
        var results = new HashMap<String, MeasurementAggregation>();
        try (var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            var fileSize = channel.size();
            var memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            long address = memorySegment.address();
            var cores = Runtime.getRuntime().availableProcessors();
            var chunkAmount = cores - 1;
            // var maxChunkSize = Math.min(fileSize, MIN_CHUNK_SIZE);
            var maxChunkSize = fileSize < MIN_CHUNK_SIZE ? fileSize : fileSize / chunkAmount;
            var chunks = splitByChunks(address, address + fileSize, maxChunkSize);

            var executor = Executors.newFixedThreadPool(cores);
            List<Future<Map<String, MeasurementAggregation>>> fResults = new ArrayList<>();
            for (int i = 1; i < chunks.size(); i++) {
                final long prev = chunks.get(i - 1);
                final long curr = chunks.get(i);
                fResults.add(executor.submit(() -> calcForChunk(prev, curr)));
            }

            fResults.forEach(f -> {
                try {
                    f.get().forEach((key, value) -> {
                        var result = results.get(key);
                        if (result != null) {
                            result.merge(value);
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

        var bos = new BufferedOutputStream(System.out);
        bos.write(new TreeMap<>(results).toString().getBytes(StandardCharsets.UTF_8));
        bos.write('\n');
        bos.flush();
    }

    private static Unsafe initUnsafe() {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            return (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Long> splitByChunks(long address, long end, long maxChunkSize) {
        List<Long> result = new ArrayList<>();
        result.add(address);
        while (address < end) {
            long ptr = address + Math.min(end - address, maxChunkSize) - 1;
            while (UNSAFE.getByte(ptr) != LINE_SEPARATOR) {
                ptr--;
            }
            address = ptr + 1;
            result.add(address);
        }
        return result;
    }

    private static Map<String, MeasurementAggregation> calcForChunk(long offset, long end) {
        var results = new MeasurementContainer();

        long cityOffset, temperatureOffset;
        int hashCode, temperature;
        byte cityNameSize, b;

        while (offset < end) {
            cityOffset = offset;
            hashCode = 0;
            while ((b = UNSAFE.getByte(offset++)) != DELIMITER) {
                hashCode = 31 * hashCode + b;
            }

            temperatureOffset = offset;
            cityNameSize = (byte) (temperatureOffset - cityOffset - 1);

            temperature = 0;
            while ((b = UNSAFE.getByte(offset++)) != LINE_SEPARATOR) {
                if (b >= ZERO && b <= NINE) {
                    temperature = temperature * 10 + (b - ZERO);
                }
            }
            if (UNSAFE.getByte(temperatureOffset) == MINUS) {
                temperature *= -1;
            }
            results.put(cityOffset, cityNameSize, hashCode, (short) temperature);
        }
        return results.toStringMap();
    }

    private static final class MeasurementAggregation {
        private long sum;
        private long count;
        private short min;
        private short max;

        public MeasurementAggregation(long sum, long count, short min, short max) {
            this.sum = sum;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public void merge(MeasurementAggregation o) {
            if (o == null) {
                return;
            }
            sum += o.sum;
            count += o.count;
            min = min < o.min ? min : o.min;
            max = max > o.max ? max : o.max;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10d, sum / 10d / count, max / 10d);
        }
    }

    private static final class MeasurementContainer {
        private static final int SIZE = 1024 * 16;

        private static final int ENTRY_SIZE = 8 + 1 + 4 + 8 + 8 + 2 + 2;
        private static final int COUNT_OFFSET = 0;
        private static final int SIZE_OFFSET = 8;
        private static final int HASH_OFFSET = 9;
        private static final int ADDRESS_OFFSET = 13;
        private static final int SUM_OFFSET = 21;
        private static final int MIN_OFFSET = 29;
        private static final int MAX_OFFSET = 31;

        private final long address;

        private MeasurementContainer() {
            address = UNSAFE.allocateMemory(ENTRY_SIZE * SIZE);
            UNSAFE.setMemory(address, ENTRY_SIZE * SIZE, (byte) 0);
            for (long ptr = address; ptr < address + SIZE * ENTRY_SIZE; ptr += ENTRY_SIZE) {
                UNSAFE.putShort(ptr + MIN_OFFSET, Short.MAX_VALUE);
                UNSAFE.putShort(ptr + MAX_OFFSET, Short.MIN_VALUE);
            }
        }

        public void put(long address, byte size, int hash, short value) {
            long ptr = findAddress(address, size, hash);

            UNSAFE.putLong(ptr + COUNT_OFFSET, UNSAFE.getLong(ptr + COUNT_OFFSET) + 1);
            UNSAFE.putByte(ptr + SIZE_OFFSET, size);
            UNSAFE.putInt(ptr + HASH_OFFSET, hash);
            UNSAFE.putLong(ptr + ADDRESS_OFFSET, address);

            UNSAFE.putLong(ptr + SUM_OFFSET, UNSAFE.getLong(ptr + SUM_OFFSET) + value);
            if (value < UNSAFE.getShort(ptr + MIN_OFFSET)) {
                UNSAFE.putShort(ptr + MIN_OFFSET, value);
            }
            if (value > UNSAFE.getShort(ptr + MAX_OFFSET)) {
                UNSAFE.putShort(ptr + MAX_OFFSET, value);
            }
        }

        public Map<String, MeasurementAggregation> toStringMap() {
            var result = new HashMap<String, MeasurementAggregation>();
            for (int i = 0; i < SIZE; i++) {
                long ptr = this.address + i * ENTRY_SIZE;
                if (UNSAFE.getLong(ptr + COUNT_OFFSET) != 0) {
                    var measurements = new MeasurementAggregation(
                            UNSAFE.getLong(ptr + SUM_OFFSET),
                            UNSAFE.getLong(ptr + COUNT_OFFSET),
                            UNSAFE.getShort(ptr + MIN_OFFSET),
                            UNSAFE.getShort(ptr + MAX_OFFSET));
                    var key = createString(UNSAFE.getLong(ptr + ADDRESS_OFFSET), UNSAFE.getByte(ptr + SIZE_OFFSET));
                    result.put(key, measurements);
                }
            }
            return result;
        }

        private long findAddress(long address, byte size, int hash) {
            int idx = Math.abs(hash % SIZE);
            long ptr = this.address + idx * ENTRY_SIZE;
            while (UNSAFE.getLong(ptr + COUNT_OFFSET) != 0) {
                if (UNSAFE.getByte(ptr + SIZE_OFFSET) == size
                        && UNSAFE.getInt(ptr + HASH_OFFSET) == hash
                        && isEqual(UNSAFE.getLong(ptr + ADDRESS_OFFSET), address, size)) {
                    break;
                }
                idx = (idx + 1) % SIZE;
                ptr = this.address + idx * ENTRY_SIZE;
            }
            return ptr;
        }

        private boolean isEqual(long address, long address2, byte size) {
            for (int i = 0; i < size; i++) {
                if (UNSAFE.getByte(address + i) != UNSAFE.getByte(address2 + i)) {
                    return false;
                }
            }
            return true;
        }

        private String createString(long address, byte size) {
            byte[] arr = new byte[size];
            for (int i = 0; i < size; i++) {
                arr[i] = UNSAFE.getByte(address + i);
            }
            return new String(arr);
        }
    }
}
