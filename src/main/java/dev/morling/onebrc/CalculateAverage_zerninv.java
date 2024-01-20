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

public class CalculateAverage_zerninv {
    private static final String FILE = "./measurements.txt";
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final int CHUNK_SIZE = 1024 * 1024 * 32;

    private static final Unsafe UNSAFE = initUnsafe();

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

    public static void main(String[] args) throws IOException, InterruptedException {
        try (var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            var fileSize = channel.size();
            var minChunkSize = Math.min(fileSize, CHUNK_SIZE);
            var segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());

            var tasks = new TaskThread[CORES];
            for (int i = 0; i < tasks.length; i++) {
                tasks[i] = new TaskThread((int) (fileSize / minChunkSize / CORES + 1));
            }

            var results = new HashMap<String, TemperatureAggregation>();
            var chunks = splitByChunks(segment.address(), segment.address() + fileSize, minChunkSize, results);
            for (int i = 0; i < chunks.size() - 1; i++) {
                var task = tasks[i % tasks.length];
                task.addChunk(chunks.get(i), chunks.get(i + 1));
            }

            for (var task : tasks) {
                task.start();
            }

            for (var task : tasks) {
                task.join();
                task.collectTo(results);
            }

            var bos = new BufferedOutputStream(System.out);
            bos.write(new TreeMap<>(results).toString().getBytes(StandardCharsets.UTF_8));
            bos.write('\n');
            bos.flush();
        }
    }

    private static List<Long> splitByChunks(long address, long end, long minChunkSize, Map<String, TemperatureAggregation> results) {
        // handle last line
        long offset = end - 1;
        int temperature = 0;
        byte b;
        int multiplier = 1;
        while ((b = UNSAFE.getByte(offset--)) != ';') {
            if (b >= '0' && b <= '9') {
                temperature += (b - '0') * multiplier;
                multiplier *= 10;
            }
            else if (b == '-') {
                temperature = -temperature;
            }
        }
        long cityNameEnd = offset;
        while (UNSAFE.getByte(offset - 1) != '\n' && offset > address) {
            offset--;
        }
        var cityName = new byte[(int) (cityNameEnd - offset + 1)];
        UNSAFE.copyMemory(null, offset, cityName, Unsafe.ARRAY_BYTE_BASE_OFFSET, cityName.length);
        results.put(new String(cityName, StandardCharsets.UTF_8), new TemperatureAggregation(temperature, 1, (short) temperature, (short) temperature));

        // split by chunks
        end = offset;
        List<Long> result = new ArrayList<>((int) ((end - address) / minChunkSize + 1));
        result.add(address);
        while (address < end) {
            address += Math.min(end - address, minChunkSize);
            while (address < end && UNSAFE.getByte(address++) != '\n') {
            }
            result.add(address);
        }
        return result;
    }

    private static final class TemperatureAggregation {
        private long sum;
        private int count;
        private short min;
        private short max;

        public TemperatureAggregation(long sum, int count, short min, short max) {
            this.sum = sum;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public void merge(long sum, int count, short min, short max) {
            this.sum += sum;
            this.count += count;
            this.min = this.min < min ? this.min : min;
            this.max = this.max > max ? this.max : max;
        }

        @Override
        public String toString() {
            return min / 10d + "/" + Math.round(sum / 1d / count) / 10d + "/" + max / 10d;
        }
    }

    private static final class MeasurementContainer {
        private static final int SIZE = 1 << 17;

        private static final int ENTRY_SIZE = 4 + 4 + 8 + 1 + 8 + 8 + 2 + 2;
        private static final int COUNT_OFFSET = 0;
        private static final int HASH_OFFSET = 4;
        private static final int LAST_BYTES_OFFSET = 8;
        private static final int SIZE_OFFSET = 16;
        private static final int ADDRESS_OFFSET = 17;
        private static final int SUM_OFFSET = 25;
        private static final int MIN_OFFSET = 33;
        private static final int MAX_OFFSET = 35;

        private final long address;

        private MeasurementContainer() {
            address = UNSAFE.allocateMemory(ENTRY_SIZE * SIZE);
            UNSAFE.setMemory(address, ENTRY_SIZE * SIZE, (byte) 0);
        }

        public void put(long address, byte size, int hash, long lastBytes, short value) {
            int idx = Math.abs(hash % SIZE);
            long ptr = this.address + idx * ENTRY_SIZE;
            int count;
            boolean fastEqual;

            while ((count = UNSAFE.getInt(ptr + COUNT_OFFSET)) != 0) {
                fastEqual = UNSAFE.getInt(ptr + HASH_OFFSET) == hash && UNSAFE.getLong(ptr + LAST_BYTES_OFFSET) == lastBytes;
                if (fastEqual && UNSAFE.getByte(ptr + SIZE_OFFSET) == size && isEqual(UNSAFE.getLong(ptr + ADDRESS_OFFSET), address, size - 8)) {

                    UNSAFE.putInt(ptr + COUNT_OFFSET, count + 1);
                    UNSAFE.putLong(ptr + ADDRESS_OFFSET, address);
                    UNSAFE.putLong(ptr + SUM_OFFSET, UNSAFE.getLong(ptr + SUM_OFFSET) + value);
                    if (value < UNSAFE.getShort(ptr + MIN_OFFSET)) {
                        UNSAFE.putShort(ptr + MIN_OFFSET, value);
                    }
                    if (value > UNSAFE.getShort(ptr + MAX_OFFSET)) {
                        UNSAFE.putShort(ptr + MAX_OFFSET, value);
                    }
                    return;
                }
                idx = (idx + 1) % SIZE;
                ptr = this.address + idx * ENTRY_SIZE;
            }

            UNSAFE.putInt(ptr + COUNT_OFFSET, 1);
            UNSAFE.putInt(ptr + HASH_OFFSET, hash);
            UNSAFE.putLong(ptr + LAST_BYTES_OFFSET, lastBytes);
            UNSAFE.putByte(ptr + SIZE_OFFSET, size);
            UNSAFE.putLong(ptr + ADDRESS_OFFSET, address);

            UNSAFE.putLong(ptr + SUM_OFFSET, value);
            UNSAFE.putShort(ptr + MIN_OFFSET, value);
            UNSAFE.putShort(ptr + MAX_OFFSET, value);
        }

        public void collectTo(Map<String, TemperatureAggregation> results) {
            int count;
            for (int i = 0; i < SIZE; i++) {
                long ptr = this.address + i * ENTRY_SIZE;
                count = UNSAFE.getInt(ptr + COUNT_OFFSET);
                if (count != 0) {
                    var station = createString(UNSAFE.getLong(ptr + ADDRESS_OFFSET), UNSAFE.getByte(ptr + SIZE_OFFSET));
                    var result = results.get(station);
                    if (result == null) {
                        results.put(station, new TemperatureAggregation(
                                UNSAFE.getLong(ptr + SUM_OFFSET),
                                count,
                                UNSAFE.getShort(ptr + MIN_OFFSET),
                                UNSAFE.getShort(ptr + MAX_OFFSET)));
                    }
                    else {
                        result.merge(UNSAFE.getLong(ptr + SUM_OFFSET), count, UNSAFE.getShort(ptr + MIN_OFFSET), UNSAFE.getShort(ptr + MAX_OFFSET));
                    }
                }
            }
        }

        private boolean isEqual(long address, long address2, int size) {
            for (int i = 0; i < size; i += 8) {
                if (UNSAFE.getLong(address + i) != UNSAFE.getLong(address2 + i)) {
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

    private static class TaskThread extends Thread {
        // #.##
        private static final int THREE_DIGITS_MASK = 0x2e0000;
        // #.#
        private static final int TWO_DIGITS_MASK = 0x2e00;
        // #.#-
        private static final int TWO_NEGATIVE_DIGITS_MASK = 0x2e002d;
        private static final int BYTE_MASK = 0xff;

        private static final int ZERO = '0';
        private static final long DELIMITER_MASK = 0x3b3b3b3b3b3b3b3bL;
        private static final long[] SIGNIFICANT_BYTES_MASK = {
                0,
                0xff,
                0xffff,
                0xffffff,
                0xffffffffL,
                0xffffffffffL,
                0xffffffffffffL,
                0xffffffffffffffL,
                0xffffffffffffffffL
        };

        private final MeasurementContainer container;
        private final List<Long> begins;
        private final List<Long> ends;

        private TaskThread(int chunks) {
            this.container = new MeasurementContainer();
            this.begins = new ArrayList<>(chunks);
            this.ends = new ArrayList<>(chunks);
        }

        public void addChunk(long begin, long end) {
            begins.add(begin);
            ends.add(end);
        }

        @Override
        public void run() {
            for (int i = 0; i < begins.size(); i++) {
                calcForChunk(begins.get(i), ends.get(i));
            }
        }

        private void calcForChunk(long offset, long end) {
            long cityOffset, lastBytes, city, masked, hashCode;
            int temperature, word, delimiterIdx;
            byte cityNameSize;

            while (offset < end) {
                cityOffset = offset;
                lastBytes = 0;
                hashCode = 0;
                delimiterIdx = 8;

                while (delimiterIdx == 8) {
                    city = UNSAFE.getLong(offset);
                    masked = city ^ DELIMITER_MASK;
                    masked = (masked - 0x0101010101010101L) & ~masked & 0x8080808080808080L;
                    delimiterIdx = Long.numberOfTrailingZeros(masked) >>> 3;
                    if (delimiterIdx == 0) {
                        break;
                    }
                    offset += delimiterIdx;
                    lastBytes = city & SIGNIFICANT_BYTES_MASK[delimiterIdx];
                    hashCode = ((hashCode >>> 5) ^ lastBytes) * 0x517cc1b727220a95L;
                }

                cityNameSize = (byte) (offset - cityOffset);

                word = UNSAFE.getInt(++offset);
                offset += 4;

                if ((word & TWO_NEGATIVE_DIGITS_MASK) == TWO_NEGATIVE_DIGITS_MASK) {
                    word >>>= 8;
                    temperature = ZERO * 11 - ((word & BYTE_MASK) * 10 + ((word >>> 16) & BYTE_MASK));
                }
                else if ((word & THREE_DIGITS_MASK) == THREE_DIGITS_MASK) {
                    temperature = (word & BYTE_MASK) * 100 + ((word >>> 8) & BYTE_MASK) * 10 + ((word >>> 24) & BYTE_MASK) - ZERO * 111;
                }
                else if ((word & TWO_DIGITS_MASK) == TWO_DIGITS_MASK) {
                    temperature = (word & BYTE_MASK) * 10 + ((word >>> 16) & BYTE_MASK) - ZERO * 11;
                    offset--;
                }
                else {
                    // #.##-
                    word = (word >>> 8) | (UNSAFE.getByte(offset++) << 24);
                    temperature = ZERO * 111 - ((word & BYTE_MASK) * 100 + ((word >>> 8) & BYTE_MASK) * 10 + ((word >>> 24) & BYTE_MASK));
                }
                offset++;
                container.put(cityOffset, cityNameSize, Long.hashCode(hashCode), lastBytes, (short) temperature);
            }
        }

        public void collectTo(Map<String, TemperatureAggregation> results) {
            container.collectTo(results);
        }
    }
}
