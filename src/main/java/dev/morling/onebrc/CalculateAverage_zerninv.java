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
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class CalculateAverage_zerninv {
    private static final String FILE = "./measurements.txt";
    private static final int L3_CACHE_SIZE = 128 * 1024 * 1024;
    private static final int MIN_CHUNK_SIZE = 1024 * 1536;
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final int L3_CACHE_CHUNK_SIZE = (L3_CACHE_SIZE - MeasurementContainer.SIZE * MeasurementContainer.ENTRY_SIZE * CORES) / CORES - 1024 * 16;

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED.length() >= 32
            ? ByteVector.SPECIES_256
            : ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED.length() >= 8
            ? IntVector.SPECIES_256
            : IntVector.SPECIES_PREFERRED;
    private static final int CITY_NAME_VEC_LENGTH = 100 / BYTE_SPECIES.length() + 1;

    public static void main(String[] args) throws IOException, InterruptedException {
        try (var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            var fileSize = channel.size();
            var minChunkSize = calcChunkSize(fileSize);
            var segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());

            var tasks = new TaskThread[CORES];
            for (int i = 0; i < tasks.length; i++) {
                tasks[i] = new TaskThread(segment, new MeasurementContainer(segment), (int) (fileSize / minChunkSize / CORES + 1));
            }

            var chunks = splitByChunks(segment, minChunkSize);
            for (int i = 0; i < chunks.size() - 1; i++) {
                var task = tasks[i % tasks.length];
                task.addChunk(chunks.get(i), chunks.get(i + 1));
            }

            for (var task : tasks) {
                task.start();
            }

            var results = new TreeMap<String, TemperatureAggregation>();
            for (var task : tasks) {
                task.join();
                task.measurements()
                        .forEach(measurement -> {
                            var aggr = results.get(measurement.station());
                            if (aggr == null) {
                                results.put(measurement.station(), measurement.aggregation());
                            }
                            else {
                                aggr.merge(measurement.aggregation());
                            }
                        });
            }

            var bos = new BufferedOutputStream(System.out);
            bos.write(new TreeMap<>(results).toString().getBytes(StandardCharsets.UTF_8));
            bos.write('\n');
            bos.flush();
        }
    }

    private static long calcChunkSize(long fileSize) {
        if (fileSize <= L3_CACHE_CHUNK_SIZE) {
            return fileSize;
        }
        if (L3_CACHE_CHUNK_SIZE < MIN_CHUNK_SIZE) {
            return fileSize / CORES;
        }
        return L3_CACHE_CHUNK_SIZE;
    }

    private static List<Long> splitByChunks(MemorySegment segment, long minChunkSize) {
        long size = segment.byteSize();
        List<Long> result = new ArrayList<>((int) (size / minChunkSize + 1));
        long offset = 0;
        result.add(offset);
        while (offset < size) {
            offset += Math.min(size - offset, minChunkSize);
            while (offset < size && segment.get(ValueLayout.JAVA_BYTE, offset++) != '\n') {
            }
            result.add(offset);
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

        public void merge(TemperatureAggregation o) {
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

    private record Measurement(String station, TemperatureAggregation aggregation) {
    }

    private static final class MeasurementContainer {
        private static final int SIZE = 1024 * 16;

        private static final int ENTRY_SIZE = 4 + 4 + 1 + 8 + 8 + 2 + 2;
        private static final int COUNT_OFFSET = 0;
        private static final int HASH_OFFSET = 4;
        private static final int SIZE_OFFSET = 8;
        private static final int ADDRESS_OFFSET = 9;
        private static final int SUM_OFFSET = 17;
        private static final int MIN_OFFSET = 25;
        private static final int MAX_OFFSET = 27;

        private static final Unsafe UNSAFE = initUnsafe();

        private final MemorySegment segment;
        private final long address;

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

        private MeasurementContainer(MemorySegment segment) {
            this.segment = segment;
            address = UNSAFE.allocateMemory(ENTRY_SIZE * SIZE);
            UNSAFE.setMemory(address, ENTRY_SIZE * SIZE, (byte) 0);
            for (long ptr = address; ptr < address + SIZE * ENTRY_SIZE; ptr += ENTRY_SIZE) {
                UNSAFE.putShort(ptr + MIN_OFFSET, Short.MAX_VALUE);
                UNSAFE.putShort(ptr + MAX_OFFSET, Short.MIN_VALUE);
            }
        }

        public void put(long offset, byte size, int hash, short value) {
            int idx = Math.abs(hash % SIZE);
            long ptr = address + idx * ENTRY_SIZE;
            int count;

            while ((count = UNSAFE.getInt(ptr + COUNT_OFFSET)) != 0) {
                if (UNSAFE.getInt(ptr + HASH_OFFSET) == hash
                        && UNSAFE.getByte(ptr + SIZE_OFFSET) == size
                        && isEqual(UNSAFE.getLong(ptr + ADDRESS_OFFSET), offset, size)) {
                    break;
                }
                idx = (idx + 1) % SIZE;
                ptr = address + idx * ENTRY_SIZE;
            }

            UNSAFE.putInt(ptr + COUNT_OFFSET, count + 1);
            UNSAFE.putInt(ptr + HASH_OFFSET, hash);
            UNSAFE.putByte(ptr + SIZE_OFFSET, size);
            UNSAFE.putLong(ptr + ADDRESS_OFFSET, offset);

            UNSAFE.putLong(ptr + SUM_OFFSET, UNSAFE.getLong(ptr + SUM_OFFSET) + value);
            if (value < UNSAFE.getShort(ptr + MIN_OFFSET)) {
                UNSAFE.putShort(ptr + MIN_OFFSET, value);
            }
            if (value > UNSAFE.getShort(ptr + MAX_OFFSET)) {
                UNSAFE.putShort(ptr + MAX_OFFSET, value);
            }
        }

        public List<Measurement> measurements() {
            var result = new ArrayList<Measurement>(1000);
            int count;
            for (int i = 0; i < SIZE; i++) {
                long ptr = this.address + i * ENTRY_SIZE;
                count = UNSAFE.getInt(ptr + COUNT_OFFSET);
                if (count != 0) {
                    var station = createString(UNSAFE.getLong(ptr + ADDRESS_OFFSET), UNSAFE.getByte(ptr + SIZE_OFFSET));
                    var measurements = new TemperatureAggregation(
                            UNSAFE.getLong(ptr + SUM_OFFSET),
                            count,
                            UNSAFE.getShort(ptr + MIN_OFFSET),
                            UNSAFE.getShort(ptr + MAX_OFFSET));
                    result.add(new Measurement(station, measurements));
                }
            }
            return result;
        }

        private boolean isEqual(long offset, long offset2, byte size) {
            for (long i = 0; i < size / BYTE_SPECIES.length() + 1; i++) {
                var v1 = VectorHelper.readByteVector(segment, offset + i * BYTE_SPECIES.length(), offset + size + 1);
                var v2 = VectorHelper.readByteVector(segment, offset2 + i * BYTE_SPECIES.length(), offset2 + size + 1);
                if (!v1.eq(v2).allTrue()) {
                    return false;
                }
            }
            return true;
        }

        private String createString(long offset, byte size) {
            long address = segment.address() + offset;
            byte[] arr = new byte[size];
            for (int i = 0; i < size; i++) {
                arr[i] = UNSAFE.getByte(address + i);
            }
            return new String(arr);
        }
    }

    private static class VectorHelper {
        public static ByteVector readByteVector(MemorySegment segment, long offset, long end) {
            if (offset < end - BYTE_SPECIES.length()) {
                return ByteVector.fromMemorySegment(BYTE_SPECIES, segment, offset, ByteOrder.nativeOrder());
            }
            var mask = BYTE_SPECIES.indexInRange(0, end - offset);
            return ByteVector.fromMemorySegment(BYTE_SPECIES, segment, offset, ByteOrder.nativeOrder(), mask);
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
        private static final byte DELIMITER = ';';

        private static final ByteVector DELIMITER_MASK = ByteVector.broadcast(BYTE_SPECIES, DELIMITER);
        private static final IntVector[] COEFS = buildHashCoefs();

        private static IntVector[] buildHashCoefs() {
            var coefs = new int[CITY_NAME_VEC_LENGTH * BYTE_SPECIES.length() / 4];
            coefs[0] = 1;
            for (int i = 1; i < coefs.length; i++) {
                coefs[i] = coefs[i - 1] * 31;
            }
            IntVector[] vectors = new IntVector[coefs.length / INT_SPECIES.length()];
            for (int i = 0; i < vectors.length; i++) {
                vectors[i] = IntVector.fromArray(INT_SPECIES, coefs, i * INT_SPECIES.length());
            }
            return vectors;
        }

        private final MemorySegment segment;
        private final MeasurementContainer container;
        private final List<Long> begins;
        private final List<Long> ends;

        private TaskThread(MemorySegment segment, MeasurementContainer container, int chunks) {
            this.segment = segment;
            this.container = container;
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

        public List<Measurement> measurements() {
            return container.measurements();
        }

        private void calcForChunk(long offset, long end) {
            long cityOffset;
            int hashCode, temperature, word, delimiterIdx;
            byte cityNameSize;
            int speciesLength = BYTE_SPECIES.length();

            ByteVector vector;
            while (offset < end) {
                cityOffset = offset;
                hashCode = 0;
                for (int i = 0; i < CITY_NAME_VEC_LENGTH; i++) {
                    vector = VectorHelper.readByteVector(segment, offset, end);
                    delimiterIdx = vector.compare(VectorOperators.EQ, DELIMITER_MASK).firstTrue();
                    if (delimiterIdx != speciesLength) {
                        hashCode += vector.expand(BYTE_SPECIES.indexInRange(0, delimiterIdx))
                                .reinterpretAsInts()
                                .mul(COEFS[i])
                                .reduceLanes(VectorOperators.ADD);
                        offset += delimiterIdx;
                        break;
                    }
                    hashCode += vector.reinterpretAsInts()
                            .mul(COEFS[i])
                            .reduceLanes(VectorOperators.ADD);
                    offset += speciesLength;
                }
                cityNameSize = (byte) (offset - cityOffset);

                word = segment.get(ValueLayout.JAVA_INT_UNALIGNED, ++offset);
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
                    word = (word >>> 8) | (segment.get(ValueLayout.JAVA_BYTE, offset++) << 24);
                    temperature = ZERO * 111 - ((word & BYTE_MASK) * 100 + ((word >>> 8) & BYTE_MASK) * 10 + ((word >>> 24) & BYTE_MASK));
                }
                offset++;
                container.put(cityOffset, cityNameSize, hashCode, (short) temperature);
            }
        }
    }
}
