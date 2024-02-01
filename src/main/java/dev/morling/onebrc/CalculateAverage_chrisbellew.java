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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.TreeMap;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

/**
 * This is Chris Bellew's implementation. Here are the key points:
 * 
 * - The file is equally split into ranges, one range per thread.
 *   18 threads was experimentally found to be optimal.
 * 
 * - Each thread memory maps the file range it is responsible for and
 *   then iterates through the range, one smaller buffer at a time.
 * 
 * - The contents are parsed by using SIMD vector equality comparisons
 *   between the source data and the newline character, effectively
 *   delimiting each line. The measurement of each line is discovered
 *   by moving back from the end of the line, parsing into an integer
 *   as it goes. The integer representation is 10x the actual value
 *   but is used because integer parsing was found to be much faster
 *   than floating point parsing, and it's also immune to floating
 *   point arithmetic errors when aggregating the measurements later.
 * 
 * - Once the name and the measurement is parsed for a line, the name
 *   is hashed and used a lookup into a hash table. The value of the
 *   hash table at the given slot is an index into another array, this
 *   time an array of SIMD vectors that represent that name as a series
 *   of vectors. The vectors are used to compare equality of the name of
 *   the source line with the name in the slot to confirm the slot is
 *   occupied by the same city name. The indirection of having a hash
 *   table storing lookups into another array of vectors is to allow
 *   the hash table slots to have a fixed size, while allowing the city
 *   names to be arbitrarily long. The hash table can then use open
 *   addressing to resolve collisions and remain efficient for lookups.
 * 
 * - After the range has been processed, the results are collected by
 *   iterating through the hash table and looking up the corresponding
 *   integer table for each slot then collecting the min, max, count
 *   and sum of the measurements for each city. Then the results are
 *   combined from all threads, using a treemap for sorting, and printed.
 */
public final class CalculateAverage_chrisbellew {
    public static final long FILE_SIZE = getFileSize();

    /**
     * The overlap is the number of bytes that is peeked into the next buffer
     * in order to find the end of the last newline in the current buffer.
     * Every buffer ignores the characters before the first newline character
     * and peeks into the next buffer to find the first newline character. This
     * way no data is lost even though the buffers are arbitrarily sliced.
     * 100 is the maximum length of a city name, 1 is the semicolon character,
     * 5 is the maximum length of a measurement, 1 is the newline character,
     * 8 is one extra vector length so that we don't overflow the buffer.
     * If we overlap to this length then we will always be able to complete the
     * last line in the buffer.
     */
    public static final int OVERLAP = 100 + 1 + 5 + 1 + 8;

    public static void main(String[] args) throws IOException {
        /**
         * The test cases use small test files. This causes issues because we
         * are trying to open the file at different locations on 16 threads.
         */
        final int NUM_THREADS = FILE_SIZE < 12_000_000_000L ? 1 : 16;

        /**
         * Experimentally optimal buffer size for iterating over each
         * memory mapped segment of the file.
         */
        final int BUFFER_SIZE = 1024 * 256;

        /**
         * Split the whole file into slices. One slice per thread.
         */
        var ranges = getThreadRanges(NUM_THREADS);

        var processors = new ThreadProcessor[NUM_THREADS];
        Thread[] threads = new Thread[NUM_THREADS];
        for (var i = 0; i < NUM_THREADS; i++) {
            processors[i] = new ThreadProcessor(ranges[i].start, ranges[i].end, BUFFER_SIZE);
            threads[i] = new Thread(processors[i]);
            threads[i].start();
        }

        var results = new TreeMap<String, CityResult>();
        for (int i = 0; i < NUM_THREADS; i++) {
            try {
                threads[i].join();
                processors[i].collectResults(results);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        printResults(results);
    }

    private static void printResults(TreeMap<String, CityResult> results) {
        var builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        for (var entry : results.entrySet()) {
            var city = entry.getKey();
            var result = entry.getValue();
            var average = ((float) Math.round((float) result.sum / (float) result.count)) / 10.0;
            var min = ((float) result.min) / 10.0;
            var max = ((float) result.max) / 10.0;

            if (first) {
                first = false;
            }
            else {
                builder.append(", ");
            }
            builder.append(city).append("=").append(min).append("/").append(average).append("/").append(max);
        }
        builder.append("}");
        System.out.println(builder.toString());
    }

    /**
     * Splits the measurements file into ranges for each thread, ensuring that the last
     * range ends at the end of the file.
     */
    public static final FileRange[] getThreadRanges(int threads) throws IOException {
        var chunkSize = FILE_SIZE / threads;
        var ranges = new FileRange[threads];
        for (var i = 0; i < threads; i++) {
            var start = i * chunkSize;
            var end = i == threads - 1 ? FILE_SIZE : (i + 1) * chunkSize;
            ranges[i] = new FileRange(start, end);
        }
        return ranges;
    }

    private static final long getFileSize() {
        try (var stream = new FileInputStream("measurements.txt")) {
            return stream.getChannel().size();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to get file size", e);
        }
    }

    /**
     * Processes a range of the file. The range is defined by a start and end
     * position. The start is inclusive and the end is exclusive.
     */
    static final class ThreadProcessor implements Runnable {
        /**
         * The number of slots in the hash table. This number was found to be the
         * minimum number to use in conjunction with the hashing function to
         * produce no collisions on the test data. The test data is a hint, but the
         * correctness of the implementation is not coupled to the test data because
         * the hash table is able to handle collisions in other arbitrary source data.
         */
        private static final int NUM_SLOTS = 12133;

        /**
         * The size of the SIMD vector to use when striding through the source data
         * in order to detect newlines, and when comparing equality of the source line
         * with a given slot in the hash table.
         */
        private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_64;

        /**
         * A precomputed lookup table of vector masks to use when comparing equality of
         * the source line and a given slot in the hash table. Each slot in the hash table
         * has a set of vectors associated with it. The source name is split into vectors
         * and each source vector is compared with the corresponding slot vector for equality.
         * Unless the length of the city name is a multiple of the vector length, the last
         * vector in the slot will be a partial vector. The masks are used to ignore the
         * unused bytes in the last vector.
         */
        private static final VectorMask<Byte>[] MASKS = generateMasks(SPECIES);

        /**
         * The unsafe instance is used to allocate memory for the hash table slots
         * and integer table slots. It skips the JVM's garbage collector and allows
         * the memory to be accessed directly without overhead such as bounds checks.
         */
        private static final Unsafe unsafe = getUnsafe();

        /**
         * The start and end positions this thread will iterate through.
         */
        private final long start;
        private final long end;

        private final int bufferSize;

        /**
         * The main memory address at the beginning of the hash table slots.
         */
        private final long slotsAddress;

        /**
         * The main memory address at the beginning of the integer table slots.
         */
        private final long numbersAddress;

        /**
         * The main memory address at the beginning of the name length table slots.
         */
        private final long lengthsAddress;

        /**
         * The SIMD vectors associated with each slot in the hash table. The
         * content of a given slot in a hash table is a lookup into this array.
         * The intent of having this array as an extra lookup is to allow N
         * vectors per slot while having fixed size slots.
         */
        private ByteVector[] vectors = new ByteVector[200000];
        private String[] cityNames = new String[NUM_SLOTS];

        /**
         * The next available index in the vectors array.
         */
        private short nextVectorIndex = 8;

        /**
         * A map of city name strings to their corresponding slot index in the
         * hash table. When the hash table slots will be sparsely populated it's
         * not efficient to iterate through the slots when collecting the results.
         * This map provides a way to discover the occupied slots.
         */
        private final HashMap<String, Integer> cityVectorLookup = new HashMap<>();

        public ThreadProcessor(long start, long end, int bufferSize) {
            this.start = start;
            this.end = end;
            this.bufferSize = bufferSize;

            /**
             * Allocate memory for the hash table and the integer table.
             * Initialise the hash table slots to 0, so we can use 0 to
             * indicate an empty slot.
             */
            slotsAddress = unsafe.allocateMemory(NUM_SLOTS * 2);
            for (int i = 0; i < NUM_SLOTS; i++) {
                unsafe.putShort(slotsAddress + i * 2, (short) 0);
            }
            numbersAddress = unsafe.allocateMemory(NUM_SLOTS * 16);
            lengthsAddress = unsafe.allocateMemory(NUM_SLOTS);
        }

        public final void run() {
            try (RandomAccessFile file = new RandomAccessFile("measurements.txt", "r")) {
                FileChannel fileChannel = file.getChannel();

                /**
                 * Work out whether we need to peek into the next range. If this is the last
                 * range then the end of this range will be the end of the file, so we won't
                 * peek. Otherwise, we'll peek just enough into the next slot to complete the
                 * last line in this range.
                 */
                boolean lastRange = end == FILE_SIZE;
                long length = lastRange ? end - start : end - start + OVERLAP;

                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, length);
                processRange(buffer, lastRange);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Iterates through the entire memory mapped range, one buffer at a time.
         * The buffers are made to overlap to allow each buffer to peek into the next
         * range to complete the last line. 
         */
        private final void processRange(MappedByteBuffer buffer, boolean lastRange) {
            byte[] buf = new byte[bufferSize];
            int remaining;
            long globalPosition = start;
            while ((remaining = buffer.remaining()) != 0) {
                int numBytes = Math.min(remaining, bufferSize);
                boolean lastBuffer = remaining == numBytes;

                /**
                 * Fill this buffer and process it.
                 */
                buffer.get(buf, 0, numBytes);
                processBuffer(buf, numBytes, lastRange, lastBuffer, globalPosition);

                /**
                 * Start the next range slightly before the end of this range.
                 */
                if (!lastBuffer) {
                    buffer.position(buffer.position() - OVERLAP);
                }

                globalPosition += numBytes;
            }
        }

        /**
         * Parses and processes each line from a buffer.
         */
        private final void processBuffer(byte[] buffer, int numBytes, boolean lastRange, boolean lastBuffer, long globalPosition) {

            /**
             * Skip past any characters before the first newline because the previous
             * segment will have already processed them. That is unless this if the 
             * first buffer in the first range (global position zero), in which case
             * we will start from the first character.
             */
            int index = globalPosition == 0 ? 0 : findFirstNewline(buffer) + 1;

            /**
             * Keep track of the start of the city name.
             */
            int nameStart = index;

            while (true) {
                /**
                 * Take a slice of bytes and convert it into a vector so we can apply
                 * SIMD operations to find newlines.
                 */
                ByteVector vector = ByteVector.fromArray(SPECIES, buffer, index);

                /**
                 * Find the newline using SIMD.
                 */
                VectorMask<Byte> newLineMask = vector.eq((byte) '\n');
                int firstTrue = newLineMask.firstTrue();
                if (firstTrue == SPECIES.length()) {
                    /**
                     * We haven't found a newline in this vector, so move on to the
                     * next vector.
                     */
                    index += SPECIES.length();
                    continue;
                }

                slice(buffer, index + firstTrue, nameStart);

                index = index + firstTrue + 1;
                nameStart = index;

                /**
                 * If this is the last buffer in the last range then we want to 
                 * process every character until the very end of the file.
                 */
                if (lastRange && lastBuffer) {
                    if (index == numBytes) {
                        return;
                    }

                    /**
                     * If we're less than one vector length away from the end
                     * of the buffer then just take the remaining bytes as the
                     * final line. If we tried to use a vector it would overflow.
                     */
                    if (index >= numBytes - SPECIES.length()) {
                        slice(buffer, numBytes - 1, nameStart);
                        return;
                    }
                    continue;
                }

                /**
                 * If it's not the last buffer or it's not the last range then
                 * we want to overlap into the next buffer, but only by enough
                 * to complete the last line.
                 */
                if (index > numBytes - OVERLAP) {
                    return;
                }
            }
        }

        /**
         * Finds the first newline in a buffer using SIMD. Used to skip past a
         * partial line at the beginning of a buffer.
         */
        private final int findFirstNewline(byte[] buffer) {
            int index = 0;
            while (true) {
                ByteVector vector = ByteVector.fromArray(SPECIES, buffer, index);
                VectorMask<Byte> newLineMask = vector.eq((byte) '\n');
                int firstTrue = newLineMask.firstTrue();
                if (firstTrue == SPECIES.length()) {
                    index += SPECIES.length();
                    continue;
                }
                return index + firstTrue;
            }
        }

        /**
         * Given the index in the buffer of where a name starts, and the index of
         * the next newline, creeps back from the next newline to find the structure
         * of the measurement, parsing it into a number as it goes. It is parsed 
         * into an integer because it's faster than parsing as a float, and it's also
         * immune to floating point arithmetic errors when aggregating the measurements
         * later.
         * 
         * Then proceeds to record the fully parsed name and measurement in the hash table.
         */
        private final void slice(byte[] buffer, int newlineIndex, int nameStart) {
            int i = newlineIndex - 1;
            int measurement = buffer[i] - '0';
            i -= 2; // Skip before the decimal point
            measurement += (buffer[i] - '0') * 10;
            i--;

            if (buffer[i] == ';') {
                // 1.2
                record(buffer, nameStart, i, measurement);
            }
            else {
                // 12.3 or -1.2 or -12.3
                if (buffer[i] == '-') {
                    // -1.2
                    record(buffer, nameStart, i - 1, -measurement);
                }
                else {
                    // 12.3 or -12.3
                    measurement += (buffer[i] - '0') * 100;
                    i--;
                    if (buffer[i] == '-') {
                        // -12.3
                        record(buffer, nameStart, i - 1, -measurement);
                    }
                    else {
                        // 12.3
                        record(buffer, nameStart, i, measurement);
                    }
                }
            }
        }

        /**
         * Given a name and measurement, looks up a slot in the hash table by hashing
         * the city name as a key, then applies the measurement to the accumulated
         * aggregation of that city's measurements.
         */
        private final void record(byte[] buffer, int nameStart, int nameEnd, int measurement) {
            int nameLength = nameEnd - nameStart;

            /**
             * The length of most city names will not be a multiple of the SIMD
             * vector length so there will be a remainder in the final vector
             * of extraneous bytes. We need to mask these bytes out when comparing.
             */
            var remainder = nameLength % SPECIES.length();

            var numVectors = nameLength / SPECIES.length() + (remainder == 0 ? 0 : 1);

            /**
             * Lookup the slot index in the hash table for the city name.
             */
            var slotIndex = nameToSlotIndex(buffer, nameStart, nameLength);

            /**
             * Identify if the slot is occupied, then check the equality of the
             * slot with the city name.
             */
            var vectorOffset = unsafe.getShort(slotsAddress + slotIndex * 2);
            while (vectorOffset != 0) {

                /**
                 * Check the set of vectors in the slot match the city name
                 */
                if (slotEquals(buffer, nameStart, vectorOffset, numVectors, remainder, slotIndex)) {

                    /**
                     * Check the length of the slot name and city name match. This
                     * check is needed because the vector equality check can give
                     * false positives if one city name starts with another.
                     */
                    byte slotNameLength = unsafe.getByte(lengthsAddress + slotIndex);
                    if (slotNameLength == nameLength) {
                        updateSlot(slotIndex, measurement);
                        break;
                    }
                }

                /**
                 * If the slot is occupied but the city name doesn't match, then
                 * we try the next slot in the hash table through linear probing.
                 */
                slotIndex = (slotIndex + 1) % NUM_SLOTS;
                vectorOffset = unsafe.getShort(slotsAddress + slotIndex * 2);
            }

            /**
             * If the slot was unoccupied, then we can initialise it with the
             * city name and measurement.
             */
            if (vectorOffset == 0) {
                /**
                 * Record where the city name length is recorded for this slot.
                 */
                unsafe.putByte(lengthsAddress + slotIndex, (byte) nameLength);

                /**
                 * Record where the start of the set of vectors are recorded for
                 */
                unsafe.putShort(slotsAddress + slotIndex * 2, nextVectorIndex);

                /**
                 * Records the vectors for the city name.
                 */
                for (int v = 0; v < numVectors; v++) {
                    vectors[nextVectorIndex] = ByteVector.fromArray(SPECIES, buffer, nameStart + v * SPECIES.length());
                    nextVectorIndex++;
                }

                cityVectorLookup.put(new String(buffer, nameStart, nameLength), slotIndex);

                /**
                 * Min, max, count, sum
                 */
                var numbersIndex = getNumbersIndex(slotIndex);
                unsafe.putInt(numbersIndex, measurement);
                unsafe.putInt(numbersIndex + 4, measurement);
                unsafe.putInt(numbersIndex + 8, 1);
                unsafe.putInt(numbersIndex + 12, measurement);

                cityNames[slotIndex] = new String(buffer, nameStart, nameLength);
            }
        }

        /**
         * Given the index bounds of a name in a buffer, creates a hash of the name
         * by multiplying the first twelve characters. This was experimentally found
         * to provide a good distribution of hash values for the test data. In
         * combination with the number of slots in the hash table, this produces no
         * collisions on the test data. The test data is a hint, but the correctness
         * of the implementation is not coupled to the test data because the hash
         * table is able to handle collisions in other arbitrary source data.
         */
        private final int nameToSlotIndex(byte[] buffer, int nameStart, int nameLength) {
            var integer = 1;
            integer *= buffer[nameStart + 0];
            if (nameLength > 1) {
                integer *= buffer[nameStart + 1];
                if (nameLength > 2) {
                    integer *= buffer[nameStart + 2];
                    if (nameLength > 3) {
                        integer *= buffer[nameStart + 3];
                        if (nameLength > 4) {
                            integer *= buffer[nameStart + 4];
                            if (nameLength > 5) {
                                integer *= buffer[nameStart + 5];
                                if (nameLength > 6) {
                                    integer *= buffer[nameStart + 6];
                                    if (nameLength > 7) {
                                        integer *= buffer[nameStart + 7];
                                        if (nameLength > 8) {
                                            integer *= buffer[nameStart + 8];
                                            if (nameLength > 9) {
                                                integer *= buffer[nameStart + 9];
                                                if (nameLength > 10) {
                                                    integer *= buffer[nameStart + 10];
                                                    if (nameLength > 11) {
                                                        integer *= buffer[nameStart + 11];
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Math.abs(integer) % NUM_SLOTS;
        }

        /**
         * Given a slot index and a measurement, updates the aggregation of the
         * measurements for the city in that slot.
         */
        private final void updateSlot(int slotIndex, int measurement) {
            var numbersIndex = getNumbersIndex(slotIndex);
            var min = unsafe.getInt(numbersIndex);
            var max = unsafe.getInt(numbersIndex + 4);
            var count = unsafe.getInt(numbersIndex + 8);
            var sum = unsafe.getInt(numbersIndex + 12);

            unsafe.putInt(numbersIndex, Math.min(min, measurement));
            unsafe.putInt(numbersIndex + 4, Math.max(max, measurement));
            unsafe.putInt(numbersIndex + 8, count + 1);
            unsafe.putInt(numbersIndex + 12, sum + measurement);
        }

        /**
         * Given a name in a buffer, a slot index, and a number of vectors, checks
         * the equality of the name and the slot.
         * 
         * The length of the name is not necessarily a multiple of the SIMD vector
         * length, so the last vector in the slot will be a partial vector. The
         * masks are used to ignore the unused bytes in the last vector.
         */
        private final boolean slotEquals(byte[] buffer, int nameStart, int vectorOffset, int numVectors, int remainder, int slotIndex) {
            for (int v = 0; v < numVectors; v++) {
                var nameVector = ByteVector.fromArray(SPECIES, buffer, nameStart + v * SPECIES.length());
                var slotVector = vectors[vectorOffset + v];
                if (v == numVectors - 1) {
                    if (remainder == 0) {
                        if (!slotVector.eq(nameVector).allTrue()) {
                            return false;
                        }
                    }
                    else {
                        var mask = MASKS[remainder - 1];
                        if (!slotVector.compare(VectorOperators.EQ, nameVector, mask).equals(mask)) {
                            return false;
                        }
                    }
                    break;
                }
                else {
                    if (!slotVector.eq(nameVector).allTrue()) {
                        return false;
                    }
                }
            }

            return true;
        }

        /**
         * Given a slot index, returns the main memory address of the integer table
         * where the min, max, count and sum of the measurements are stored.
         */
        private final long getNumbersIndex(int slotIndex) {
            return numbersAddress + slotIndex * 16;
        }

        public void collectResults(TreeMap<String, CityResult> results) {
            for (var entry : cityVectorLookup.entrySet()) {
                var city = entry.getKey();
                var slotIndex = entry.getValue();
                var numbersIndex = getNumbersIndex(slotIndex);
                var min = unsafe.getInt(numbersIndex);
                var max = unsafe.getInt(numbersIndex + 4);
                var count = unsafe.getInt(numbersIndex + 8);
                var sum = unsafe.getInt(numbersIndex + 12);
                results.compute(city, (k, v) -> {
                    if (v == null) {
                        return new CityResult(min, max, sum, count);
                    }
                    else {
                        v.min = Math.min(v.min, min);
                        v.max = Math.max(v.max, max);
                        v.sum += sum;
                        v.count += count;
                        return v;
                    }
                });
            }
        }

        /**
         * Generates a lookup table of vector masks to use when comparing equality of
         * the last vector of the source line and a given slot in the hash table.
         */
        private static final VectorMask<Byte>[] generateMasks(VectorSpecies<Byte> species) {
            VectorMask<Byte>[] masks = new VectorMask[species.length() - 1];
            masks[0] = VectorMask.fromArray(species, new boolean[]{ true, false, false, false, false, false, false, false }, 0);
            masks[1] = VectorMask.fromArray(species, new boolean[]{ true, true, false, false, false, false, false, false }, 0);
            masks[2] = VectorMask.fromArray(species, new boolean[]{ true, true, true, false, false, false, false, false }, 0);
            masks[3] = VectorMask.fromArray(species, new boolean[]{ true, true, true, true, false, false, false, false }, 0);
            masks[4] = VectorMask.fromArray(species, new boolean[]{ true, true, true, true, true, false, false, false }, 0);
            masks[5] = VectorMask.fromArray(species, new boolean[]{ true, true, true, true, true, true, false, false }, 0);
            masks[6] = VectorMask.fromArray(species, new boolean[]{ true, true, true, true, true, true, true, false }, 0);
            return masks;
        }

        private static final Unsafe getUnsafe() {
            Field field;
            try {
                field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                return (Unsafe) field.get(null);
            }
            catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                throw new RuntimeException("Failed to get unsafe", e);
            }
        }
    }

    static final class CityResult {
        public int min;
        public int max;
        public int sum;
        public int count;

        public CityResult(int min, int max, int sum, int count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }
    }

    static final class FileRange {
        public final long start;
        public final long end;

        public FileRange(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}