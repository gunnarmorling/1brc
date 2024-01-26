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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class CalculateAverage_jonathanaotearoa {

    public static final Unsafe UNSAFE;

    static {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Error getting instance of %s".formatted(Unsafe.class.getName()));
        }
    }

    private static final Path FILE_PATH = Path.of("./measurements.txt");
    private static final byte MAX_LINE_BYTES = 107;
    private static final byte NEW_LINE_BYTE = '\n';
    private static final long SEPARATOR_XOR_MASK = 0x3b3b3b3b3b3b3b3bL;

    // A mask where the 4th bit of the 5th, 6th and 7th bytes is set to 1.
    // Leverages the fact that the 4th bit of a digit byte will 1.
    // Whereas the 4th bit of the decimal point byte will be 0.
    // Assumes little endianness.
    private static final long DECIMAL_POINT_MASK = 0x10101000L;

    // This mask performs two tasks:
    // Sets the right-most and 3 left-most bytes to zero.
    // Given a temp value be at most 5 bytes in length, .e.g -99.9, we can safely ignore the last 3 bytes.
    // Subtracts 48, i.e. the UFT-8 value offset, from the digits bytes.
    // As a result, '0' (48) becomes 0, '1' (49) becomes 1, and so on.
    private static final long TEMP_DIGITS_MASK = 0x0f000f0f00L;

    public static void main(final String[] args) throws IOException {
        try (final FileChannel fc = FileChannel.open(FILE_PATH, StandardOpenOption.READ)) {
            final long fileSize = fc.size();
            try (final Arena arena = Arena.ofConfined()) {
                final long fileAddress = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena).address();
                final TreeMap<String, StationData> result = createChunks(fileAddress, fileSize)
                        .parallel()
                        .map(CalculateAverage_jonathanaotearoa::processChunk)
                        .flatMap(Repository::entries)
                        .collect(Collectors.toMap(
                                sd -> sd.name,
                                sd -> sd,
                                StationData::merge,
                                TreeMap::new));
                System.out.println(result);
            }
        }
    }

    private static Stream<Chunk> createChunks(final long fileAddress, final long fileSize) {
        final long[] chunkAddresses = getChunkAddresses(fileAddress, fileSize);
        final long lastByteAddress = fileAddress + fileSize - 1;
        return IntStream.range(0, chunkAddresses.length)
                .mapToObj(chunkIndex -> Chunk.fromAddress(chunkAddresses, chunkIndex, lastByteAddress));
    }

    private static long[] getChunkAddresses(final long fileAddress, final long fileSize) {
        // Should be the number of cores - 1.
        final int parallelism = ForkJoinPool.getCommonPoolParallelism();
        if ((long) parallelism * MAX_LINE_BYTES > fileSize) {
            // There might not be enough lines in the file. Return a single chunk.
            return new long[]{ fileAddress };
        }
        final long[] addresses = new long[parallelism];
        final long chunkSize = fileSize / parallelism;
        addresses[0] = fileAddress;
        for (int i = 1; i < addresses.length; i++) {
            long chunkAddress = fileAddress + (i * chunkSize);
            // Find the end of the previous line.
            while (UNSAFE.getByte(chunkAddress) != NEW_LINE_BYTE) {
                chunkAddress--;
            }
            addresses[i] = chunkAddress + 1;
        }
        return addresses;
    }

    private static Repository processChunk(final Chunk chunk) {
        final Repository repo = new Repository();
        long address = chunk.startAddress;

        while (address <= chunk.lastByteAddress) {
            // Read station name.
            long nameAddress = address;
            long nameWord;
            long separatorMask;
            int nameHash = 1;

            while (true) {
                // We don't care about byte order when constructing a hash.
                // There's no need, therefore, to reverse the bytes if the OS is little endian.
                nameWord = chunk.getWord(address);

                // Based on the Hacker's Delight "Find First 0-Byte" branch-free, 5-instruction, algorithm.
                // See also https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
                final long separatorXorResult = nameWord ^ SEPARATOR_XOR_MASK;
                // If the separator is not present, all bits in the mask will be zero.
                // If the separator is present, the first bit of the corresponding byte in the mask will be 1.
                separatorMask = (separatorXorResult - 0x0101010101010101L) & (~separatorXorResult & 0x8080808080808080L);
                if (separatorMask == 0) {
                    address += Long.BYTES;
                    // We could use XOR here, but it "might" produce more collisions.
                    nameHash = 31 * nameHash + Long.hashCode(nameWord);
                }
                else {
                    break;
                }
            }

            // We've found the separator.
            // The following operations assume little endianness, i.e. the UTF-8 bytes are ordered from right to left.
            // We therefore use the *trailing* number of zeros to get the number of name bits.
            final int numberOfNameBits = Long.numberOfTrailingZeros(separatorMask) & ~7;
            final int numberOfNameBytes = numberOfNameBits >> 3;
            final long separatorAddress = address + numberOfNameBytes;

            if (numberOfNameBytes > 0) {
                // Truncate the word, so we only have the portion before the separator, i.e. the name bytes.
                // As per the separator index step above, this assumes little endianness.
                final int bitsToDiscard = Long.SIZE - numberOfNameBits;
                final long truncatedNameWord = (nameWord << bitsToDiscard) >>> bitsToDiscard;
                // Multiplicative hashing, as per Arrays.hashCode().
                nameHash = 31 * nameHash + Long.hashCode(truncatedNameWord);
            }

            final long tempAddress = separatorAddress + 1;
            final long tempWord = chunk.getWord(tempAddress);

            // Get the position of the decimal point...
            // "." in UTF-8 is 46, which is 00101110 in binary.
            // We can therefore use the 4th bit to check which byte is the decimal point.
            // Given we're assuming little endianness, the decimal point index is from right to left.
            final int decimalPointIndex = Long.numberOfTrailingZeros(~tempWord & DECIMAL_POINT_MASK) >> 3;

            // Check if we've got a negative or positive number...
            // "0" in UTF-8 is 48, which is 00110000 in binary.
            // The first 4 bits of any UTF-8 digit byte are therefore 0011.
            // "-" in UTF-8 is 45, which is 00101101 in binary.
            // We can, therefore, use the 4th bit to check if the word contains a positive, or negative, temperature.
            // If the temperature is negative, the value of "sign" will be -1. If it's positive, it'll be 0.
            final long sign = (~tempWord << 59) >> 63;

            // Create a mask that zeros out the minus-sign byte, if present.
            // Assumes little endianness, i.e. the minus sign is the right-most byte.
            final long signMask = ~(sign & 0xFF);

            // To get the temperature value, we left-shift the digit bytes into the following, known, positions.
            // 0x00 0x00 0x00 <fractional-digit> 0x00 <integer-part-digit> <integer-part-digit> 0x00
            // Because we're ANDing with the sign mask, if the value only has a single integer-part digit, the right-most one will be zero.
            final int leftShift = (3 - decimalPointIndex) * Byte.SIZE;
            final long digitsWord = ((tempWord & signMask) << leftShift) & TEMP_DIGITS_MASK;

            // Get the unsigned int value.
            final byte b100 = (byte) (digitsWord >> 8);
            final byte b10 = (byte) (digitsWord >> 16);
            final byte b1 = (byte) (digitsWord >> 32);
            final int unsignedTemp = b100 * 100 + b10 * 10 + b1;

            final int temp = (int) ((unsignedTemp + sign) ^ sign);

            final byte nameSize = (byte) (separatorAddress - nameAddress);
            repo.put(nameAddress, nameSize, nameHash, temp);

            // Calculate the address of the next line.
            address = tempAddress + decimalPointIndex + 3;
        }

        return repo;
    }

    private record Chunk(long startAddress, long lastByteAddress, long lastWordAddress, boolean isLast) {

        public static Chunk fromAddress(final long[] chunkAddresses,
                                        final int chunkIndex,
                                        final long lastFileByteAddress) {
            final long startAddress = chunkAddresses[chunkIndex];
            final boolean isLast = chunkIndex == chunkAddresses.length - 1;
            final long lastByteAddress = isLast ? lastFileByteAddress : chunkAddresses[chunkIndex + 1] - 1;
            final long lastWordAddress = lastByteAddress - (Long.BYTES - 1);
            return new Chunk(startAddress, lastByteAddress, lastWordAddress, isLast);
        }

        public Long getWord(final long address) {
            if (isLast && address > lastWordAddress) {
                // Make sure we don't read beyond the end of the file and potentially crash the JVM.
                final long word = UNSAFE.getLong(lastWordAddress);
                final int bytesToDiscard = (int) (address - lastWordAddress);
                // As with elsewhere, this assumes little endianness.
                return word >>> (bytesToDiscard << 3);
            }
            return UNSAFE.getLong(address);
        }
    }

    private static class StationData implements Comparable<StationData> {

        private final String name;
        public final int nameHash;
        private int tempMin;
        private int tempMax;
        private long tempSum;
        private int count;

        public StationData(final String name, final int nameHash, final int temp) {
            this.name = name;
            this.nameHash = nameHash;
            tempMin = tempMax = temp;
            tempSum = temp;
            count = 1;
        }

        public void addTemp(final int temp) {
            tempMin = min(temp, tempMin);
            tempMax = max(temp, tempMax);
            tempSum += temp;
            count++;
        }

        public StationData merge(final StationData other) {
            tempMin = min(tempMin, other.tempMin);
            tempMax = max(tempMax, other.tempMax);
            tempSum += other.tempSum;
            count += other.count;
            return this;
        }

        @Override
        public int compareTo(final StationData other) {
            return name.compareTo(other.name);
        }

        @Override
        public String toString() {
            // Only include the temp information as we're relying on TreeMap's toString for the rest.
            return round(tempMin) + "/" + round((1.0 * tempSum) / count) + "/" + round(tempMax);
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    /**
     * Open addressing, linear probing, hash map repository.
     */
    private static class Repository {

        // First prime number greater than unique keys * 10.
        private static final int CAPACITY = 100_003;

        private final StationData[] table;

        public Repository() {
            this.table = new StationData[CAPACITY];
        }

        public void put(final long nameAddress, final byte nameSize, final int nameHash, int temp) {
            final int index = findIndex(nameHash);
            if (table[index] == null) {
                final byte[] nameBytes = new byte[nameSize];
                UNSAFE.copyMemory(null, nameAddress, nameBytes, UNSAFE.arrayBaseOffset(nameBytes.getClass()), nameSize);
                final String name = new String(nameBytes, StandardCharsets.UTF_8);
                table[index] = new StationData(name, nameHash, temp);
            }
            else {
                table[index].addTemp(temp);
            }
        }

        public Stream<StationData> entries() {
            return Arrays.stream(table).filter(Objects::nonNull);
        }

        private int findIndex(int nameHash) {
            int i = (nameHash & 0x7FFFFFFF) % table.length;
            while (table[i] != null && table[i].nameHash != nameHash) {
                i = (i + 1) % table.length;
            }
            return i;
        }
    }
}
