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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
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
            throw new RuntimeException(STR."Error getting instance of \{Unsafe.class.getName()}");
        }
    }

    private static final Path FILE_PATH = Path.of("./measurements.txt");
    private static final Path SAMPLE_DIR_PATH = Path.of("./src/test/resources/samples");
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
        System.out.println(processFile(FILE_PATH));
    }

    /**
     * Processes the specified file.
     * <p>
     * Extracted from the main method for testability.
     * </p>
     *
     * @param filePath the path of the file we want to process.
     * @return a sorted map of station data keyed by station name.
     * @throws IOException if an error occurs.
     */
    private static SortedMap<String, StationData> processFile(final Path filePath) throws IOException {
        assert filePath != null : "filePath cannot be null";
        assert Files.isRegularFile(filePath) : STR."\{filePath.toAbsolutePath()} is not a valid file";

        try (final FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            final long fileSize = fc.size();
            if (fileSize < Long.BYTES) {
                // The file size is less than our word size!
                // Keep it simple and fall back to non-performant processing.
                return processTinyFile(fc, fileSize);
            }
            return processFile(fc, fileSize);
        }
    }

    /**
     * An unoptimised method for processing a tiny file.
     * <p>
     * Handling tiny files in a separate method reduces the complexity of {@link #processFile(FileChannel, long)}.
     * </p>
     *
     * @param fc the file channel to read from.
     * @param fileSize the file size in bytes.
     * @return a sorted map of station data keyed by station name.
     * @throws IOException if an error occurs reading from the file channel.
     */
    private static SortedMap<String, StationData> processTinyFile(final FileChannel fc, final long fileSize) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate((int) fileSize);
        fc.read(byteBuffer);
        return new String(byteBuffer.array(), StandardCharsets.UTF_8)
                .lines()
                .map(line -> line.trim().split(";"))
                .map(tokens -> {
                    final String stationName = tokens[0];
                    final int temp = Integer.parseInt(tokens[1].replace(".", ""));
                    return new StationData(stationName, stationName.hashCode(), temp);
                })
                .collect(Collectors.toMap(
                        sd -> sd.name,
                        sd -> sd,
                        StationData::merge,
                        TreeMap::new));
    }

    /**
     * An optimised method for processing files > {@link Long#BYTES} in size.
     *
     * @param fc the file channel to map into memory.
     * @param fileSize the file size in bytes.
     * @return a sorted map of station data keyed by station name.
     * @throws IOException if an error occurs mapping the file channel into memory.
     */
    private static SortedMap<String, StationData> processFile(final FileChannel fc, final long fileSize) throws IOException {
        assert fileSize >= Long.BYTES : STR."File size must be >= \{Long.BYTES} but was \{fileSize}";

        try (final Arena arena = Arena.ofConfined()) {
            final long fileAddress = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena).address();
            return createChunks(fileAddress, fileSize)
                    .parallel()
                    .map(CalculateAverage_jonathanaotearoa::processChunk)
                    .flatMap(Repository::entries)
                    .collect(Collectors.toMap(
                            sd -> sd.name,
                            sd -> sd,
                            StationData::merge,
                            TreeMap::new));
        }
    }

    /**
     * Divides the file into chunks that can be processed in parallel.
     * <p>
     * If dividing the file into {@link ForkJoinPool#getCommonPoolParallelism() parallelism} chunks would result in a
     * chunk size less than the maximum line size in bytes, then a single chunk is returned for the entire file.
     * </p>
     *
     * @param fileAddress the address of the file.
     * @param fileSize the size of the file in bytes.
     * @return a stream of chunks.
     */
    private static Stream<Chunk> createChunks(final long fileAddress, final long fileSize) {
        // The number of cores - 1.
        final int parallelism = ForkJoinPool.getCommonPoolParallelism();
        final long chunkStep = fileSize / parallelism;
        final long lastFileByteAddress = fileAddress + fileSize - 1;
        if (chunkStep < MAX_LINE_BYTES) {
            // We're dealing with a small file, return a single chunk.
            return Stream.of(new Chunk(fileAddress, lastFileByteAddress, true));
        }
        final Chunk[] chunks = new Chunk[parallelism];
        long startAddress = fileAddress;
        for (int i = 0, n = parallelism - 1; i < n; i++) {
            // Find end of the *previous* line.
            // We know there's a previous line in this chunk because chunkStep >= MAX_LINE_BYTES.
            // The last chunk may be slightly bigger than the others.
            // For a 1 billion line file, this has zero impact.
            long lastByteAddress = startAddress + chunkStep;
            while (UNSAFE.getByte(lastByteAddress) != NEW_LINE_BYTE) {
                lastByteAddress--;
            }
            // We've found the end of the previous line.
            chunks[i] = new Chunk(startAddress, lastByteAddress, false);
            startAddress = ++lastByteAddress;
        }
        // The remaining bytes are assigned to the last chunk.
        chunks[chunks.length - 1] = (new Chunk(startAddress, lastFileByteAddress, true));
        return Stream.of(chunks);
    }

    /**
     * Does the work of processing a chunk.
     *
     * @param chunk the chunk to process.
     * @return a repository containing the chunk's station data.
     */
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

    /**
     * Represents a portion of a file containing 1 or more whole lines.
     *
     * @param startAddress the memory address of the first byte.
     * @param lastByteAddress the memory address of the last byte.
     * @param lastWordAddress the memory address of the last whole word.
     * @param isLast whether this is the last chunk.
     */
    private record Chunk(long startAddress, long lastByteAddress, long lastWordAddress, boolean isLast) {

        public Chunk(final long startAddress, final long lastByteAddress, final boolean isLast) {
            this(startAddress, lastByteAddress, lastByteAddress - (Long.BYTES - 1), isLast);
            // Enabled when running TestRunner.
            assert lastByteAddress > startAddress : STR."lastByteAddress \{lastByteAddress} must be > startAddress \{startAddress}";
            assert lastWordAddress >= startAddress : STR."lastWordAddress \{lastWordAddress} must be >= startAddress \{startAddress}";
        }

        /**
         * Gets an 8 byte word from this chunk.
         * <p>
         * If the specified address is greater than {@link Chunk#lastWordAddress} and {@link Chunk#isLast}, the word
         * will be truncated. This ensures we never read beyond the end of the file.
         * </p>
         *
         * @param address the address of the word we want.
         * @return the word at the specified address.
         */
        public long getWord(final long address) {
            assert address >= startAddress : STR."address must be >= startAddress \{startAddress}, but was \{address}";
            assert address < lastByteAddress : STR."address must be < lastByteAddress \{lastByteAddress}, but was \{address}";

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

    /**
     * Helper for running tests without blowing away the main measurements.txt file.
     * Saves regenerating the 1 billion line file after each test run.
     * Enable assertions in the IDE run config.
     */
    public static final class TestRunner {
        public static void main(String[] args) throws IOException {
            final StringBuilder testResults = new StringBuilder();
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(SAMPLE_DIR_PATH, "*.txt")) {
                dirStream.forEach(filePath -> {
                    testResults.append(STR."Testing '\{filePath.getFileName()}'... ");
                    final String expectedResultFileName = filePath.getFileName().toString().replace(".txt", ".out");
                    try {
                        final String expected = Files.readString(SAMPLE_DIR_PATH.resolve(expectedResultFileName));
                        final String actual = processFile(filePath).toString();
                        if (actual.equals(expected)) {
                            testResults.append("Passed\n");
                        }
                        else {
                            testResults.append("Failed. Actual output does not match expected\n");
                        }
                    }
                    catch (IOException e) {
                        throw new RuntimeException(STR."Error testing '\{filePath.getFileName()}");
                    }
                });
            }
            finally {
                System.out.println(testResults);
            }
        }
    }
}
