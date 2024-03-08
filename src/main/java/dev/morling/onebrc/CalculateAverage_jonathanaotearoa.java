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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_jonathanaotearoa {

    public static final Unsafe UNSAFE;

    static {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(STR."Error getting instance of \{Unsafe.class.getName()}");
        }
    }

    private static final int WORD_BYTES = Long.BYTES;
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
        assert ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN : "Big endian byte order is not supported";
        System.out.println(resultsToString(processFile(FILE_PATH)));
    }

    /**
     * A custom version of AbstractMap's toString() method.
     * <p>
     * This should be more performant as we can:
     * <ul>
     *     <li>Set the initial capacity of the string builder</li>
     *     <li>Append double values directly, which avoids string creation</li>
     * </ul>
     * </p>
     *
     * @param results the results.
     * @return a string representation of the results.
     */
    private static String resultsToString(final Map<String, TemperatureData> results) {
        final Iterator<Map.Entry<String, TemperatureData>> i = results.entrySet().iterator();
        if (!i.hasNext()) {
            System.out.println("{}");
        }
        // Capacity based the output for measurements.txt.
        final StringBuilder sb = new StringBuilder(1100).append('{');
        while (i.hasNext()) {
            Map.Entry<String, TemperatureData> e = i.next();
            sb.append(e.getKey())
                    .append('=')
                    .append(e.getValue().getMin())
                    .append('/')
                    .append(e.getValue().getMean())
                    .append('/')
                    .append(e.getValue().getMax());
            if (i.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        sb.append('}');
        return sb.toString();
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
    private static SortedMap<String, TemperatureData> processFile(final Path filePath) throws IOException {
        assert filePath != null : "filePath cannot be null";
        assert Files.isRegularFile(filePath) : STR."\{filePath.toAbsolutePath()} is not a valid file";

        try (final FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            final long fileSize = fc.size();
            if (fileSize < WORD_BYTES) {
                // The file size is less than our word size.
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
     * @param fc       the file channel to read from.
     * @param fileSize the file size in bytes.
     * @return a sorted map of station data keyed by station name.
     * @throws IOException if an error occurs reading from the file channel.
     */
    private static SortedMap<String, TemperatureData> processTinyFile(final FileChannel fc, final long fileSize) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate((int) fileSize);
        fc.read(byteBuffer);
        return new String(byteBuffer.array(), StandardCharsets.UTF_8)
                .lines()
                .map(line -> line.trim().split(";"))
                .map(tokens -> {
                    final String stationName = tokens[0];
                    final short temp = Short.parseShort(tokens[1].replace(".", ""));
                    return new SimpleStationData(stationName, temp);
                })
                .collect(Collectors.toMap(
                        sd -> sd.name,
                        sd -> sd,
                        TemperatureData::merge,
                        TreeMap::new));
    }

    /**
     * An optimised method for processing files > {@link Long#BYTES} in size.
     *
     * @param fc       the file channel to map into memory.
     * @param fileSize the file size in bytes.
     * @return a sorted map of station data keyed by station name.
     * @throws IOException if an error occurs mapping the file channel into memory.
     */
    private static SortedMap<String, TemperatureData> processFile(final FileChannel fc, final long fileSize) throws IOException {
        assert fileSize >= WORD_BYTES : STR."File size cannot be less than word size \{WORD_BYTES}, but was \{fileSize}";

        try (final Arena arena = Arena.ofConfined()) {
            final long fileAddress = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena).address();
            return createChunks(fileAddress, fileSize)
                    .parallel()
                    .map(CalculateAverage_jonathanaotearoa::processChunk)
                    .flatMap(Repository::entries)
                    .collect(Collectors.toMap(
                            StationData::getName,
                            sd -> sd,
                            TemperatureData::merge,
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
     * @param fileSize    the size of the file in bytes.
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
                nameWord = chunk.getWord(address);

                // Based on the Hacker's Delight "Find First 0-Byte" branch-free, 5-instruction, algorithm.
                // See also https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
                final long separatorXorResult = nameWord ^ SEPARATOR_XOR_MASK;
                // If the separator is not present, all bits in the mask will be zero.
                // If the separator is present, the first bit of the corresponding byte in the mask will be 1.
                separatorMask = (separatorXorResult - 0x0101010101010101L) & (~separatorXorResult & 0x8080808080808080L);
                if (separatorMask == 0) {
                    address += Long.BYTES;
                    // Multiplicative hashing, as per Arrays.hashCode().
                    // We could use XOR here, but it "might" produce more collisions.
                    nameHash = 31 * nameHash + Long.hashCode(nameWord);
                }
                else {
                    break;
                }
            }

            // We've found the separator.
            // We only support little endian, so we use the *trailing* number of zeros to get the number of name bits.
            final int numberOfNameBits = Long.numberOfTrailingZeros(separatorMask) & ~7;
            final int numberOfNameBytes = numberOfNameBits >> 3;
            final long separatorAddress = address + numberOfNameBytes;

            if (numberOfNameBytes > 0) {
                // Truncate the word, so we only have the portion before the separator, i.e. the name bytes.
                final int bitsToDiscard = Long.SIZE - numberOfNameBits;
                // Little endian.
                final long truncatedNameWord = (nameWord << bitsToDiscard) >>> bitsToDiscard;
                nameHash = 31 * nameHash + Long.hashCode(truncatedNameWord);
            }

            final long tempAddress = separatorAddress + 1;
            final long tempWord = chunk.getWord(tempAddress);

            // "0" in UTF-8 is 48, which is 00110000 in binary.
            // The first 4 bits of any UTF-8 digit byte are therefore 0011.

            // Get the position of the decimal point...
            // "." in UTF-8 is 46, which is 00101110 in binary.
            // We can therefore use the 4th bit to check which byte is the decimal point.
            final int decimalPointIndex = Long.numberOfTrailingZeros(~tempWord & DECIMAL_POINT_MASK) >> 3;

            // Check if we've got a negative or positive number...
            // "-" in UTF-8 is 45, which is 00101101 in binary.
            // As per above, we use the 4th bit to check if the word contains a positive, or negative, temperature.
            // If the temperature is negative, the value of "sign" will be -1. If it's positive, it'll be 0.
            final long sign = (~tempWord << 59) >> 63;

            // Create a mask that zeros out the minus-sign byte, if present.
            // Little endian, i.e. the minus sign is the right-most byte.
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
            final short unsignedTemp = (short) (b100 * 100 + b10 * 10 + b1);
            final short temp = (short) ((unsignedTemp + sign) ^ sign);

            final byte nameSize = (byte) (separatorAddress - nameAddress);
            repo.addTemp(nameHash, nameAddress, nameSize, temp);

            // Calculate the address of the next line.
            address = tempAddress + decimalPointIndex + 3;
        }

        return repo;
    }

    /**
     * Represents a portion of a file containing 1 or more whole lines.
     *
     * @param startAddress    the memory address of the first byte.
     * @param lastByteAddress the memory address of the last byte.
     * @param lastWordAddress the memory address of the last whole word.
     * @param isLast          whether this is the last chunk.
     */
    private record Chunk(long startAddress, long lastByteAddress, long lastWordAddress, boolean isLast) {

        public Chunk(final long startAddress, final long lastByteAddress, final boolean isLast) {
            this(startAddress, lastByteAddress, lastByteAddress - (Long.BYTES - 1), isLast);

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

    /**
     * Abstract class encapsulating temperature data.
     */
    private static abstract class TemperatureData {

        private short min;
        private short max;
        private long sum;
        private int count;

        protected TemperatureData(final short temp) {
            min = max = temp;
            sum = temp;
            count = 1;
        }

        void addTemp(final short temp) {
            if (temp < min) {
                min = temp;
            }
            else if (temp > max) {
                max = temp;
            }
            sum += temp;
            count++;
        }

        TemperatureData merge(final TemperatureData other) {
            if (other.min < min) {
                min = other.min;
            }
            if (other.max > max) {
                max = other.max;
            }
            sum += other.sum;
            count += other.count;
            return this;
        }

        double getMin() {
            return round(((double) min) / 10.0);
        }

        double getMax() {
            return round(((double) max) / 10.0);
        }

        double getMean() {
            return round((((double) sum) / 10.0) / count);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    /**
     * For use with tiny files.
     *
     * @see CalculateAverage_jonathanaotearoa#processTinyFile(FileChannel, long).
     */
    private static final class SimpleStationData extends TemperatureData implements Comparable<SimpleStationData> {

        private final String name;

        SimpleStationData(final String name, final short temp) {
            super(temp);
            this.name = name;
        }

        @Override
        public int compareTo(final SimpleStationData other) {
            return name.compareTo(other.name);
        }
    }

    private static final class StationData extends TemperatureData implements Comparable<StationData> {

        private final int nameHash;
        private final long nameAddress;
        private final byte nameSize;
        private String name;

        StationData(final int nameHash, final long nameAddress, final byte nameSize, final short temp) {
            super(temp);
            this.nameAddress = nameAddress;
            this.nameSize = nameSize;
            this.nameHash = nameHash;
        }

        @Override
        public int compareTo(final StationData other) {
            return getName().compareTo(other.getName());
        }

        String getName() {
            if (name == null) {
                final byte[] nameBytes = new byte[nameSize];
                UNSAFE.copyMemory(null, nameAddress, nameBytes, UNSAFE.arrayBaseOffset(nameBytes.getClass()), nameSize);
                name = new String(nameBytes, StandardCharsets.UTF_8);
            }
            return name;
        }
    }

    /**
     * Open addressing, linear probing, hash map repository.
     */
    private static final class Repository {

        private static final int CAPACITY = 100_003;
        private static final int LAST_INDEX = CAPACITY - 1;

        private final StationData[] table;

        public Repository() {
            this.table = new StationData[CAPACITY];
        }

        /**
         * Adds a station temperature value to this repository.
         *
         * @param nameHash    the station name hash.
         * @param nameAddress the station name address in memory.
         * @param nameSize    the station name size in bytes.
         * @param temp        the temperature value.
         */
        public void addTemp(final int nameHash, final long nameAddress, final byte nameSize, short temp) {
            final int index = findIndex(nameHash, nameAddress, nameSize);
            if (table[index] == null) {
                table[index] = new StationData(nameHash, nameAddress, nameSize, temp);
            }
            else {
                table[index].addTemp(temp);
            }
        }

        public Stream<StationData> entries() {
            return Arrays.stream(table).filter(Objects::nonNull);
        }

        private int findIndex(int nameHash, final long nameAddress, final byte nameSize) {
            // Think about replacing modulo.
            // https://lemire.me/blog/2018/08/20/performance-of-ranged-accesses-into-arrays-modulo-multiply-shift-and-masks/
            int index = (nameHash & 0x7FFFFFFF) % CAPACITY;
            while (isCollision(index, nameHash, nameAddress, nameSize)) {
                index = index == LAST_INDEX ? 0 : index + 1;
            }
            return index;
        }

        private boolean isCollision(final int index, final long nameHash, final long nameAddress, final byte nameSize) {
            final StationData existing = table[index];
            if (existing == null) {
                return false;
            }
            if (nameHash != existing.nameHash) {
                return true;
            }
            if (nameSize != existing.nameSize) {
                return true;
            }
            // Last resort; check if the names are the same.
            // This is real performance hit :(
            return !isMemoryEqual(nameAddress, existing.nameAddress, nameSize);
        }

        /**
         * Checks if two locations in memory have the same value.
         *
         * @param address1 the address of the first location.
         * @param address2 the address of the second locations.
         * @param size     the number of bytes to check for equality.
         * @return true if both addresses contain the same bytes.
         */
        private static boolean isMemoryEqual(final long address1, final long address2, final byte size) {
            // Checking 1 byte at a time, so we can bail as early as possible.
            for (int offset = 0; offset < size; offset++) {
                final byte b1 = UNSAFE.getByte(address1 + offset);
                final byte b2 = UNSAFE.getByte(address2 + offset);
                if (b1 != b2) {
                    return false;
                }
            }
            return true;
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
                        final SortedMap<String, TemperatureData> results = processFile(filePath);
                        // Appending \n to the results string to mimic println().
                        final String actual = STR."\{resultsToString(results)}\n";
                        if (actual.equals(expected)) {
                            testResults.append("Passed\n");
                        } else {
                            testResults.append("Failed. Actual output does not match expected\n");
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(STR."Error testing '\{filePath.getFileName()}");
                    }
                });
            } finally {
                System.out.println(testResults);
            }
        }
    }
}
