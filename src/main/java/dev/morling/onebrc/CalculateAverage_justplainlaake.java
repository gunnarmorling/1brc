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
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import sun.misc.Unsafe;

/*
    Possibilities to improve:
        * Reduce Standard Memory Reads and/or Swaps for threading 
            - For the read file; using Unsafe or MemorySegment to map the file to an existing register instead of keeping the bytes local
            - For normal variables; Most of the time reading a value performs a load from memory and registers it for faster lookups, but with multithreading causes each thread to re read and register each get [volatile] keyword
        * Add multithreading to process multiple segments at once (When you have 1,000,000,000 cars driving might as well open as many lanes as possible)
        * Improve Mapping of entries (More O(1) lookups the better, i.e. hashed key maps, preferebly open maps to skip needing linked lists or trees, also simplifies since we don't need to delete anything)
        * Remove use of java streams (They can be much slower than expected, good for developer readability but not for performance 90% of the time)
        * Reduce amount of bytecode instructions (Usually just a micro-optimization, but since we are reading 1,000,000,000 lines, then this is really helpful in the processing code)
        * Never use division in processing code, division is 2x+ slower than multiplication (Easy fix is multiplying by decimal 2/2 vs 2*0.5)

    My System:
        Device:
            Processor(16)	11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz   3.60 GHz
            Installed RAM	32.0 GB (31.8 GB usable)
            System type	64-bit operating system, x64-based processor
            Pen and touch	No pen or touch input is available for this display
        Windows Specification:
            Edition	Windows 11 Home
            Version	23H2
            OS build	22635.3061
            Experience	Windows Feature Experience Pack 1000.22684.1000.0


    Runs (Only IDE open, just after complete shutdown, measured using System.nanoTime around main method):
        - Baseline
            * 144,403.3814ms
        - merrykittyunsafe (#1 on LB)
            * 2,757.8295ms
        - royvanrijn (#2 on LB)
            * 1,643.9123ms ??? Assuming this is because of my system specs compared to specs on testing system
        //Obviously there were more runs than this, but these were the significant jumps
        - Me run 1 (Initial attempt;multithreading, file mapped to global Unsafe, long hash of name, read byte by byte, store in hashmap and merge from threads)
            * 5,423.4432ms
        - Me run 2 (Read longs instead of bytes to determine name hash)
            * 3,937.3234ms
        - Me run 3 (Swap to using a rolling long hash with murmur3 hashing function, change hashmap to be an openmap with unboxed long as the key)
            * 2,951.6891ms
        - Me run 4 (Change entire line reading to be long based with bit operations to determine number)
            * 2,684.9823ms
        - Me run 5 (Use main thread as one of the processing threads)
            * 2,307.3038ms
        - Me run 6 (Remove use of math.min and math.max in favor of ternary operator (Reduces getStatic operation))
            * 2,265.3521ms
 */

public class CalculateAverage_justplainlaake {

    // Constants
    private static final String FILE = "./measurements.txt";
    private static final byte SEPERATOR_BYTE = ';';
    private static final byte NEW_LINE_BYTE = '\n';
    private static final DecimalFormat STATION_FORMAT = new DecimalFormat("#,##0.0");

    private static final long[] OFFSET_CLEARS = {
            0x0000000000000000L, // 8 Offset (Clear whole thing)
            0x00000000000000FFL,
            0x000000000000FFFFL,
            0x0000000000FFFFFFL,
            0x00000000FFFFFFFFL,
            0x000000FFFFFFFFFFL,
            0x0000FFFFFFFFFFFFL,
            0x00FFFFFFFFFFFFFFL,
            0xFFFFFFFFFFFFFFFFL,// 0 Offset (Clear nothing)
    };

    private static final Unsafe UNSAFE;
    static {
        Unsafe _unsafe = null;
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            _unsafe = (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            System.exit(1);
        }
        UNSAFE = _unsafe;// Just to get around "The blank final field UNSAFE may not have been initialized"
    }

    public static void main(String[] args) throws IOException {
        int processors = Runtime.getRuntime().availableProcessors();

        ExecutorService e = null;

        List<Future<OpenMap>> futures = new ArrayList<>();
        OpenMap mainMap = null;
        try (FileChannel channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = channel.size();

            if (fileSize < 10_000) {// File is smaller than 10,000 bytes, we will lose performance trying to multithread so just set processors to 1 which will skip the futures and only use main thread
                processors = 1;
            }
            else {
                e = Executors.newFixedThreadPool(processors);// Create a ThreadPool based executor using the count of processors available
            }

            long chunkSize = fileSize / processors;// Determine approximate size of each chunk based on amount of processors available

            long startAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global())// Map the file channel into memory using the global arena (accessible by all threads)
                    .address();// And get the starting address of mapped section

            long endAddress = startAddress + fileSize;
            long currentAddress = startAddress + chunkSize;
            long chunkStart = startAddress;

            for (int i = 0; i < processors; i++) {// We need to chunk the file for each processor/thread

                while (currentAddress < endAddress) {// While loop to locate the next new line character from the chunk we are in
                    long match = UNSAFE.getLong(currentAddress);// Read the next 8 bytes as a long from the memory address
                    short offset = getMaskOffset(match, NEW_LINE_BYTE);// find the byte in the long which equals 10 aka '\n', if it is not found this returns -1
                    if (offset != -1) {// We found the offset, so add it to the current adress and break the while loop
                        currentAddress += offset;
                        break;
                    }
                    currentAddress += 8;// No offset was found so advance 8 bytes, aka 1 long
                }

                long finalChunkStart = chunkStart, finalChunkEnd = Math.min(endAddress, currentAddress - 1);// Create final fields to pass to the thread call below,
                // Also Math.min doesn't matter here since its called x times where x = count of processors

                if (i == processors - 1) {// if on last processor use main thread to optimize threading, doing on last processor means the others are already processing while this runs
                    mainMap = process(finalChunkStart, finalChunkEnd);
                }
                else {
                    futures.add(e.submit(() -> process(finalChunkStart, finalChunkEnd)));
                }
                chunkStart = currentAddress + 1;// Advance the start of the next chunk to be the end of this chunk + 1 to move past the new line character
                currentAddress = Math.min(currentAddress + chunkSize, endAddress);// Advance the next chunks end to be the end of the mapped file or the end of the approximated chunk
            }
        }

        OpenMap merged = mainMap;// Set the main map created with the process called on main thread to make it effectively final

        if (processors > 1) {// If there is only one processor then we only used the main thread so no point in merging the futures
            // The merging of processing takes ~10ms
            for (Future<OpenMap> f : futures) {
                try {

                    OpenMap processed = f.get();// Waits until the process task is done but then returns the callable value from the process method

                    // Simple way to merge both lists, tried doing it more inline inside the map and ended up taking a 10ms longer
                    processed.forEach((i, s) -> {
                        merged.merge(i, s);
                    });
                }
                catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }
            // Mark threadpool to be shutdown, call it here to let the threadpool finish out while the rest of the processing occurs
            e.shutdown();
        }

        // Ordering and printing takes 50ms
        Station[] nameOrdered = merged.toArray();// Turn the merged map into an array to quickly sort it

        Arrays.sort(nameOrdered, (n1, n2) -> n1.name.compareTo(n2.name));// Sort based on name, this might be optimizable based on the longs of the name, but would likely only gain some ms??

        // Print results to the sys out
        System.out.print("{");
        for (int i = 0; i < nameOrdered.length; i++) {
            if (i != 0) {
                System.out.print(", ");
            }
            System.out.print(nameOrdered[i]);
        }
        System.out.print("}\n");// Need newline character to meet specs
    }

    // Core processing functionality, processes a chunk of memory
    private static OpenMap process(long fromAddress, long toAddress) {

        OpenMap stationsLookup = new OpenMap();// Create a new map for this specific chunk, this is also the returned value for the callable

        long blockStart = fromAddress;
        long currentAddress = fromAddress;

        while (currentAddress < toAddress) {// Just keep looping until we exhaust the chunk

            long read = 0l;
            short offset = -1;
            // The hash is a long hash based on the murmur3 algorithm. Look at the getMurmurHash3 method to find link
            long hash = 1;

            while ((offset = getMaskOffset(read = UNSAFE.getLong(currentAddress), SEPERATOR_BYTE)) == -1) {// Read and compute the hash until we locate the seperator byte 59 or ';'
                currentAddress += 8;// forwardscan
                hash = (997 * hash) ^ getMurmurHash3(991 * read);
            }

            // Compute the final hash based using the last read long but only the effective bits (anything before the byte 59 or ';').
            // Using the OFFSET_CLEARS masks that are defined statically we can essentially segregate the important bits of the name based on the offset read above
            hash = (997 * hash) ^ getMurmurHash3(991 * (read & OFFSET_CLEARS[offset]));

            // Advance the current address/pointer to be 1 character past the end of the name Example: BillyJoel;29 would make the current address start at the '2' character
            currentAddress += offset + 1;

            Station station = stationsLookup.getOrCreate(hash, currentAddress, blockStart);

            /*
             * Possible combinations (x = number) -99.9 -> 99.9; ex: 54.4, -31.7, -4.5, 1.9
             * x.x
             * xx.x
             * -x.x
             * -xx.x
             */

            // Encoding is UTF8 however, since numbers in UTF8 are all single byte characters we can do some byte math to determin the number; 0=48 and 9=57, so character - 48 = number
            // And since - and . are also single byte characters we can make some assumptions, leading us with the primary one that no matter what the number will be 3 to 5 bytes (see above combinations)
            // Unfortunately since an integer is only 4 bytes we must read the long; Something to test would be to see if we could read an integer and then read an extra byte if it is the 5 character edge case
            read = UNSAFE.getLong(currentAddress);

            offset = 0;// reinitiate the offset to reuse the local address

            byte sign = (byte) ((read >> offset) ^ 45);// Check the first byte of the new long to see if it is 45 aka '-', if it is this byte will be 0

            // The logic below is based on the fact that we are reading
            int num = sign == 0 ? (((byte) (read >> (offset += 8))) - 48) : (((byte) read) - 48);// Start the number reading, if it is a negative advance 8 bits in the long (8 bits = 1 byte)
            currentAddress += 4;// There will always be at least 3 digits to read and the newline digit (4 total)
            if ((byte) ((read >> (offset + 8)) ^ 46) != 0) {// There can only be one more possible number for cases of (XY.X | -XY.X) where Y is that other number
                num *= 10;
                num += ((byte) (read >> (offset += 8))) - 48;
                currentAddress++;// Add one digit read if temp is 3 digits
            }
            num *= 10;
            num += ((byte) (read >> (offset + 16))) - 48;// Read the decimal character (no matter what it is 16 bits past the offset here, since 8 bits is the last number and 8 bits is the decimal)
            if (sign == 0) {
                num *= -1;
                currentAddress++;// Add another digit read for the negative sign
            }

            // Assign the values, don't use Math.min or any special bit manipulation. Faster to just use ternary
            station.min = station.min < num ? station.min : num;
            station.max = station.max > num ? station.max : num;
            station.count++;
            station.sum += num;
            // And now set the next block to start at the current address
            blockStart = currentAddress;
        }
        return stationsLookup;
    }

    // Avalanche hashing function for longs: https://github.com/aappleby/smhasher/blob/master/README.md
    public final static long getMurmurHash3(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
    }

    // Simple way to identify if a byte is set in a long at any of the 8 spots, and also to get the offset of that byte.
    // On average this is fast but certain cases could make it slow (checking 500,000,000,000 longs that don't have the test byte at all...)
    private static short getMaskOffset(long value, byte test) {
        for (short i = 0; i < 8; i++) {
            if (((byte) value & 0xFF) == test) {
                return i;
            }
            value = value >> 8;
        }
        return -1;
    }

    private static class Station {
        private final long nameStart, nameEnd;// Store the starting and ending address of the name, to fill it later
        private final int nameLength;
        private int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, count;
        private long sum;
        private String name;

        Station(long nameStart, long nameEnd) {
            this.nameStart = nameStart;
            this.nameEnd = nameEnd;
            this.nameLength = (int) (nameEnd - nameStart) + 1;// Add 1 to include seperator
        }

        protected void fillName() {
            byte[] nameBuffer = new byte[(int) (nameEnd - nameStart)];
            UNSAFE.copyMemory(null, this.nameStart, nameBuffer, Unsafe.ARRAY_BYTE_BASE_OFFSET, nameBuffer.length);// Quick memory copy, using null as src copies from the file we mapped earlier
            name = new String(nameBuffer, StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {// Use decimal format to print numbers
            return name + "=" + STATION_FORMAT.format(Math.round(min) * 0.1) + "/" + STATION_FORMAT.format(Math.round(((double) sum) / count) * 0.1) + "/"
                    + STATION_FORMAT.format(Math.round(max) * 0.1);
        }

    }

    public static class OpenMap {
        public static final float LOAD_FACTOR = 0.75f;
        public static final int EXPECTED_INITIAL_SIZE = 100_000;

        protected transient long[] keys;// Use unboxed long values as a key, faster than a doing new HashMap<Long,X>() as with generics it will box/unbox every action (can be costly in large quantities)
        protected transient Station[] values;
        protected transient int capacity;
        protected transient int maxFill;
        protected transient int mask;
        protected int size;

        public OpenMap() {
            // capacity = (int) getNextPowerOfTwo((long) Math.ceil(EXPECTED_INITIAL_SIZE / LOAD_FACTOR));// need to base the capacity on the next power of two for the mask to work properly
            // initial size of 100k gives 262,144 Capacity, since we know this and its way oversized for a max of 10k keys theres no need to recalculate
            capacity = 262_144;
            mask = capacity - 1;
            maxFill = (int) Math.ceil(capacity * 0.75f);// Only allow 75% of capacity before resizing
            keys = new long[capacity];
            values = new Station[capacity];
        }

        public void merge(long key, Station toMerge) {
            // Simple compute function, if exists pass existing, if it doesn't pass null
            int pos = (int) key & mask;// Key has already been hashed as we read, but cap it by mask
            while (values[pos] != null) {
                if (keys[pos] == key) {
                    final Station oldValue = values[pos];

                    // If names are different size but key was same, then continue to next step as hash collided
                    // Compare memory values to see if the name is same as well, prevents hash collision
                    if (oldValue.nameLength == toMerge.nameLength && compareMemory(toMerge.nameStart, oldValue.nameStart, oldValue.nameLength)) {
                        // Memory was the same, making these the same station
                        oldValue.count += toMerge.count;
                        oldValue.sum += toMerge.sum;
                        oldValue.min = oldValue.min < toMerge.min ? oldValue.min : toMerge.min;
                        oldValue.max = oldValue.max > toMerge.max ? oldValue.max : toMerge.max;
                        return;
                    }
                }
                pos = (pos + 1) & mask;
            }
            keys[pos] = key;
            values[pos] = toMerge;
            size++;
        }

        public Station getOrCreate(final long key, long currentAddress, long blockStart) {
            int pos = (int) key & mask;// Key has already been hashed as we read, but cap it by mask
            while (values[pos] != null) {// While position is set
                if (keys[pos] == key) {// Check if key is correct

                    // If names are different size but key was same, then continue to next step as hash collided
                    // Compare memory values to see if the name is same as well, prevents hash collision
                    if (values[pos].nameLength == currentAddress - blockStart && compareMemory(blockStart, values[pos].nameStart, values[pos].nameLength)) {
                        return values[pos];
                    }
                }
                pos = (pos + 1) & mask;// Since this is an open map we keep checking next masked key for an open spot (Faster than tree or linked list on a specific node)
            }
            keys[pos] = key;
            size++;
            return values[pos] = new Station(blockStart, currentAddress - 1);// Since current address contains the splitter (we will subtract by 1 here, better to do here since this is only called when it doesn't exist less math = performance)
        }

        // Simple iterator for each set value
        public void forEach(OpenConsumer consumer) {
            for (int i = 0; i < this.capacity; i++) {
                if (values[i] != null) {
                    consumer.accept(keys[i], values[i]);
                }
            }
        }

        public Station[] toArray() {
            Station[] array = new Station[size];
            int setter = 0;
            for (int i = 0; i < capacity; i++) {
                if (values[i] != null) {
                    array[setter++] = values[i];
                    values[i].fillName();
                }
            }
            return array;
        }

        // Bit function to get the next power of two on some number, used to determine best capacity based on initial size
        public long getNextPowerOfTwo(long length) {
            if (length-- == 0)
                return 1;
            length |= length >> 1;
            length |= length >> 2;
            length |= length >> 4;
            length |= length >> 8;
            length |= length >> 16;
            return (length | length >> 32) + 1;
        }

        private boolean compareMemory(long start1, long start2, int length) {
            while (length > 0) {
                if (length >= 8) {
                    if (UNSAFE.getLong(start1) != UNSAFE.getLong(start2)) {
                        return false;
                    }
                }
                else {
                    if ((UNSAFE.getLong(start1) & OFFSET_CLEARS[length]) != (UNSAFE.getLong(start2) & OFFSET_CLEARS[length])) {
                        System.out.println("Found collision: " + start1 + ": " + start2);
                        System.out.println("Found collision: " + UNSAFE.getLong(start1) + ": " + UNSAFE.getLong(start2));
                        System.out.println("Length: " + length);
                        return false;
                    }
                }
                length -= 8;
                start1 += 8;
                start2 += 8;
            }
            return true;
        }

        @FunctionalInterface
        public static interface OpenConsumer {
            void accept(long key, Station value);
        }

        @FunctionalInterface
        public static interface OpenFunction {
            Station action(long key, Station value);
        }

    }

}
