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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
import java.util.concurrent.ThreadLocalRandom;

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
            Processor	11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz   3.60 GHz
            Installed RAM	32.0 GB (31.8 GB usable)
            Device ID	58C79E9F-1E2D-433B-A739-A901DFD2EDE1
            Product ID	00326-10000-00000-AA794
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

    static int getCharNumber(long serial, int offset){
        return (byte)((byte)(serial >> (8*offset)) - 48);
    }
    public static void main(String[] args) throws IOException {
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService e = Executors.newFixedThreadPool(processors);
        List<Future<OpenMap>> futures = new ArrayList<>();
        OpenMap mainMap = null;
        try (FileChannel channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long chunkSize = fileSize / processors;
            long startAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            long endAddress = startAddress + fileSize;
            long currentAddress = startAddress + chunkSize;
            long chunkStart = startAddress;
            for (int i = 0; i < processors; i++) {
                while (currentAddress < endAddress) {
                    long match = UNSAFE.getLong(currentAddress);
                    short offset = getMaskOffset(match, NEW_LINE_BYTE);
                    if (offset != -1) {
                        currentAddress += offset;
                        break;
                    }
                    currentAddress += 8;// forwardscan
                }
                long finalChunkStart = chunkStart, finalChunkEnd = Math.min(endAddress, currentAddress - 1);
                if (i == processors-1){//if on last processor use main thread to optimize threading.
                    mainMap = process(finalChunkStart, finalChunkEnd);
                } else {
                    futures.add(e.submit(() -> process(finalChunkStart, finalChunkEnd)));
                }
                chunkStart = currentAddress + 1;
                currentAddress = Math.min(currentAddress + chunkSize, endAddress);
            }
        }
        OpenMap merged = mainMap;
        //The merging of processing takes ~10ms
        for (Future<OpenMap> f : futures) {
            try {
                OpenMap processed = f.get();
                processed.forEach((i, s) -> {
                    merged.compute(i, (key, s1) -> {
                        if (s1 == null) {
                            return s;
                        }
                        s1.count += s.count;
                        s1.max = Math.max(s1.max, s.max);
                        s1.min = Math.min(s1.min, s.min);
                        s1.sum += s.sum;
                        return s1;
                    });
                });
            } catch (InterruptedException | ExecutionException e1) {
                e1.printStackTrace();
            }
        }
        //Ordering and printing takes 50ms
        Station[] nameOrdered = merged.toArray();// TODO Convert to some other way to compare?
        Arrays.sort(nameOrdered, (n1, n2) -> n1.name.compareTo(n2.name));
        System.out.print("{");
        for (int i = 0; i < nameOrdered.length; i++) {
            if (i != 0) {
                System.out.print(", ");
            }
            System.out.print(nameOrdered[i]);
        }
        System.out.print("}");
        e.shutdownNow();
    }

    private static OpenMap process(long fromAddress, long toAddress) {
        OpenMap stationsLookup = new OpenMap();
        long blockStart = fromAddress;
        long currentAddress = fromAddress;
        while (currentAddress < toAddress) {
            long read = 0l;
            short offset = -1;
            long hash = 1;
            while ((offset = getMaskOffset(read = UNSAFE.getLong(currentAddress), SEPERATOR_BYTE)) == -1) {
                currentAddress += 8;// forwardscan
                hash = 997 * hash ^ 991 * getMurmurHash3(read);
            }
            
            hash = 997 * hash ^ 991 * getMurmurHash3(read & OFFSET_CLEARS[offset]);
            currentAddress += offset + 1;
            Station station = stationsLookup.getOrCreate(hash, currentAddress, blockStart);
            // Bet on the likelyhood that there are no collisions in the hashed name (Extremely Rare, more likely to get eaten by a shark in Kansas)
            // Odds are over 1 in 100 Billion since there are a max of 10,000 unique names

            
            /* Possible combinations (x = number) -99.9 -> 99.9; ex: 54.4, -31.7, -4.5, 1.9
            *  x.x
            *  xx.x
            *  -x.x
            *  -xx.x
            */
            read = UNSAFE.getLong(currentAddress);
            offset = 0;
            byte sign = (byte) ((read >> offset) ^ 45);
            int num = sign == 0 ? (((byte) (read >> (offset+=8))) - 48) : (((byte) read) - 48);
            currentAddress += 4;//There will always be at least 3 digits to read and the newline digit (4 total)
            if ((byte) ((read >> (offset+8)) ^ 46) != 0){//There can only be one more possible number for cases of (XY.X | -XY.X) where Y is that other number
                num *= 10;
                num += ((byte) (read >> (offset+=8))) - 48;
                currentAddress++;//Add one digit read if temp > 10
            }
            num *= 10;
            num += ((byte) (read >> (offset+16))) - 48;
            if (sign == 0){
                num *= -1;
                currentAddress++;//Add another digit read for the negative sign
            }
            station.min = station.min < num ? station.min : num;
            station.max = station.max > num ? station.max : num;
            station.count++;
            station.sum += num;
            hash = 1;
            blockStart = currentAddress;
        }
        return stationsLookup;
    }

    //Avalanche hashing function for longs: https://github.com/aappleby/smhasher/blob/master/README.md
    public final static long getMurmurHash3(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
    }

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
        protected final long nameStart, nameEnd;
        protected int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, count;
        protected long sum;
        protected String name;

        Station(long nameStart, long nameEnd) {
            this.nameStart = nameStart;
            this.nameEnd = nameEnd;
        }

        protected void fillName(){
            byte[] nameBuffer = new byte[(int) (nameEnd - nameStart)];
            for (int k = 0; k < nameBuffer.length; k++) {
                nameBuffer[k] = UNSAFE.getByte(nameStart + k);
            }
            name = new String(nameBuffer, StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return name + "=" + STATION_FORMAT.format(min * 0.1) + '/' + STATION_FORMAT.format((sum * 0.1) / count) + '/' + STATION_FORMAT.format(max * 0.1);
        }

    }

    @SuppressWarnings("unchecked")
    public static  class OpenMap {
            public static final float LOAD_FACTOR = 0.75f;
            public static final int EXPECTED_INITIAL_SIZE = 100_000;

            protected transient long[] keys;
            protected transient Station[] values;
            protected transient boolean[] marked;
            protected transient int capacity;
            protected transient int maxFill;
            protected transient int mask;
            protected int size;

            public OpenMap() {
                capacity = (int) getNextPowerOfTwo((long) Math.ceil(EXPECTED_INITIAL_SIZE / LOAD_FACTOR));//need to base the capacity on the next power of two for the mask to work properly
                mask = capacity - 1;
                maxFill = (int) Math.ceil(capacity * 0.75f);
                keys = new long[capacity];
                values = new Station[capacity];
                marked = new boolean[capacity];
            }

            public Station put(final long k, final Station v) {
                int pos = (int) getMurmurHash3(k) & mask;
                while (marked[pos]) {
                    if (((keys[pos]) == (k))) {
                        final Station oldValue = values[pos];
                        values[pos] = v;
                        return oldValue;
                    }
                    pos = (pos + 1) & mask;
                }
                marked[pos] = true;
                keys[pos] = k;
                values[pos] = v;
                if (++size >= maxFill)
                    rehash((int) getNextPowerOfTwo((long) Math.ceil((size+1) / LOAD_FACTOR)));
                return null;
            }

            public void compute(long key, OpenFunction compute) {
                Station value = compute.action(key, get(key));
                if (value != null) {
                    put(key, value);
                }
            }

            public Station get(final long k) {
                int pos = (int) getMurmurHash3(k) & mask;
                while (marked[pos]) {
                    if (((keys[pos]) == (k)))
                        return values[pos];
                    pos = (pos + 1) & mask;
                }
                return null;
            }

            public Station getOrCreate(final long key, long currentAddress, long blockStart) {
                int pos = (int) getMurmurHash3(key) & mask;
                while (marked[pos]) {
                    if (((keys[pos]) == (key)))
                        return values[pos];
                    pos = (pos + 1) & mask;
                }
                marked[pos] = true;
                keys[pos] = key;
                values[pos] = new Station(blockStart, currentAddress-1);
                if (++size >= maxFill)
                    rehash((int) getNextPowerOfTwo((long) Math.ceil((size+1) / LOAD_FACTOR)));
                return values[pos];
            }

            public Station[] toArray() {
                Station[] array = new Station[size];
                int setter = 0;
                for (int i = 0; i < capacity; i++) {
                    if (marked[i]) {
                        array[setter++] = values[i];
                        values[i].fillName();
                    }
                }
                return array;
            }

            public void forEach(OpenConsumer consumer) {
                for (int i = 0; i < this.capacity; i++) {
                    if (marked[i]) {
                        consumer.accept(keys[i], values[i]);
                    }
                }
            }

            protected void rehash(final int newCapacity) {
                final long[] newKeys = new long[newCapacity];
                final Station[] newValues = new Station[newCapacity];
                final boolean[] newMarked = new boolean[newCapacity];

                int i = 0, pos;
                long k;
                final int newMask = newCapacity - 1;
                for (int j = size; j-- != 0;) {
                    while (!this.marked[i])
                        i++;
                    k = this.keys[i];
                    pos = (int) getMurmurHash3(k) & newMask;
                    while (newMarked[pos])
                        pos = (pos + 1) & newMask;
                    newMarked[pos] = true;
                    newKeys[pos] = k;
                    newValues[pos] = this.values[i];
                    i++;
                }
                capacity = newCapacity;
                mask = newMask;
                maxFill = (int) Math.ceil(capacity * LOAD_FACTOR);
                this.keys = newKeys;
                this.values = newValues;
                this.marked = newMarked;
            }

            //Bit function
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
