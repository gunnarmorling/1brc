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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import sun.misc.Unsafe;

/*
 
    BASELINE (Shell)
        - 141.88user 20.25system 9:06.44elapsed 29%CPU (0avgtext+0avgdata 353580maxresident)k 
          0inputs+2848outputs (357major+46292minor)pagefaults 0swaps
    ROYVANRIJN (Bash)
        -   real    1m19.580s
            user    0m18.550s
            sys     0m55.714s
    ME - Attempt 1 (Bash)
        -   real    1m21.375s
            user    1m6.361s
            sys     0m52.716s
        -  Checking results difference from (https://www.diffchecker.com/text-compare/)
            The two files are identical
            There is no difference to show between these two files
    ME - Attempt 2 [Some optimizations and changes to using longs for names] (Bash)
        -   real    1m19.508s
            user    0m42.588s
            sys     0m53.621s
    ME - Attempt 3 [Added timers, added long hashing for keys and offset to get proper key and reset the address, After Restart]
        Timer:
        - main.create-executors (1.30/1.30/1.30)
        - main.map-channel (2.11/2.11/2.11)
        - main.open-channel (3.75/3.75/3.75)
        - main.processors (2.61/2.61/2.61)
            - main.processors.locate-end (0.00/0.01/0.02)
            - main.processors.schedule-task (0.04/0.14/1.23)
        - main.task-merger (4,942.01/4,942.01/4,942.01)
    ME - Attempt 4 [With OpenMap]
        Timer:
        - main.create-executors (1.30/1.30/1.30)
        - main.map-channel (2.11/2.11/2.11)
        - main.open-channel (3.75/3.75/3.75)
        - main.processors (2.61/2.61/2.61)
            - main.processors.locate-end (0.00/0.01/0.02)
            - main.processors.schedule-task (0.04/0.14/1.23)
        - main.task-merger (4,942.01/4,942.01/4,942.01)
    royvanrijn - Current #1 tested on windows 
        Millis 1,814.9423
    Me - Remove timer
        Millis 3,876.1646

 */

public class CalculateAverage_justplainlaake {

    // Constants
    private static final String FILE = "./measurements.txt";
    private static final byte SEPERATOR_BYTE = ';';
    private static final byte NEW_LINE_BYTE = '\n';
    private static final DecimalFormat STATION_FORMAT = new DecimalFormat("#,##0.0");
    private static final DecimalFormat TIMER_FORMAT = new DecimalFormat("#,##0.00");

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

        /* Possible combinations (x = number)
         *  x.x
         *  xx.x
         *  -x.x
         *  -xx.x
         */

        // System.out.println();
        // long[][] unsafes = {
        //     {7020080520972088369l, 143, 1},
        //     {7449318865373114929l, 125, 1},
        //     {7521378658417522481l, 174, 1},
        //     {7009582564255150381l, -134, -1},
        //     {7953728620758709037l, -38, -1},
        //     {8533869686817107512l, 85, 1},
        //     {7306036007582708019l, 314, 1},
        //     {8458430025076649529l, 91, 1},
        //     {8028864847236970802l, 291, 1},
        //     {7089020999645933617l, 109, 1},
        //     {7600188353344844851l, 343, 1},
        //     {7238764587710820404l, 407, 1},
        //     {7166706993672894513l, 187, 1},
        //     {7449334258569458482l, 237, 1},
        //     {7018951322530361906l, 223, 1},
        //     {7020922746912519730l, 265, 1},
        //     {7810733834998002477l, -36, -1},
        // };


        // for (int i = 0; i < unsafes.length; i++){
        //     long unsafe = unsafes[i][0];
        //     long num = unsafes[i][1];
        //     long sign = unsafes[i][2];

        //     int offset = 0;
        //     byte attemptSign = (byte) ((unsafe >> offset) ^ 45);
        //     long attemptedNum = attemptSign == 0 ? (((byte) (unsafe >> (offset+=8))) - 48) : (((byte) unsafe) - 48);
        //     if ((byte) ((unsafe >> (offset+8)) ^ 46) != 0){//There can only be one more possible number
        //         attemptedNum *= 10;
        //         attemptedNum += ((byte) (unsafe >> (offset+=8))) - 48;
        //     }
        //     attemptedNum *= 10;
        //     attemptedNum += ((byte) (unsafe >> (offset+16))) - 48;
        //     System.out.println("Attempt " + num + ", " + (attemptSign == 0 ? -attemptedNum : attemptedNum));
        // }

        // if (true){
        //     return;
        // }

        long start = System.nanoTime();

        int processors = Runtime.getRuntime().availableProcessors() + 1;
        ExecutorService e = Executors.newFixedThreadPool(processors);
        List<Future<OpenMap>> futures = new ArrayList<>();
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
                futures.add(e.submit(() -> process(finalChunkStart, finalChunkEnd)));
                chunkStart = currentAddress + 1;
                currentAddress = Math.min(currentAddress + chunkSize, endAddress);
            }
        }
        OpenMap merged = new OpenMap();
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
            }
            catch (InterruptedException | ExecutionException e1) {
                e1.printStackTrace();
            }
        }
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
        e.shutdown();

        System.out.println("\nMillis: " + ((System.nanoTime() - start) / 1_000_000.0));
    }

    private static OpenMap process(long fromAddress, long toAddress) {
        OpenMap stationsLookup = new OpenMap();
        long hash = 1;
        int num = 0;
        byte sign = 1;
        Station station = null;
        long blockStart = fromAddress;
        long currentAddress = fromAddress;
        while (currentAddress < toAddress) {
            long read = 0l;
            short offset = -1;
            while ((offset = getMaskOffset(read = UNSAFE.getLong(currentAddress), SEPERATOR_BYTE)) == -1) {
                currentAddress += 8;// forwardscan
                hash = 997 * hash ^ 991 * getMurmurHash3(read);
            }
            if (offset != -1) {
                hash = 997 * hash ^ 991 * getMurmurHash3(read & OFFSET_CLEARS[offset]);
                currentAddress += offset + 1;
                station = stationsLookup.getOrCreate(hash, currentAddress, blockStart);// Bet on the likelyhood that there are no collisions in the hashed name (Extremely Rare, more likely to get eaten by a shark in Kansas)
                // Odds are over 1 in 100 Billion since there are a max of 10,000 unique names
            }
            long unsafeNum = UNSAFE.getLong(currentAddress);
            offset = 0;
            sign = (byte) ((unsafeNum >> offset) ^ 45);
            num = sign == 0 ? (((byte) (unsafeNum >> (offset+=8))) - 48) : (((byte) unsafeNum) - 48);
            currentAddress += 4;//There will always be at least 3 digits to read and the newline digit (4 total)
            if ((byte) ((unsafeNum >> (offset+8)) ^ 46) != 0){//There can only be one more possible number
                num *= 10;
                num += ((byte) (unsafeNum >> (offset+=8))) - 48;
                currentAddress++;//Add one digit read if temp > 10
            }
            num *= 10;
            num += ((byte) (unsafeNum >> (offset+16))) - 48;
            if (sign == 0){
                num *= -1;
                currentAddress++;//Add another digit read for the negative sign
            }
            station.min = Math.min(station.min, num);
            station.max = Math.max(station.max, num);
            station.count++;
            station.sum += num;
            hash = 0;
            station = null;
            blockStart = currentAddress;
            
            // K: for (int i = 0; i < 6; i++){
            //     switch (UNSAFE.getByte(currentAddress++)) {
            //         case NEW_LINE_BYTE:
            //             num *= sign;
            //             station.min = Math.min(station.min, num);
            //             station.max = Math.max(station.max, num);
            //             station.count++;
            //             station.sum += num;
            //             // Reset
            //             station = null;
            //             hash = 1;
            //             sign = 1;
            //             num = 0;
            //             blockStart = currentAddress;
            //             break K;
            //         case 48:
            //         case 49:
            //         case 50:
            //         case 51:
            //         case 52:
            //         case 53:
            //         case 54:
            //         case 55:
            //         case 56:
            //         case 57:
            //             num *= 10;
            //             num += UNSAFE.getByte(currentAddress - 1) - 48;
            //             break;
            //         case 45:// negative sign
            //             sign = -1;
            //             break;
            //         case 46:// decimal
            //             break;
            //         default:
            //             System.err.println("Found non valid byte " + UNSAFE.getByte(currentAddress - 1) + " @ " + (currentAddress - 1));
            //             // System.err.println("Processing Line " + new String(threadBuffer));
            //             // System.err.println("\t - " + Arrays.toString(threadBuffer));
            //             System.exit(1);
            //             break;
            //     }
            // }
        }
        if (station != null && blockStart != currentAddress) {
            num *= sign;
            station.min = Math.min(station.min, num);
            station.max = Math.max(station.max, num);
            station.count++;
            station.sum += num;
            // Reset
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
            return name + "=" + STATION_FORMAT.format(min / 10.0) + '/' + STATION_FORMAT.format((sum / 10.0) / count) + '/' + STATION_FORMAT.format(max / 10.0);
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

            public boolean containsKey(final long k) {
                int pos = (int) getMurmurHash3(k) & mask;
                while (marked[pos]) {
                    if (((keys[pos]) == (k)))
                        return true;
                    pos = (pos + 1) & mask;
                }
                return false;
            }

            protected void rehash(final int newCapacity) {
                int i = 0, pos;
                long k;
                final int newMask = newCapacity - 1;
                final long[] keys = this.keys;
                final Station[] values = this.values;
                final boolean[] marked = this.marked;

                final long[] newKeys = new long[newCapacity];
                final Station[] newValues = new Station[newCapacity];
                final boolean[] newMarked = new boolean[newCapacity];
                for (int j = size; j-- != 0;) {
                    while (!marked[i])
                        i++;
                    k = keys[i];
                    pos = (int) getMurmurHash3(k) & newMask;
                    while (newMarked[pos])
                        pos = (pos + 1) & newMask;
                    newMarked[pos] = true;
                    newKeys[pos] = k;
                    newValues[pos] = values[i];
                    i++;
                }
                capacity = newCapacity;
                mask = newMask;
                maxFill = (int) Math.ceil(capacity * LOAD_FACTOR);
                this.keys = newKeys;
                this.values = newValues;
                this.marked = newMarked;
            }

            public void forEach(OpenConsumer consumer) {
                for (int i = 0; i < this.capacity; i++) {
                    if (marked[i]) {
                        consumer.accept(keys[i], values[i]);
                    }
                }
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
