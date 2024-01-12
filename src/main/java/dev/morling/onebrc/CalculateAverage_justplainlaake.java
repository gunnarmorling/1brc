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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

 */

public class CalculateAverage_justplainlaake {

    // Constants
    private static final String FILE = "./measurements.txt";
    private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
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
        /*
         * Hash(3:fe;-45.1): 1829792843, -1099511627776, 3329624868385023593
         * Hash(3:fe;-45.2): 1829792843, -1099511627776, 3329624868385023593
         * Hash(3:fe;-54.9): 1829727563, -1099511627776, 3329344492919940713
         * Hash(3:fe;-48.1): 1829989451, -1099511627776, 3330469293315155561
         * Hash(3:fe;-48.4): 1829989451, -1099511627776, 3330469293315155561
         */
        // int offseta = 3;
        // long[] reads = {
        // 3329624868385023593l,
        // 3329624868385023593l,
        // 3329344492919940713l,
        // 3330469293315155561l,
        // 3330469293315155561l,
        // };

        // for (int i = 0; i < reads.length; i++){
        // long read = reads[i];
        // if (isBigEndian){
        // read = Long.reverseBytes(read);
        // }
        // System.out.println("Hash: " + (read & OFFSET_CLEARS[offseta]));
        // }

        // if (true){
        // return;
        // }

        Timer timer = new Timer(10);

        timer.push("main");
        timer.push("create-executors");
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService e = Executors.newFixedThreadPool(processors);
        List<Future<Map<Integer, Station>>> futures = new ArrayList<>();
        timer.popPush("open-channel");
        try (FileChannel channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long chunkSize = fileSize / processors;
            timer.popPush("map-channel");
            long startAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
            long endAddress = startAddress + fileSize;
            long currentAddress = startAddress + chunkSize;
            long chunkStart = startAddress;
            timer.popPush("processors");
            for (int i = 0; i < processors; i++) {
                timer.push("locate-end");
                while (currentAddress < endAddress) {
                    long match = UNSAFE.getLong(currentAddress);
                    short offset = getMaskOffset(match, NEW_LINE_BYTE);
                    if (offset != -1) {
                        currentAddress += offset;
                        break;
                    }
                    currentAddress += 8;// forwardscan
                }
                timer.popPush("schedule-task");
                long finalChunkStart = chunkStart, finalChunkEnd = Math.min(endAddress, currentAddress - 1);
                futures.add(e.submit(() -> process(finalChunkStart, finalChunkEnd)));
                chunkStart = currentAddress + 1;
                currentAddress = Math.min(currentAddress + chunkSize, endAddress);
                timer.pop();
            }
            timer.pop();
        }
        timer.push("task-merger");
        Map<Integer, Station> merged = new HashMap<>();
        for (Future<Map<Integer, Station>> f : futures) {
            try {
                Map<Integer, Station> processed = f.get();
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
        Station[] nameOrdered = merged.values().toArray(Station[]::new);// TODO Convert to some other way to compare?
        Arrays.sort(nameOrdered, (n1, n2) -> n1.name.compareTo(n2.name));
        timer.pop();
        System.out.print("{");
        for (int i = 0; i < nameOrdered.length; i++) {
            if (i != 0) {
                System.out.print(", ");
            }
            System.out.print(nameOrdered[i]);
        }
        System.out.print("}");
        timer.print();
        e.shutdown();
    }

    private static short getMaskOffset(long value, byte test) {
        if (isBigEndian) {
            value = Long.reverseBytes(value);
        }
        for (short i = 0; i < 8; i++) {
            if (((byte) value & 0xFF) == test) {
                return i;
            }
            value = value >> 8;
        }
        return -1;
    }

    private static Map<Integer, Station> process(long fromAddress, long toAddress) {
        Map<Integer, Station> stationsLookup = new HashMap<>(2_000);
        int hash = 1;
        byte stage = 0;
        byte sign = 1;
        short num = 0;
        Station station = null;
        long blockStart = fromAddress;
        long currentAddress = fromAddress;
        while (currentAddress < toAddress) {
            try {
                switch (stage) {
                    case 0:
                        long read = 0l;
                        short offset = -1;
                        while ((offset = getMaskOffset(read = UNSAFE.getLong(currentAddress), SEPERATOR_BYTE)) == -1) {
                            currentAddress += 8;// forwardscan
                            hash = longHashStep(hash, read);
                        }
                        if (offset != -1) {
                            if (isBigEndian) {
                                read = Long.reverseBytes(read);
                            }
                            hash = longHashStep(hash, read & OFFSET_CLEARS[offset]);// make sure to hash again due to the while loop exiting before hashing
                            currentAddress += offset + 1;
                            stage++;
                            station = stationsLookup.get(hash);// Bet on the likelyhood that there are no collisions in the hashed name (Extremely Rare, more likely to get eaten by a shark in Kansas)
                            if (station == null) {
                                byte[] nameBuffer = new byte[(int) (currentAddress - blockStart - 1)];
                                for (int k = 0; k < nameBuffer.length; k++) {
                                    nameBuffer[k] = UNSAFE.getByte(blockStart + k);
                                }
                                String name = new String(nameBuffer, StandardCharsets.UTF_8);
                                // if (name.equals("Yellowknife")){
                                // byte[] suffer = new byte[8];
                                // for (int k = 0; k < 8; k++) {
                                // suffer[k] = UNSAFE.getByte(currentAddress - offset + k);
                                // }
                                // System.out.println("Hash(" + offset + ":" + new String(suffer, StandardCharsets.UTF_8) + "): " + hash + ", " + OFFSET_CLEARS[offset] + ", " + read);
                                // }
                                stationsLookup.put(hash, station = new Station(name));
                            }
                        }
                        break;
                    case 1:
                        switch (UNSAFE.getByte(currentAddress++)) {
                            case NEW_LINE_BYTE:
                                num *= sign;
                                station.min = Math.min(station.min, num);
                                station.max = Math.max(station.max, num);
                                station.count++;
                                station.sum += num;
                                // Reset
                                station = null;
                                hash = 1;
                                stage = 0;
                                sign = 1;
                                num = 0;
                                blockStart = currentAddress;
                                break;
                            case 48:
                            case 49:
                            case 50:
                            case 51:
                            case 52:
                            case 53:
                            case 54:
                            case 55:
                            case 56:
                            case 57:
                                num *= 10;
                                num += UNSAFE.getByte(currentAddress - 1) - 48;
                                break;
                            case 45:// negative sign
                                sign = -1;
                                break;
                            case 46:// decimal
                                break;
                            default:
                                System.err.println("Found non valid byte " + UNSAFE.getByte(currentAddress - 1) + " @ " + (currentAddress - 1));
                                // System.err.println("Processing Line " + new String(threadBuffer));
                                // System.err.println("\t - " + Arrays.toString(threadBuffer));
                                System.exit(1);
                                break;
                        }
                        break;
                }
            }
            catch (Exception e) {
                byte[] sbuffer = new byte[(int) (toAddress - fromAddress)];
                for (int k = 0; k < sbuffer.length; k++) {
                    sbuffer[k] = UNSAFE.getByte(fromAddress + k);
                }
                System.err.println("Processing (" + fromAddress + ", " + toAddress + ") " + new String(sbuffer));
                System.err.println("\t - " + Arrays.toString(sbuffer));
                e.printStackTrace();
                System.exit(1);
            }
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

    private static int longHashStep(final int hash, final long word) {
        return 31 * hash + (int) (word ^ (word >>> 32));
    }

    private static class Station {
        private final String name;
        protected volatile int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, count;
        protected volatile long sum;

        Station(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name + "=" + STATION_FORMAT.format(min / 10.0) + '/' + STATION_FORMAT.format((sum / 10.0) / count) + '/' + STATION_FORMAT.format(max / 10.0);
        }

    }

    private static class TimerStatistics {
        protected volatile double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        protected volatile int count;
        protected volatile double sum;
        private final int depth;

        TimerStatistics(int depth) {
            this.depth = depth;
        }

        @Override
        public String toString() {
            return TIMER_FORMAT.format(min) + '/' + TIMER_FORMAT.format(sum / count) + '/' + TIMER_FORMAT.format(max);
        }
    }

    private static class Timer {
        private final Map<String, TimerStatistics> timings;
        private final String[] keyStates;
        private final long[] startStates;
        private int state = -1;

        public Timer(int stackSize) {
            keyStates = new String[stackSize];
            startStates = new long[stackSize];
            timings = new HashMap<>(stackSize * 3);//
        }

        public Timer push(String state) {
            if (this.state++ >= keyStates.length) {
                System.err.println("Timer stackSize is not big enough; Has=" + this.keyStates.length + " Requires=" + this.state);
                return this;
            }
            keyStates[this.state] = this.state > 0 ? keyStates[this.state - 1] + "." + state : state;
            startStates[this.state] = System.nanoTime();
            return this;
        }

        public Timer pop() {
            if (this.state-- < 0) {
                System.err.println("Timer popped below 0??? Pop=" + this.state);
                return this;
            }
            if (this.state < keyStates.length - 1) {
                double duration = (System.nanoTime() - startStates[this.state + 1]) / 1_000_000.0;// MS conversion
                timings.compute(keyStates[this.state + 1], (k, old) -> {
                    if (old == null) {
                        old = new TimerStatistics(this.state + 1);
                    }
                    old.count++;
                    old.sum += duration;
                    old.max = Math.max(old.max, duration);
                    old.min = Math.min(old.min, duration);
                    return old;
                });
            }
            return this;
        }

        public Timer popPush(String state) {
            return pop().push(state);
        }

        public void print() {
            System.out.println("\nTimer:");
            Entry<String, TimerStatistics>[] values = timings.entrySet().toArray(Entry[]::new);
            Arrays.sort(values, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));
            for (int i = 0; i < values.length; i++) {
                String name = values[i].getKey();
                TimerStatistics stats = values[i].getValue();
                System.out.println("  ".repeat(stats.depth) + " - " + name + " (" + stats + ")");
            }
        }

    }

    @SuppressWarnings("unchecked")
    private static class Long2ObjectHashMap<V> {

        private static final int DEFAULT_INITIAL_NODE_CAPACITY = 8;
        private static final int DEFAULT_CAPACITY = 2048;

        private final Node<V>[] entries;
        private final int initialNodeCapacity;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        Long2ObjectHashMap() {
            this(DEFAULT_CAPACITY, DEFAULT_INITIAL_NODE_CAPACITY);
        }

        Long2ObjectHashMap(int capacity) {
            this(capacity, DEFAULT_INITIAL_NODE_CAPACITY);
        }

        Long2ObjectHashMap(int capacity, int initialNodeCapacity) {
            this.entries = new Node[capacity];
            this.initialNodeCapacity = initialNodeCapacity;
        }

        public void forEach(BiLongConsumer<V> consumer) {
            try {
                lock.readLock().lock();
                for (int i = 0; i < entries.length; i++) {
                    Node<V> entry = entries[i];
                    if (entry != null) {
                        entry.forEach(consumer);
                    }
                }
            }
            finally {
                lock.readLock().unlock();
            }
        }

        public IntStream getCounts() {
            return Arrays.stream(entries).filter(Objects::nonNull).mapToInt(e -> e.count);
        }

        public V get(long key) {
            try {
                lock.readLock().lock();
                int index = hash(key);
                Node<V> entry = entries[index];
                return entry != null ? entry.get(key) : null;
            }
            finally {
                lock.readLock().unlock();
            }
        }

        public V set(long key, V value) {
            try {
                lock.writeLock().lock();
                int index = hash(key);
                Node<V> entry = entries[index];
                if (entry == null) {
                    entries[index] = entry = new Node<V>(initialNodeCapacity);
                }
                return entry.set(key, value);
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        public V getOrSet(long key, Supplier<V> supplier) {
            try {
                lock.writeLock().lock();
                int index = hash(key);
                Node<V> entry = entries[index];
                if (entry == null) {
                    entries[index] = entry = new Node<V>(initialNodeCapacity);
                }
                return entry.getOrSet(key, supplier);
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        public V getOrSet(long key, V value) {
            try {
                lock.writeLock().lock();
                int index = hash(key);
                Node<V> entry = entries[index];
                if (entry == null) {
                    entries[index] = entry = new Node<V>(initialNodeCapacity);
                }
                return entry.getOrSet(key, value);
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        private int hash(long key) {
            return Math.abs((int) (key ^ (key >>> 32))) % entries.length;
        }

    }

    @SuppressWarnings("unchecked")
    private static class Node<V> {
        private long[] keys;
        private Object[] values;
        private int count = 0;

        Node(int initilaCapacity) {
            this.keys = new long[initilaCapacity];
            this.values = new Object[initilaCapacity];
        }

        V get(long key) {
            int index = Arrays.binarySearch(keys, 0, count, key);
            if (index >= 0) {
                return (V) values[index];
            }
            return null;
        }

        V set(long key, V value) {
            int index = Arrays.binarySearch(keys, 0, count, key);
            if (index >= 0) {
                V old = (V) values[index];
                values[index] = value;
                return old;
            }
            int insertIndex = -(index + 1);
            if (count == keys.length - 1) {// current count is too small for an addition, expand
                keys = Arrays.copyOf(keys, keys.length * 2);
                values = Arrays.copyOf(values, values.length * 2);
            }
            System.arraycopy(keys, insertIndex, keys, insertIndex + 1, count - insertIndex);
            System.arraycopy(values, insertIndex, values, insertIndex + 1, count - insertIndex);
            count++;
            keys[insertIndex] = key;
            values[insertIndex] = value;
            return null;
        }

        V getOrSet(long key, Supplier<V> supplier) {
            int index = Arrays.binarySearch(keys, 0, count, key);
            if (index >= 0) {
                return (V) values[index];
            }
            int insertIndex = -(index + 1);
            if (count == keys.length - 1) {// current count is too small, expand
                keys = Arrays.copyOf(keys, keys.length * 2);
                values = Arrays.copyOf(values, values.length * 2);
            }
            System.arraycopy(keys, insertIndex, keys, insertIndex, count - insertIndex);
            System.arraycopy(values, insertIndex, values, insertIndex, count - insertIndex);
            count++;
            keys[insertIndex] = key;
            return (V) (values[insertIndex] = supplier.get());
        }

        V getOrSet(long key, V value) {
            int index = Arrays.binarySearch(keys, 0, count, key);
            if (index >= 0) {
                return (V) values[index];
            }
            int insertIndex = -(index + 1);
            if (count == keys.length - 1) {// current count is too small, expand
                keys = Arrays.copyOf(keys, keys.length * 2);
                values = Arrays.copyOf(values, values.length * 2);
            }
            System.arraycopy(keys, insertIndex, keys, insertIndex, count - insertIndex);
            System.arraycopy(values, insertIndex, values, insertIndex, count - insertIndex);
            count++;
            keys[insertIndex] = key;
            return (V) (values[insertIndex] = value);
        }

        void forEach(BiLongConsumer<V> consumer) {
            for (int i = 0; i < count; i++) {
                consumer.accept(keys[i], (V) values[i]);
            }
        }

    }

    @FunctionalInterface
    private static interface BiLongConsumer<V> {
        void accept(long l, V value);
    }

}
