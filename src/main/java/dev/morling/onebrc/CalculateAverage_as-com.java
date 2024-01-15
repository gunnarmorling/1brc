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

import jdk.incubator.vector.*;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLongArray;

// based on spullara's submission

class CalculateAverage_asun {
    private static final String FILE = "./measurements.txt";

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;
    private static final int VECTOR_SIZE = 32;

    private static final ByteVector ASC;
    static {
        byte[] bytes = new byte[VECTOR_SIZE];
        for (int i = 0; i < VECTOR_SIZE; i++) {
            bytes[i] = (byte) i;
        }

        ASC = ByteVector.fromArray(BYTE_SPECIES, bytes, 0);
    }

    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static AtomicLongArray segmentQueue;
    @SuppressWarnings("FieldMayBeFinal")
    // @jdk.internal.vm.annotation.Contended
    private static volatile int head = 0;
    @SuppressWarnings("FieldMayBeFinal")
    // @jdk.internal.vm.annotation.Contended
    private static volatile int tail = 0;
    @SuppressWarnings("FieldMayBeFinal")
    // @jdk.internal.vm.annotation.Contended
    private static volatile boolean doneQueueing = false;

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final VarHandle headHandle;
    private static final VarHandle tailHandle;
    private static final VarHandle doneHandle;

    static {
        try {
            headHandle = LOOKUP.findStaticVarHandle(CalculateAverage_asun.class, "head", int.class);
            tailHandle = LOOKUP.findStaticVarHandle(CalculateAverage_asun.class, "tail", int.class);
            doneHandle = LOOKUP.findStaticVarHandle(CalculateAverage_asun.class, "doneQueueing", boolean.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final ArrayBlockingQueue<ByteArrayToResultMap> workerOutput = new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors());

    private static class Worker implements Runnable {
        private long segmentStart;
        private long segmentEnd;

        private final MemorySegment ms;

        private Worker(MemorySegment ms) {
            this.ms = ms;
        }

        @Override
        public void run() {
            var resultMap = new ByteArrayToResultMap();
            var ms = this.ms.asSlice(0);
            var msAddr = ms.address();
            var actualLimit = ms.byteSize();
            var buffer = new byte[100 + VECTOR_SIZE];

            while (pollSegment()) {
                long startLine;
                long pos = segmentStart;
                long limit = segmentEnd;
                long vectorLimit = Math.min(limit, actualLimit - VECTOR_SIZE);
                long longLimit = Math.min(limit, actualLimit - 8);

                // int[] lastHashMult = new int[]{ 7, 31, 63, 15, 255, 127, 3, 511 };
                // IntVector lastMul = IntVector.fromArray(INT_SPECIES, lastHashMult, 0);

                vector: while ((startLine = pos) < vectorLimit) {
                    long currentPosition = startLine;
                    ByteVector r;
                    VectorMask<Byte> m;
                    int offset = 0;

                    IntVector h = IntVector.zero(INT_SPECIES);
                    while (true) {
                        if (currentPosition >= vectorLimit) {
                            break vector;
                        }

                        r = ByteVector.fromMemorySegment(BYTE_SPECIES, ms, currentPosition, ByteOrder.LITTLE_ENDIAN);
                        r.intoArray(buffer, offset);
                        offset += VECTOR_SIZE;
                        m = r.eq((byte) ';');
                        if (m.anyTrue()) {
                            int firstTrue = m.firstTrue();
                            currentPosition += firstTrue;
                            // note: target platform likely does not have AVX-512, so manipulating and using m directly is likely to be slow
                            ByteVector lastMask = (ByteVector) ASC.lt((byte) firstTrue).toVector();
                            h = h.mul(31);
                            h = h.add(r.and(lastMask).reinterpretAsInts());
                            break;
                        }
                        else {
                            currentPosition += VECTOR_SIZE;
                            h = h.mul(31);
                            h = h.add(r.reinterpretAsInts());
                        }
                    }

                    // h = h.mul(lastMul);

                    int hash = h.reduceLanes(VectorOperators.ADD);

                    // currentPosition now has index of semicolon
                    int nameLen = (int) (currentPosition - startLine);
                    currentPosition++;

                    if (currentPosition >= longLimit) {
                        break;
                    }

                    long g = UNSAFE.getLong(msAddr + currentPosition);
                    // long g = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, currentPosition);
                    boolean minus = (g & 0xff) == '-';
                    long minusL = (minus ? 1L : 0L) - 1;
                    int negative = minus ? -1 : 1;

                    // 00101101 MINUS
                    // 00101110 PERIOD
                    // 00001101 CR
                    // 00001010 LF
                    // 00110000 0
                    // 00111001 9

                    // scan for LF
                    long lf = ~g & 0x20202020202020L;
                    int tzc = Long.numberOfTrailingZeros(lf);
                    long bytesToLF = tzc / 8;

                    int shift = 72 - tzc & 0b111000;

                    long reversedDigits = Long.reverseBytes(g & (0xFFFFFFFFFFFFFF00L | minusL)) >> shift;

                    long temp = (reversedDigits & 0xf)
                            + 10 * ((reversedDigits >> 16) & 0xf)
                            + 100 * ((reversedDigits >> 24) & 0xf);

                    temp *= negative;

                    currentPosition += bytesToLF + 1;

                    resultMap.putOrMerge(buffer, 0, nameLen, temp, hash);
                    pos = currentPosition;

                }

                while ((startLine = pos) < limit) {
                    long currentPosition = startLine;
                    byte b;
                    int offset = 0;

                    while (currentPosition != segmentEnd && (b = ms.get(ValueLayout.JAVA_BYTE, currentPosition++)) != ';') {
                        buffer[offset++] = b;
                    }
                    // Invariant: the remaining length is less than VECTOR_SIZE, so we can just run the last round of hashing
                    int hash = ByteVector.fromArray(BYTE_SPECIES, buffer, 0, ASC.lt((byte) offset))
                            .reinterpretAsInts()
                            // .mul(lastMul)
                            .reduceLanes(VectorOperators.ADD);

                    int temp;
                    int negative = 1;
                    // Inspired by @yemreinci to unroll this even further
                    if (ms.get(ValueLayout.JAVA_BYTE, currentPosition) == '-') {
                        negative = -1;
                        currentPosition++;
                    }
                    if (ms.get(ValueLayout.JAVA_BYTE, currentPosition + 1) == '.') {
                        temp = negative * ((ms.get(ValueLayout.JAVA_BYTE, currentPosition) - '0') * 10 + (ms.get(ValueLayout.JAVA_BYTE, currentPosition + 2) - '0'));
                        currentPosition += 3;
                    }
                    else {
                        temp = negative * ((ms.get(ValueLayout.JAVA_BYTE, currentPosition) - '0') * 100
                                + ((ms.get(ValueLayout.JAVA_BYTE, currentPosition + 1) - '0') * 10 + (ms.get(ValueLayout.JAVA_BYTE, currentPosition + 3) - '0')));
                        currentPosition += 4;
                    }
                    if (ms.get(ValueLayout.JAVA_BYTE, currentPosition) == '\r') {
                        currentPosition++;
                    }
                    currentPosition++;
                    resultMap.putOrMerge(buffer, 0, offset, temp, hash);
                    pos = currentPosition;
                }
            }

            workerOutput.add(resultMap);
        }

        private boolean pollSegment() {
            int head;
            int tail;

            do {
                head = (int) headHandle.getAcquire();
                tail = (int) tailHandle.getAcquire();

                while (head >= tail) {
                    if ((boolean) doneHandle.getAcquire()) {
                        return false;
                    }

                    head = (int) headHandle.getAcquire();
                    tail = (int) tailHandle.getAcquire();
                }
            } while (!headHandle.compareAndSet(head, head + 1));

            segmentStart = segmentQueue.getPlain(head * 2);
            segmentEnd = segmentQueue.getPlain(head * 2 + 1);

            return true;
        }

    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // long start = System.currentTimeMillis();
        var filename = args.length == 0 ? FILE : args[0];
        var file = new File(filename);

        @SuppressWarnings("resource")
        var fileChannel = (FileChannel) Files.newByteChannel(Path.of(filename), StandardOpenOption.READ);
        var ms = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());

        long fileSize = file.length();
        long segmentSize = 10_000_000;
        int numberOfSegments = (int) (file.length() / segmentSize + 1) * 2;
        segmentQueue = new AtomicLongArray(numberOfSegments);
        int tail = 0;

        int processors = Runtime.getRuntime().availableProcessors();

        Thread.ofPlatform().daemon().start(() -> {
            for (int i = 0; i < processors - 1; i++) {
                Thread.ofPlatform().daemon().start(new Worker(ms));
            }

            new Worker(ms).run();
        });

        long segStart = 0;
        while (segStart < fileSize) {
            long segEnd = findSegment(ms, Math.min(segStart + segmentSize, fileSize), fileSize);
            segmentQueue.setRelease(tail * 2, segStart);
            segmentQueue.setRelease(tail * 2 + 1, segEnd);
            tailHandle.setRelease(++tail);

            segStart = segEnd;
        }

        doneHandle.setRelease(true);

        // System.out.println(System.currentTimeMillis() - start);

        var resultsMap = new TreeMap<String, Result>();
        for (int i = 0; i < processors; i++) {
            var result = workerOutput.take();

            // System.out.println(i + " " + (System.currentTimeMillis() - start));

            for (Entry e : result.getAll()) {
                resultsMap.merge(new String(e.key()), e.value(), CalculateAverage_asun::merge);
            }

            // System.out.println(i + " " + (System.currentTimeMillis() - start));
        }

        System.out.println(resultsMap);

        // System.out.println(System.currentTimeMillis() - start);

        Runtime.getRuntime().halt(0);
    }

    private static Result merge(Result v, Result value) {
        return merge(v, value.min, value.max, value.sum, value.count);
    }

    private static Result merge(Result v, long value, long value1, long value2, long value3) {
        v.min = Math.min(v.min, value);
        v.max = Math.max(v.max, value1);
        v.sum += value2;
        v.count += value3;
        return v;
    }

    private static long findSegment(MemorySegment ms, long location, long fileSize) {
        while (location < fileSize) {
            if (ms.get(ValueLayout.JAVA_BYTE, location) == '\n') {
                location++;
                break;
            }

            location++;
        }
        return location;
    }

    static class Result {
        long min, max, sum;
        long count;

        Result(long value) {
            min = max = sum = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round((sum / 10.0) / count) + "/" + round(max);
        }

        double round(double v) {
            return Math.round(v * 10.0) / 10.0;
        }

        double round(long v) {
            return v / 10.0;
        }

    }

    record Entry(byte[] key, Result value) {
    }

    static class ByteArrayToResultMap {
        public static final int MAPSIZE = 1024 * 128;
        Result[] slots = new Result[MAPSIZE];
        byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(byte[] key, int offset, int size, long temp, int hash) {
            int slot = hash & (slots.length - 1);
            var slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            Result value = slotValue;
            if (value == null) {
                slots[slot] = new Result(temp);
                byte[] bytes = new byte[size];
                System.arraycopy(key, offset, bytes, 0, size);
                keys[slot] = bytes;
            }
            else {
                value.min = Math.min(value.min, temp);
                value.max = Math.max(value.max, temp);
                value.sum += temp;
                value.count += 1;
            }
        }

        // Get all pairs
        public List<Entry> getAll() {
            List<Entry> result = new ArrayList<>(slots.length);
            for (int i = 0; i < slots.length; i++) {
                Result slotValue = slots[i];
                if (slotValue != null) {
                    result.add(new Entry(keys[i], slotValue));
                }
            }
            return result;
        }
    }
}
