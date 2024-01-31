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

import java.io.*;
import java.lang.foreign.*;
import java.lang.reflect.Field;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import sun.misc.Unsafe;

/*
 * Stephen Von Worley's (von@von.io) entry to Gunnar Morling's "One Billion Row Challenge":
 * https://www.morling.dev/blog/one-billion-row-challenge/
 *
 * To compute the desired result, this program:
 * 1. Memory maps the input file.
 * 2. Partitions the file into a queue of Chunks, which delimit sections of the file.
 * 3. Spawns one thread per processor. Each thread:
 *    a. Allocates a Table, which will accumulate names and tallies (min/max/total/count).
 *    b. Get a Chunk from the queue.
 *    c. Processes the Chunk using a parser that reads the Chunk simultaneously at three
 *       different, evenly-spaced locations, using heavily-optimized scalar code.
 *    d. Repeats steps b and c until there are no more Chunks.
 * 4. Aggregates the resulting Tables into a treemap of names to Tallies.
 * 5. Outputs the names and Tallies in ascending name order.
 *
 * Runs fastest as a natively-compiled, standalone binary, as might be produced by Graal's
 * `native-image` utility.  Tested with Oracle Graal 21.0.2.
 * 
 * Incorporates code authored by a number of submitters, including Thomas Wue, Quan Anh
 * Mai, and others.
 *
 * Thanks y'all, and Happy Rowing!
 * Steve
 * von@von.io
 * www.von.io
 */

public class CalculateAverage_stephenvonworley {

    private static final int NAME_LIMIT = 10000;

    private static final long CHUNK_SIZE = 5000000;
    private static final long CHUNK_PAD = 200;
    private static final long CHUNK_PARSE3_LIMIT = 1000;

    private static final long GOLDEN_LONG = 0x9e3779b97f4a7c15L;
    private static final long TALLY_BITS = 7;
    private static final long TALLY_SIZE = 1L << TALLY_BITS;
    private static final long HASH_BITS = 16;
    private static final long HASH_MASK = ((1L << HASH_BITS) - 1) << TALLY_BITS;
    private static final long TABLE_SIZE = 1L << (HASH_BITS + TALLY_BITS);

    private static final long OFFSET_MIN = 0;
    private static final long OFFSET_MAX = 2;
    private static final long OFFSET_COUNT = 4;
    private static final long OFFSET_TOTAL = 8;
    private static final long OFFSET_LEN = 16;
    private static final long OFFSET_NAME = 17;

    private static final Unsafe unsafe;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException("Exception initializing unsafe", e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (!List.of(args).contains("--worker")) {
            spawnWorker();
            return;
        }

        MemorySegment in = map("./measurements.txt");
        Queue<Chunk> chunks = partition(in);
        List<Table> tables = process(chunks, processorCount());
        Map<String, Tally> nameToTally = aggregate(tables);

        System.out.println(nameToTally);
        System.out.close();
    }

    // credit: "Spawn worker" code by Thomas Wue
    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder().command(workerCommand).inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start().getInputStream().transferTo(System.out);
    }

    private static int processorCount() {
        return Runtime.getRuntime().availableProcessors();
    }

    private static MemorySegment map(String path) throws IOException {
        FileChannel file = FileChannel.open(Path.of(path), StandardOpenOption.READ);
        return file.map(FileChannel.MapMode.READ_ONLY, 0, file.size(), Arena.global());
    }

    private static MemorySegment allocate(long len) {
        return Arena.global().allocate(len, 4096);
    }

    private static Queue<Chunk> partition(MemorySegment in) throws IOException {
        Queue<Chunk> chunks = new ConcurrentLinkedDeque<>();
        long address = in.address();
        long len = in.byteSize();
        long start = address;
        while (start < address + len) {
            long end = start + CHUNK_SIZE;
            if (end >= address + len) {
                end = address + len;
            }
            else {
                end = afterNewline(end);
            }
            Chunk chunk;
            if (end + CHUNK_PAD < address + len) {
                chunk = new Chunk(start, end);
            }
            else {
                MemorySegment padded = allocate(end - start + CHUNK_PAD);
                MemorySegment.copy(in, start - address, padded, 0, end - start);
                chunk = new Chunk(padded.address(), padded.address() + (end - start));
            }
            chunks.offer(chunk);
            start = end;
        }
        return chunks;
    }

    private static List<Table> process(Queue<Chunk> chunks, int threadCount) throws InterruptedException {
        List<Table> tables = Collections.synchronizedList(new ArrayList<>(threadCount));
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                Table t = new Table();
                tables.add(t);
                Chunk chunk;
                while ((chunk = chunks.poll()) != null) {
                    parse3(chunk.start(), chunk.end(), t);
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        return tables;
    }

    private static Map<String, Tally> aggregate(List<Table> tables) {
        Map<String, Tally> nameToTally = new TreeMap<>();
        tables.forEach(table -> aggregate(nameToTally, table));
        return nameToTally;
    }

    private static void aggregate(Map<String, Tally> nameToTally, Table table) {
        table.process((name, min, max, total, count) -> nameToTally.computeIfAbsent(name, _ -> new Tally()).add(min, max, total, count));
    }

    private static void parse3(long start, long end, Table table) {

        if (end - start < CHUNK_PARSE3_LIMIT) {
            parse1(start, end, table);
            return;
        }

        final long tallies = table.tallies;

        long part = (end - start) / 3;
        long startA = start;
        long startB = afterNewline(start + part);
        long startC = afterNewline(start + 2 * part);
        long endA = startB;
        long endB = startC;
        long endC = end;

        while (true) {
            long N = min(
                    remaining(startA, endA),
                    remaining(startB, endB),
                    remaining(startC, endC));

            if (N <= 1) {
                break;
            }

            while (N > 0) {
                long semicolonA = semicolon(startA);
                long semicolonB = semicolon(startB);
                long semicolonC = semicolon(startC);

                long tallyA = locate(startA, semicolonA, tallies, table);
                long tallyB = locate(startB, semicolonB, tallies, table);
                long tallyC = locate(startC, semicolonC, tallies, table);

                long numberA = number(semicolonA);
                tally(tallyA, numberA);
                long numberB = number(semicolonB);
                tally(tallyB, numberB);
                long numberC = number(semicolonC);
                tally(tallyC, numberC);

                startA = next(semicolonA);
                startB = next(semicolonB);
                startC = next(semicolonC);
                N--;
            }
        }

        parse1(startA, endA, table);
        parse1(startB, endB, table);
        parse1(startC, endC, table);
    }

    private static void parse1(long start, long end, Table table) {
        final long tallies = table.tallies;

        while (start < end) {
            long semicolon = semicolon(start);
            long tally = locate(start, semicolon, tallies, table);
            long number = number(semicolon);
            tally(tally, number);
            start = next(semicolon);
        }
    }

    private static long remaining(long start, long end) {
        return (end - start) >> 7;
    }

    // credit: Adapted from code by Thomas Wue
    private static long semicolon(long start) {
        start++;
        long word = getLong(start);
        long input = word ^ 0x3B3B3B3B3B3B3B3BL;
        long tmp = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
        if (tmp != 0) {
            return start + (Long.numberOfTrailingZeros(tmp) >>> 3);
        }
        while (true) {
            start += 8;
            long word2 = getLong(start);
            long input2 = word2 ^ 0x3B3B3B3B3B3B3B3BL;
            long tmp2 = (input2 - 0x0101010101010101L) & ~input2 & 0x8080808080808080L;
            if (tmp2 != 0) {
                return start + (Long.numberOfTrailingZeros(tmp2) >>> 3);
            }
        }
    }

    private static long trim(long value, long remove) {
        long shift = remove << 3;
        return ((value << shift) >>> shift);
    }

    // https://softwareengineering.stackexchange.com/questions/402542/where-do-magic-hashing-constants-like-0x9e3779b9-and-0x9e3779b1-come-from
    private static long locate(long start, long semicolon, long tallies, Table table) {
        long len = semicolon - start;
        long word = getLong(start);
        if (len <= 8) {
            word = trim(word, 8 - len);
            long hash = word * GOLDEN_LONG;
            long offset = (hash >>> (64 - HASH_BITS)) << TALLY_BITS;
            while (true) {
                long tally = tallies + offset;
                long tlen = getByte(tally + OFFSET_LEN);
                long tword = getLong(tally + OFFSET_NAME);
                if (len == tlen && word == tword) {
                    return tally;
                }
                if (tword == 0) {
                    init(tally, start, len, table);
                    return tally;
                }
                offset = (offset + TALLY_SIZE) & HASH_MASK;
            }
        }
        else {
            long word2 = getLong(semicolon - 8);
            long hash = (word + word2) * GOLDEN_LONG;
            long offset = (hash >>> (64 - HASH_BITS)) << TALLY_BITS;
            while (true) {
                long tally = tallies + offset;
                long tword = getLong(tally + OFFSET_NAME);
                if (len <= 16) {
                    long tlen = getByte(tally + OFFSET_LEN);
                    long tword2 = getLong(tally + OFFSET_NAME + len - 8);
                    if (len == tlen && word == tword && word2 == tword2) {
                        return tally;
                    }
                }
                else {
                    if (match(tally, start, len)) {
                        return tally;
                    }
                }
                if (tword == 0) {
                    init(tally, start, len, table);
                    return tally;
                }
                offset = (offset + TALLY_SIZE) & HASH_MASK;
            }
        }
    }

    private static void init(long tally, long start, long len, Table t) {
        setShort(tally + OFFSET_MIN, Short.MAX_VALUE);
        setShort(tally + OFFSET_MAX, Short.MIN_VALUE);
        setByte(tally + OFFSET_LEN, (byte) len);
        copyMemory(start, tally + OFFSET_NAME, len);
        t.addresses[t.count++] = tally;
    }

    private static boolean match(long tally, long name, long len) {
        if (getByte(tally + OFFSET_LEN) != len) {
            return false;
        }
        long a = name;
        long b = tally + OFFSET_NAME;
        while (len > 7) {
            if (getLong(a) != getLong(b)) {
                return false;
            }
            a += 8;
            b += 8;
            len -= 8;
        }
        if (len > 0) {
            return (trim(getLong(a), 8 - len) == getLong(b));
        }
        return true;
    }

    // credit: Wonderfully-fast number parsing implementation by Quan Anh Mai
    private static long number(long semicolon) {
        long numberWord = getLong(semicolon + 1);
        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        int shift = 28 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~numberWord << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii to digit value
        long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;
        // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
        // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
        // 0x000000UU00TTHH00 + 0x00UU00TTHH000000 * 10 + 0xUU00TTHH00000000 * 100
        long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        return (absValue ^ signed) - signed;
    }

    private static void tally(long tally, long number) {
        short min = getShort(tally + OFFSET_MIN);
        short max = getShort(tally + OFFSET_MAX);
        int count = getInt(tally + OFFSET_COUNT);
        long total = getLong(tally + OFFSET_TOTAL);
        if (number < min) {
            setShort(tally + OFFSET_MIN, (short) number);
        }
        if (number > max) {
            setShort(tally + OFFSET_MAX, (short) number);
        }
        setInt(tally + OFFSET_COUNT, count + 1);
        setLong(tally + OFFSET_TOTAL, total + number);
    }

    private static long next(long semicolon) {
        long word = getLong(semicolon);
        semicolon += 7;
        semicolon -= (~word >>> (24 + 4)) & 1;
        semicolon -= (~word >>> (16 + 4 - 1)) & 2;
        return semicolon;
    }

    private static long afterNewline(long start) {
        while (getByte(start) != '\n')
            start++;
        return start + 1;
    }

    private static long min(long a, long b, long c) {
        return Math.min(a, Math.min(b, c));
    }

    private static byte getByte(long addr) {
        return unsafe.getByte(addr);
    }

    private static short getShort(long addr) {
        return unsafe.getShort(addr);
    }

    private static int getInt(long addr) {
        return unsafe.getInt(addr);
    }

    private static long getLong(long addr) {
        return unsafe.getLong(addr);
    }

    private static void setByte(long addr, byte value) {
        unsafe.putByte(addr, value);
    }

    private static void setShort(long addr, short value) {
        unsafe.putShort(addr, value);
    }

    private static void setInt(long addr, int value) {
        unsafe.putInt(addr, value);
    }

    private static void setLong(long addr, long value) {
        unsafe.putLong(addr, value);
    }

    private static void copyMemory(long srcAddr, long dstAddr, long count) {
        unsafe.copyMemory(srcAddr, dstAddr, count);
    }

    private static record Chunk(long start, long end) {
    }

    private static class Table {
        public final long tallies;
        public final long[] addresses;
        public int count;

        public Table() {
            tallies = allocate(TABLE_SIZE).address();
            addresses = new long[NAME_LIMIT];
            count = 0;
        }

        public void process(Consumer consumer) {
            for (int i = 0; i < count; i++) {
                long address = addresses[i];
                int len = getByte(address + OFFSET_LEN);
                byte[] bytes = new byte[len];
                for (int j = 0; j < len; j++) {
                    bytes[j] = getByte(address + OFFSET_NAME + j);
                }
                String name = new String(bytes, StandardCharsets.UTF_8);
                long min = getShort(address + OFFSET_MIN);
                long max = getShort(address + OFFSET_MAX);
                long total = getLong(address + OFFSET_TOTAL);
                long count = getInt(address + OFFSET_COUNT);
                consumer.consume(name, min, max, total, count);
            }
        }
    }

    private static interface Consumer {
        public void consume(String name, long min, long max, long total, long count);
    }

    private static class Tally {

        private long min;
        private long max;
        private long total;
        private long count;

        public Tally() {
            this.min = Short.MAX_VALUE;
            this.max = Short.MIN_VALUE;
            this.total = 0;
            this.count = 0;
        }

        public void add(long addMin, long addMax, long addTotal, long addCount) {
            min = Math.min(min, addMin);
            max = Math.max(max, addMax);
            total += addTotal;
            count += addCount;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public long getTotal() {
            return total;
        }

        public long getCount() {
            return count;
        }

        public String toString() {
            return String.format("%.1f/%.1f/%.1f",
                    getMin() / 10.0,
                    getTotal() / (10.0 * getCount()),
                    getMax() / 10.0);
        }
    }
}
