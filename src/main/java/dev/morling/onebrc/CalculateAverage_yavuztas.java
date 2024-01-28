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

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;

public class CalculateAverage_yavuztas {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final Unsafe UNSAFE = unsafe();

    // I compared all three: MappedByteBuffer, MemorySegment and Unsafe.
    // Accessing the memory using Unsafe is still the fastest in my experience.
    // However, I would never use it in production, single programming error will crash your app.
    private static Unsafe unsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract bytes from a long
     */
    private static long partial(long word, int length) {
        final long mask = (~0L) << (length << 3);
        return word & (~mask);
    }

    // Only one object, both for measurements and keys, less object creation in hotpots is always faster
    static class Record {

        long start; // memory address of the underlying data
        int length;
        long word1;
        long word2;
        long wordLast;
        int hash;
        Record next; // linked list to resolve hash collisions

        private int min; // calculations over int is faster than double, we convert to double in the end only once
        private int max;
        private long sum;
        private int count;

        public Record(long start, int length, long word1, long word2, long wordLast, int hash, int temp) {
            this.start = start;
            this.length = length;
            this.word1 = word1;
            this.word2 = word2;
            this.wordLast = wordLast;
            this.hash = hash;
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        @Override
        public boolean equals(Object o) {
            final Record record = (Record) o;
            return equals(record.start, record.word1, record.word2, record.wordLast, record.length);
        }

        private static boolean equalsComparingLongs(long start1, long last1, long start2, long last2, int length) {
            int step = 16; // starting from 4th long
            length -= step;
            while (length >= 8) { // scan longs
                if (UNSAFE.getLong(start1 + step) != UNSAFE.getLong(start2 + step)) {
                    return false;
                }
                length -= 8;
                step += 8; // 8 bytes
            }
            return last1 == last2;
        }

        public boolean equals(long start, long word1, long word2, long last, int length) {

            if (this.word1 != word1) {
                return false;
            }
            if (this.word2 != word2) {
                return false;
            }

            // equals check is done by comparing longs instead of byte by byte check, this is faster
            return equalsComparingLongs(this.start, this.wordLast, start, last, length);
        }

        @Override
        public String toString() {
            final byte[] bytes = new byte[this.length];
            UNSAFE.copyMemory(null, this.start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, this.length);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        public void collect(int temp) {
            if (temp < this.min)
                this.min = temp;
            if (temp > this.max)
                this.max = temp;
            this.sum += temp;
            this.count++;
        }

        public void merge(Record that) {
            if (that.min < this.min)
                this.min = that.min;
            if (that.max > this.max)
                this.max = that.max;
            this.sum += that.sum;
            this.count += that.count;
        }

        public String measurements() {
            // here is only executed once for each unique key, so StringBuilder creation doesn't harm
            final StringBuilder sb = new StringBuilder(14);
            sb.append(round(this.min)).append("/");
            sb.append(round(1.0 * this.sum / this.count)).append("/");
            sb.append(round(this.max));
            return sb.toString();
        }
    }

    // Inspired by @spullara - customized hashmap on purpose
    // The main difference is we hold only one array instead of two, fewer objects is faster
    static class RecordMap {

        // Bigger bucket size less collisions, but you have to find a sweet spot otherwise it is becoming slower.
        // Also works good enough for 10K stations
        static final int SIZE = 1 << 14; // 16kb - enough for 10K
        static final int BITMASK = SIZE - 1;
        Record[] keys = new Record[SIZE];

        static int hashBucket(int hash) {
            hash = hash ^ (hash >>> 16); // naive bit spreading but surprisingly decreases collision :)
            return hash & BITMASK; // fast modulo, to find bucket
        }

        void putAndCollect(int hash, int temp, long start, int length, long word1, long word2, long wordLast) {
            final int bucket = hashBucket(hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                this.keys[bucket] = new Record(start, length, word1, word2, wordLast, hash, temp);
                return;
            }

            if (!existing.equals(start, word1, word2, wordLast, length)) { // collision
                // find possible slot by scanning the slot linked list
                while (existing.next != null && !existing.next.equals(start, word1, word2, wordLast, length)) {
                    existing = existing.next; // go on to next
                }
                if (existing.next == null) {
                    existing.next = new Record(start, length, word1, word2, wordLast, hash, temp);
                    return;
                }
                existing = existing.next;
            }
            existing.collect(temp);
        }

        void putOrMerge(Record key) {
            final int bucket = hashBucket(key.hash);
            Record existing = this.keys[bucket];
            if (existing == null) {
                key.next = null;
                this.keys[bucket] = key;
                return;
            }

            if (!existing.equals(key)) { // collision
                // find possible slot by scanning the slot linked list
                while (existing.next != null && !existing.next.equals(key)) {
                    existing = existing.next;
                }
                if (existing.next == null) {
                    key.next = null;
                    existing.next = key;
                    return;
                }
                existing = existing.next;
            }
            existing.merge(key);
        }

        void forEach(Consumer<Record> consumer) {
            int pos = 0;
            Record key;
            while (pos < SIZE) {
                if ((key = this.keys[pos++]) == null) {
                    continue;
                }
                Record next = key.next;
                consumer.accept(key);
                while (next != null) { // also traverse the records in collision list
                    final Record tmp = next.next;
                    consumer.accept(next);
                    next = tmp;
                }
            }
        }

        void merge(RecordMap other) {
            other.forEach(this::putOrMerge);
        }

    }

    // One actor for one thread, no synchronization
    static class RegionActor extends Thread {

        final long startPos; // start of region memory address
        final int size;

        final RecordMap map = new RecordMap();

        public RegionActor(long startPos, int size) {
            this.startPos = startPos;
            this.size = size;
        }

        static long getWord(long address) {
            return UNSAFE.getLong(address);
        }

        static long getWord2(long address) {
            return UNSAFE.getLong(address + 8);
        }

        // hasvalue & haszero
        // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
        public static long hasSemicolon(long word) {
            // semicolon pattern
            long SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
            final long hasVal = word ^ SEMICOLON; // hasvalue
            return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L); // haszero
        }

        public static int semicolonPos(long hasVal) {
            return Long.numberOfTrailingZeros(hasVal) >>> 3;
        }

        static int decimalPos(long numberWord) {
            return Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        }

        @Override
        public void run() {
            long s; // semicolon check word
            int pos; // semicolon position
            long hash; // key hash
            long word1; // first word to read
            long word2; // second word to read
            long word; // the rest between 3 - 13. Max key size 13 longs - 13*8 = 104 > 100
            long pointer = this.startPos;
            final long size = pointer + this.size;
            while (pointer < size) { // line start
                hash = 0; // reset hash
                word1 = getWord(pointer);
                if ((s = hasSemicolon(word1)) != 0) {
                    pos = semicolonPos(s);
                    // read temparature
                    final long numberWord = getWord(pointer + pos + 1);
                    final int decimalPos = decimalPos(numberWord);
                    final int temp = convertIntoNumber(decimalPos, numberWord);

                    word1 = partial(word1, pos); // last word
                    this.map.putAndCollect(completeHash(hash, word1), temp, pointer, pos, word1, 0, 0);

                    pointer += pos + (decimalPos >>> 3) + 4; // seek to the line end
                }
                else {
                    int length = 8;
                    word2 = getWord2(pointer);
                    if ((s = hasSemicolon(word2)) != 0) {
                        pos = semicolonPos(s);
                        length += pos;
                        // read temparature
                        final long numberWord = getWord(pointer + length + 1);
                        final int decimalPos = decimalPos(numberWord);
                        final int temp = convertIntoNumber(decimalPos, numberWord);

                        word2 = partial(word2, pos); // last word
                        this.map.putAndCollect(completeHash(hash, word1, word2), temp, pointer, length, word1, word2, 0);

                        pointer += length + (decimalPos >>> 3) + 4; // seek to the line end
                    }
                    else {
                        length = 16;
                        hash = appendHash(hash, word1, word2);
                        while ((s = hasSemicolon((word = getWord(pointer + length)))) == 0) {
                            hash = appendHash(hash, word);
                            length += 8;
                        }

                        pos = semicolonPos(s);
                        length += pos;
                        // read temparature
                        final long numberWord = getWord(pointer + length + 1);
                        final int decimalPos = decimalPos(numberWord);
                        final int temp = convertIntoNumber(decimalPos, numberWord);

                        word = partial(word, pos); // last word
                        this.map.putAndCollect(completeHash(hash, word), temp, pointer, length, word1, word2, word);

                        pointer += length + (decimalPos >>> 3) + 4; // seek to the line end
                    }
                }
            }
        }

        // Hashes are calculated by a Mersenne Prime (1 << 7) -1
        // This is faster than multiplication in some machines
        static long appendHash(long hash, long word) {
            return (hash << 7) - hash + word;
        }

        static long appendHash(long hash, long word1, long word2) {
            hash = (hash << 7) - hash + word1;
            return (hash << 7) - hash + word2;
        }

        static int completeHash(long hash, long partial) {
            hash = (hash << 7) - hash + partial;
            return (int) (hash ^ (hash >>> 25));
        }

        static int completeHash(long hash, long word1, long word2) {
            hash = (hash << 7) - hash + word1;
            hash = (hash << 7) - hash + word2;
            return (int) hash ^ (int) (hash >>> 25);
        }

        // Credits to @merrykitty. Magical solution to parse temparature values branchless!
        // Taken as without modification, comments belong to @merrykitty
        private static int convertIntoNumber(int decimalSepPos, long numberWord) {
            final int shift = 28 - decimalSepPos;
            // signed is -1 if negative, 0 otherwise
            final long signed = (~numberWord << 59) >> 63;
            final long designMask = ~(signed & 0xFF);
            // Align the number to a specific position and transform the ascii code
            // to actual digit value in each byte
            final long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;
            // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
            // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
            // 0x000000UU00TTHH00 +
            // 0x00UU00TTHH000000 * 10 +
            // 0xUU00TTHH00000000 * 100
            // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
            // This results in our value lies in the bit 32 to 41 of this product
            // That was close :)
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            final long value = (absValue ^ signed) - signed;
            return (int) value;
        }

        /**
         * blocks until the map is fully collected
         */
        RecordMap get() throws InterruptedException {
            join();
            return this.map;
        }
    }

    private static double round(double value) {
        return Math.round(value) / 10.0;
    }

    /**
     * Scans the given buffer to the left
     */
    private static long findClosestLineEnd(long start, int size) {
        long position = start + size;
        while (UNSAFE.getByte(--position) != '\n') {
            // read until a linebreak
            size--;
        }
        return size;
    }

    private static boolean isWorkerProcess(String[] args) {
        return Arrays.asList(args).contains("--worker");
    }

    private static void runAsWorker() throws Exception {
        final ProcessHandle.Info info = ProcessHandle.current().info();
        final List<String> commands = new ArrayList<>();
        info.command().ifPresent(commands::add);
        info.arguments().ifPresent(args -> commands.addAll(Arrays.asList(args)));
        commands.add("--worker");

        new ProcessBuilder()
                .command(commands)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    public static void main(String[] args) throws Exception {

        // Dased on @thomaswue's idea, to cut unmapping delay.
        // Strangely, unmapping delay doesn't occur on macOS/M1 however in Linux/AMD it's substantial - ~200ms
        if (!isWorkerProcess(args)) {
            runAsWorker();
            return;
        }

        var concurrency = 2 * Runtime.getRuntime().availableProcessors();
        final long fileSize = Files.size(FILE);
        long regionSize = fileSize / concurrency;

        // handling extreme cases
        while (regionSize > Integer.MAX_VALUE) {
            concurrency *= 2;
            regionSize /= 2;
        }
        if (fileSize <= 1 << 20) { // small file (1mb), no need concurrency
            concurrency = 1;
            regionSize = fileSize;
        }

        long startPos = 0;
        final FileChannel channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ);
        // get the memory address, this is the only thing we need for Unsafe
        final long memoryAddress = channel.map(FileChannel.MapMode.READ_ONLY, startPos, fileSize, Arena.global()).address();

        final RegionActor[] actors = new RegionActor[concurrency];
        for (int i = 0; i < concurrency; i++) {
            // calculate boundaries
            long maxSize = (startPos + regionSize > fileSize) ? fileSize - startPos : regionSize;
            // shift position to back until we find a linebreak
            maxSize = findClosestLineEnd(memoryAddress + startPos, (int) maxSize);

            final RegionActor region = (actors[i] = new RegionActor(memoryAddress + startPos, (int) maxSize));
            region.start(); // start processing

            startPos += maxSize;
        }

        final RecordMap output = new RecordMap(); // output to merge all records
        for (RegionActor actor : actors) {
            final RecordMap partial = actor.get(); // blocks until get the result
            output.merge(partial);
        }

        // sort and print the result
        final TreeMap<String, String> sorted = new TreeMap<>();
        output.forEach(key -> {
            sorted.put(key.toString(), key.measurements());
        });
        System.out.println(sorted);
        System.out.close(); // closing the stream will trigger the main process to pick up the output early
    }

}
