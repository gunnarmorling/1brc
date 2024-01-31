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
import java.io.PrintStream;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_armandino {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final int NUM_CHUNKS = Math.max(8, Runtime.getRuntime().availableProcessors());
    private static final int INITIAL_MAP_CAPACITY = 8192;
    private static final byte SEMICOLON = 59;
    private static final byte NL = 10;
    private static final int PRIME = 1117;

    private static final int KEY_OFFSET = 0, // 100b
            HASH_OFFSET = 100, // int
            KEY_LENGTH_OFFSET = 104, // short
            MIN_OFFSET = 106, // short
            MAX_OFFSET = 108, // short
            COUNT_OFFSET = 110, // int
            SUM_OFFSET = 114; // long

    private static final long ENTRY_SIZE = 100 // key: offset=0
            + 4 // keyHash: offset=100
            + 2 // keyLength: offset=104
            + 2 // min: 108; offset=106
            + 2 // max: 110; offset=108
            + 4 // count: 114; offset=110
            + 8; // sum: 122; offset=118

    private static final Unsafe UNSAFE = getUnsafe();

    public static void main(String[] args) throws Exception {
        var channel = FileChannel.open(FILE, StandardOpenOption.READ);

        Chunk[] chunks = split(channel);
        ChunkProcessor[] processors = new ChunkProcessor[chunks.length];

        for (int i = 0; i < processors.length; i++) {
            processors[i] = new ChunkProcessor(chunks[i].start, chunks[i].end);
            processors[i].start();
        }

        Map<String, Stats> results = new TreeMap<>();

        for (int i = 0; i < processors.length; i++) {
            processors[i].join();
            final long end = processors[i].map.mapEnd;

            for (long addr = processors[i].map.mapStart; addr < end; addr += ENTRY_SIZE) {
                final short keyLength = UNSAFE.getShort(addr + KEY_LENGTH_OFFSET);

                if (keyLength == 0)
                    continue;

                final byte[] keyBytes = new byte[keyLength];
                UNSAFE.copyMemory(null, addr, keyBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, keyLength);
                final short min = UNSAFE.getShort(addr + MIN_OFFSET);
                final short max = UNSAFE.getShort(addr + MAX_OFFSET);
                final int count = UNSAFE.getInt(addr + COUNT_OFFSET);
                final long sum = UNSAFE.getLong(addr + SUM_OFFSET);
                final Stats s = new Stats(new String(keyBytes, 0, keyLength, UTF_8), min, max, count, sum);
                results.merge(s.key, s, CalculateAverage_armandino::mergeStats);
            }
        }

        print(results.values());
    }

    private static Stats mergeStats(final Stats x, final Stats y) {
        x.min = Math.min(x.min, y.min);
        x.max = Math.max(x.max, y.max);
        x.count += y.count;
        x.sum += y.sum;
        return x;
    }

    private static class ChunkProcessor extends Thread {
        private final UnsafeMap map = new UnsafeMap(INITIAL_MAP_CAPACITY);

        final long chunkStart;
        final long chunkEnd;

        private ChunkProcessor(long chunkStart, long chunkEnd) {
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
        }

        @Override
        public void run() {
            long i = chunkStart;
            while (i < chunkEnd) {
                final long keyAddress = i;
                int keyHash = 0;
                byte b;

                while ((b = UNSAFE.getByte(i++)) != SEMICOLON) {
                    keyHash = PRIME * keyHash + b;
                }

                final short keyLength = (short) (i - keyAddress - 1);
                final long numberWord = UNSAFE.getLong(i);
                final int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000);
                final short measurement = parseNumber(decimalSepPos, numberWord);
                final int addOffset = (decimalSepPos >>> 3) + 3;
                i += addOffset;

                map.addEntry(keyHash, keyAddress, keyLength, measurement);
            }
        }

        // credit: merykitty
        private static short parseNumber(int decimalSepPos, long numberWord) {
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
            return (short) ((absValue ^ signed) - signed);
        }
    }

    private static class Stats {
        private final String key;
        private int min;
        private int max;
        private int count;
        private long sum;

        Stats(final String key, final int min, final int max, final int count, final long sum) {
            this.min = min;
            this.max = max;
            this.count = count;
            this.sum = sum;
            this.key = key;
        }

        void print(final PrintStream out) {
            out.print(key);
            out.print('=');
            out.print(round(min / 10f));
            out.print('/');
            out.print(round((sum / 10f) / count));
            out.print('/');
            out.print(round(max) / 10f);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static void print(final Collection<Stats> sorted) {
        int size = sorted.size();
        System.out.print('{');
        for (Stats stats : sorted) {
            stats.print(System.out);
            if (--size > 0) {
                System.out.print(", ");
            }
        }
        System.out.println('}');
    }

    private static Chunk[] split(final FileChannel channel) throws IOException {
        final long fileSize = channel.size();
        long start = channel.map(READ_ONLY, 0, fileSize, Arena.global()).address();
        final long endAddress = start + fileSize;
        if (fileSize < 10000) {
            return new Chunk[]{ new Chunk(start, endAddress) };
        }

        final long chunkSize = fileSize / NUM_CHUNKS;
        final var chunks = new Chunk[NUM_CHUNKS];
        long end = start + chunkSize;

        for (int i = 0; i < NUM_CHUNKS; i++) {
            if (i > 0) {
                start = chunks[i - 1].end;
                end = Math.min(start + chunkSize, endAddress);
            }
            if (end < endAddress) {
                while (UNSAFE.getByte(end) != NL) {
                    end++;
                }
                end++;
            }
            chunks[i] = new Chunk(start, end);
        }
        return chunks;
    }

    private record Chunk(long start, long end) {
    }

    private static Unsafe getUnsafe() {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            return (Unsafe) unsafe.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class UnsafeMap {

        long mapStart;
        long mapEnd;
        int capacity; // num entries

        UnsafeMap(int numEntries) {
            capacity = numEntries;
            final long size = ENTRY_SIZE * numEntries;
            mapStart = UNSAFE.allocateMemory(size);
            mapEnd = mapStart + size;
            UNSAFE.setMemory(mapStart, size, (byte) 0);
        }

        void addEntry(final int keyHash, final long keyAddress, final short keyLength, final short measurement) {
            final int pos = (capacity - 1) & keyHash;

            long addr = mapStart + pos * ENTRY_SIZE;
            int hash = UNSAFE.getInt(addr + HASH_OFFSET);

            if (hash == 0) { // new entry
                initEntry(addr, keyAddress, keyLength, measurement, keyHash);
                return;
            }
            if (hash == keyHash && keysEqual(addr, keyAddress, keyLength)) {
                updateEntry(addr, measurement);
                return;
            }

            // this can be improved to avoid clustering at the start.
            // should only affect the 10k test
            addr = mapStart;

            while (addr < mapEnd) {
                addr += ENTRY_SIZE;
                hash = UNSAFE.getInt(addr + HASH_OFFSET);

                if (hash == 0) {
                    initEntry(addr, keyAddress, keyLength, measurement, keyHash);
                    return;
                }
                if (hash == keyHash && keysEqual(addr, keyAddress, keyLength)) {
                    updateEntry(addr, measurement);
                    return;
                }
            }

            resize(keyHash, keyAddress, keyLength, measurement);
        }

        private void resize(final int keyHash, final long keyAddress, final short keyLength, final short measurement) {
            UnsafeMap newMap = new UnsafeMap(capacity * 2);

            for (long addr = mapStart; addr < mapEnd; addr += ENTRY_SIZE) {
                final short oKeyLength = UNSAFE.getShort(addr + KEY_LENGTH_OFFSET);
                final int oKeyHsh = UNSAFE.getInt(addr + HASH_OFFSET);
                final short oMin = UNSAFE.getShort(addr + MIN_OFFSET);
                final short oMax = UNSAFE.getShort(addr + MAX_OFFSET);
                final int oCount = UNSAFE.getInt(addr + COUNT_OFFSET);
                final long oSum = UNSAFE.getLong(addr + SUM_OFFSET);

                final int newPos = (newMap.capacity - 1) & oKeyHsh;
                long newAddr = newMap.mapStart + newPos * ENTRY_SIZE;

                UNSAFE.putShort(newAddr + KEY_LENGTH_OFFSET, oKeyLength);
                UNSAFE.putInt(newAddr + HASH_OFFSET, oKeyHsh);
                UNSAFE.putShort(newAddr + MIN_OFFSET, oMin);
                UNSAFE.putShort(newAddr + MAX_OFFSET, oMax);
                UNSAFE.putInt(newAddr + COUNT_OFFSET, oCount);
                UNSAFE.putLong(newAddr + SUM_OFFSET, oSum);
            }

            newMap.addEntry(keyHash, keyAddress, keyLength, measurement);

            this.mapStart = newMap.mapStart;
            this.mapEnd = newMap.mapEnd;
            this.capacity = newMap.capacity;
        }

        private static void initEntry(final long entry, final long keyAddress, final short keyLength, final short measurement, final int keyHash) {
            UNSAFE.copyMemory(keyAddress, entry, keyLength);
            UNSAFE.putInt(entry + HASH_OFFSET, keyHash);
            UNSAFE.putShort(entry + KEY_LENGTH_OFFSET, keyLength);
            UNSAFE.putShort(entry + MIN_OFFSET, Short.MAX_VALUE);
            UNSAFE.putShort(entry + MAX_OFFSET, Short.MIN_VALUE);

            updateEntry(entry, measurement);
        }

        private static void updateEntry(final long entry, final short measurement) {
            UNSAFE.putShort(entry + MIN_OFFSET,
                    (short) Math.min(UNSAFE.getShort(entry + MIN_OFFSET), measurement));
            UNSAFE.putShort(entry + MAX_OFFSET,
                    (short) Math.max(UNSAFE.getShort(entry + MAX_OFFSET), measurement));
            UNSAFE.putInt(entry + COUNT_OFFSET,
                    UNSAFE.getInt(entry + COUNT_OFFSET) + 1);
            UNSAFE.putLong(entry + SUM_OFFSET,
                    UNSAFE.getLong(entry + SUM_OFFSET) + measurement);
        }
    }

    private static boolean keysEqual(long key1Address, long key2Address, final int keyLength) {
        // credit: abeobk
        long xsum = 0;
        int n = keyLength & 0xF8;
        for (int i = 0; i < n; i += 8) {
            xsum |= (UNSAFE.getLong(key1Address + i) ^ UNSAFE.getLong(key2Address + i));
        }
        return xsum == 0;
    }
}
