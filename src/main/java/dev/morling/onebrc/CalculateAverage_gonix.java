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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_gonix {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {

        var file = new RandomAccessFile(FILE, "r");

        var res = buildChunks(file).stream().parallel()
                .flatMap(chunk -> new Aggregator().processChunk(chunk).stream())
                .collect(Collectors.toMap(
                        Aggregator.Entry::getKey,
                        Aggregator.Entry::getValue,
                        Aggregator.Entry::add,
                        TreeMap::new));

        System.out.println(res);
        System.out.close();
    }

    private static List<MappedByteBuffer> buildChunks(RandomAccessFile file) throws IOException {
        var fileSize = file.length();
        var chunkSize = Math.min(Integer.MAX_VALUE - 512, fileSize / Runtime.getRuntime().availableProcessors());
        if (chunkSize <= 0) {
            chunkSize = fileSize;
        }
        var chunks = new ArrayList<MappedByteBuffer>((int) (fileSize / chunkSize) + 1);
        var start = 0L;
        while (start < fileSize) {
            var pos = start + chunkSize;
            if (pos < fileSize) {
                file.seek(pos);
                while (file.read() != '\n') {
                    pos += 1;
                }
                pos += 1;
            }
            else {
                pos = fileSize;
            }
            var buf = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, pos - start);
            buf.order(ByteOrder.nativeOrder());
            chunks.add(buf);
            start = pos;
        }
        return chunks;
    }

    private static class Aggregator {
        private static final int MAX_STATIONS = 10_000;
        private static final int MAX_STATION_SIZE = Math.ceilDiv(100, 8) + 5;
        private static final int INDEX_SIZE = 1024 * 1024;
        private static final int INDEX_MASK = INDEX_SIZE - 1;
        private static final int FLD_COUNT = 0;
        private static final int FLD_SUM = 1;
        private static final int FLD_MIN = 2;
        private static final int FLD_MAX = 3;

        // Poor man's hash map: hash code to offset in `mem`.
        private final int[] index;

        // Contiguous storage of key (station name) and stats fields of all
        // unique stations.
        // The idea here is to improve locality so that stats fields would
        // possibly be already in the CPU cache after we are done comparing
        // the key.
        private final long[] mem;
        private int memUsed;

        Aggregator() {
            assert ((INDEX_SIZE & (INDEX_SIZE - 1)) == 0) : "INDEX_SIZE must be power of 2";
            assert (INDEX_SIZE > MAX_STATIONS) : "INDEX_SIZE must be greater than MAX_STATIONS";

            index = new int[INDEX_SIZE];
            mem = new long[1 + (MAX_STATIONS * MAX_STATION_SIZE)];
            memUsed = 1;
        }

        Aggregator processChunk(MappedByteBuffer buf) {
            // To avoid checking if it is safe to read a whole long near the
            // end of a chunk, we copy last couple of lines to a padded buffer
            // and process that part separately.
            int limit = buf.limit();
            int pos = Math.max(limit - 16, -1);
            while (pos >= 0 && buf.get(pos) != '\n') {
                pos--;
            }
            pos++;
            if (pos > 0) {
                processChunkLongs(buf, pos);
            }
            int tailLen = limit - pos;
            var tailBuf = ByteBuffer.allocate(tailLen + 8).order(ByteOrder.nativeOrder());
            buf.get(pos, tailBuf.array(), 0, tailLen);
            processChunkLongs(tailBuf, tailLen);
            return this;
        }

        Aggregator processChunkLongs(ByteBuffer buf, int limit) {
            int pos = 0;
            while (pos < limit) {

                int start = pos;
                long keyLong = buf.getLong(pos);
                long valueSepMark = valueSepMark(keyLong);
                if (valueSepMark != 0) {
                    int tailBits = tailBits(valueSepMark);
                    pos += valueOffset(tailBits);
                    // assert (UNSAFE.getByte(pos - 1) == ';') : "Expected ';' (1), pos=" + (pos - startAddr);
                    long tailAndLen = tailAndLen(tailBits, keyLong, pos - start - 1);

                    long valueLong = buf.getLong(pos);
                    int decimalSepMark = decimalSepMark(valueLong);
                    pos += nextKeyOffset(decimalSepMark);
                    // assert (UNSAFE.getByte(pos - 1) == '\n') : "Expected '\\n' (1), pos=" + (pos - startAddr);
                    int measurement = decimalValue(decimalSepMark, valueLong);

                    add1(buf, start, tailAndLen, hash(hash1(tailAndLen)), measurement);
                    continue;
                }

                pos += 8;
                long keyLong1 = keyLong;
                keyLong = buf.getLong(pos);
                valueSepMark = valueSepMark(keyLong);
                if (valueSepMark != 0) {
                    int tailBits = tailBits(valueSepMark);
                    pos += valueOffset(tailBits);
                    // assert (UNSAFE.getByte(pos - 1) == ';') : "Expected ';' (2), pos=" + (pos - startAddr);
                    long tailAndLen = tailAndLen(tailBits, keyLong, pos - start - 1);

                    long valueLong = buf.getLong(pos);
                    int decimalSepMark = decimalSepMark(valueLong);
                    pos += nextKeyOffset(decimalSepMark);
                    // assert (UNSAFE.getByte(pos - 1) == '\n') : "Expected '\\n' (2), pos=" + (pos - startAddr);
                    int measurement = decimalValue(decimalSepMark, valueLong);

                    add2(buf, start, keyLong1, tailAndLen, hash(hash(hash1(keyLong1), tailAndLen)), measurement);
                    continue;
                }

                long hash = hash1(keyLong1);
                do {
                    pos += 8;
                    hash = hash(hash, keyLong);
                    keyLong = buf.getLong(pos);
                    valueSepMark = valueSepMark(keyLong);
                } while (valueSepMark == 0);
                int tailBits = tailBits(valueSepMark);
                pos += valueOffset(tailBits);
                // assert (UNSAFE.getByte(pos - 1) == ';') : "Expected ';' (N), pos=" + (pos - startAddr);
                long tailAndLen = tailAndLen(tailBits, keyLong, pos - start - 1);
                hash = hash(hash, tailAndLen);

                long valueLong = buf.getLong(pos);
                int decimalSepMark = decimalSepMark(valueLong);
                pos += nextKeyOffset(decimalSepMark);
                // assert (UNSAFE.getByte(pos - 1) == '\n') : "Expected '\\n' (N), pos=" + (pos - startAddr);
                int measurement = decimalValue(decimalSepMark, valueLong);

                addN(buf, start, tailAndLen, hash(hash), measurement);
            }

            return this;
        }

        public Stream<Entry> stream() {
            return Arrays.stream(index)
                    .filter(offset -> offset != 0)
                    .mapToObj(offset -> new Entry(mem, offset));
        }

        private static long hash1(long value) {
            return value;
        }

        private static long hash(long hash, long value) {
            return hash ^ value;
        }

        private static int hash(long hash) {
            hash *= 0x9E3779B97F4A7C15L; // Fibonacci hashing multiplier
            return (int) (hash >>> 39);
        }

        private static long valueSepMark(long keyLong) {
            // Seen this trick used in multiple other solutions.
            // Nice breakdown here: https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
            long match = keyLong ^ 0x3B3B3B3B_3B3B3B3BL; // 3B == ';'
            match = (match - 0x01010101_01010101L) & (~match & 0x80808080_80808080L);
            return match;
        }

        private static int tailBits(long valueSepMark) {
            return Long.numberOfTrailingZeros(valueSepMark >>> 7);
        }

        private static int valueOffset(int tailBits) {
            return (int) (tailBits >>> 3) + 1;
        }

        private static long tailAndLen(int tailBits, long keyLong, long keyLen) {
            long tailMask = ~(-1L << tailBits);
            long tail = keyLong & tailMask;
            return (tail << 8) | ((keyLen >> 3) & 0xFF);
        }

        private static int decimalSepMark(long value) {
            // Seen this trick used in multiple other solutions.
            // Looks like the original author is @merykitty.

            // The 4th binary digit of the ascii of a digit is 1 while
            // that of the '.' is 0. This finds the decimal separator
            // The value can be 12, 20, 28
            return Long.numberOfTrailingZeros(~value & 0x10101000);
        }

        private static int decimalValue(int decimalSepMark, long value) {
            // Seen this trick used in multiple other solutions.
            // Looks like the original author is @merykitty.

            int shift = 28 - decimalSepMark;
            // signed is -1 if negative, 0 otherwise
            long signed = (~value << 59) >> 63;
            long designMask = ~(signed & 0xFF);
            // Align the number to a specific position and transform the ascii code
            // to actual digit value in each byte
            long digits = ((value & designMask) << shift) & 0x0F000F0F00L;

            // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
            // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
            // 0x000000UU00TTHH00 +
            // 0x00UU00TTHH000000 * 10 +
            // 0xUU00TTHH00000000 * 100
            // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
            // This results in our value lies in the bit 32 to 41 of this product
            // That was close :)
            long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            return (int) ((absValue ^ signed) - signed);
        }

        private static int nextKeyOffset(int decimalSepMark) {
            return (decimalSepMark >>> 3) + 3;
        }

        private void add1(ByteBuffer buf, int start, long tailAndLen, int hash, int measurement) {
            int idx = hash & INDEX_MASK;
            for (; index[idx] != 0; idx = (idx + 1) & INDEX_MASK) {
                if (update1(index[idx], tailAndLen, measurement)) {
                    return;
                }
            }
            index[idx] = create(buf, start, tailAndLen, measurement);
        }

        private void add2(ByteBuffer buf, int start, long keyLong, long tailAndLen, int hash, int measurement) {
            int idx = hash & INDEX_MASK;
            for (; index[idx] != 0; idx = (idx + 1) & INDEX_MASK) {
                if (update2(index[idx], keyLong, tailAndLen, measurement)) {
                    return;
                }
            }
            index[idx] = create(buf, start, tailAndLen, measurement);
        }

        private void addN(ByteBuffer buf, int start, long tailAndLen, int hash, int measurement) {
            int idx = hash & INDEX_MASK;
            for (; index[idx] != 0; idx = (idx + 1) & INDEX_MASK) {
                if (updateN(index[idx], buf, start, tailAndLen, measurement)) {
                    return;
                }
            }
            index[idx] = create(buf, start, tailAndLen, measurement);
        }

        private int create(ByteBuffer buf, int start, long tailAndLen, int measurement) {
            int offset = memUsed;

            mem[offset] = tailAndLen;

            int memPos = offset + 1;
            int memEnd = memPos + (int) (tailAndLen & 0xFF);
            int bufPos = start;
            while (memPos < memEnd) {
                mem[memPos] = buf.getLong(bufPos);
                memPos += 1;
                bufPos += 8;
            }

            mem[memPos + FLD_MIN] = measurement;
            mem[memPos + FLD_MAX] = measurement;
            mem[memPos + FLD_SUM] = measurement;
            mem[memPos + FLD_COUNT] = 1;
            memUsed = memPos + 4;

            return offset;
        }

        private boolean update1(int offset, long tailAndLen, int measurement) {
            if (mem[offset] != tailAndLen) {
                return false;
            }
            updateStats(offset + 1, measurement);
            return true;
        }

        private boolean update2(int offset, long keyLong, long tailAndLen, int measurement) {
            if (mem[offset] != tailAndLen || mem[offset + 1] != keyLong) {
                return false;
            }
            updateStats(offset + 2, measurement);
            return true;
        }

        private boolean updateN(int offset, ByteBuffer buf, int start, long tailAndLen, int measurement) {
            var mem = this.mem;
            if (mem[offset] != tailAndLen) {
                return false;
            }
            int memPos = offset + 1;
            int memEnd = memPos + (int) (tailAndLen & 0xFF);
            int bufPos = start;
            while (memPos < memEnd) {
                if (mem[memPos] != buf.getLong(bufPos)) {
                    return false;
                }
                memPos += 1;
                bufPos += 8;
            }
            updateStats(memPos, measurement);
            return true;
        }

        private void updateStats(int memPos, int measurement) {
            mem[memPos + FLD_COUNT] += 1;
            mem[memPos + FLD_SUM] += measurement;
            if (measurement < mem[memPos + FLD_MIN]) {
                mem[memPos + FLD_MIN] = measurement;
            }
            if (measurement > mem[memPos + FLD_MAX]) {
                mem[memPos + FLD_MAX] = measurement;
            }
        }

        public static class Entry {
            private final long[] mem;
            private final int offset;
            private String key;

            Entry(long[] mem, int offset) {
                this.mem = mem;
                this.offset = offset;
            }

            public String getKey() {
                if (key == null) {
                    int pos = this.offset;
                    long tailAndLen = mem[pos++];
                    int keyLen = (int) (tailAndLen & 0xFF);
                    var tmpBuf = ByteBuffer.allocate((keyLen << 3) + 8).order(ByteOrder.nativeOrder());
                    for (int i = 0; i < keyLen; i++) {
                        tmpBuf.putLong(mem[pos++]);
                    }
                    long tail = tailAndLen >>> 8;
                    tmpBuf.putLong(tail);
                    int keyLenBytes = (keyLen << 3) + 8 - (Long.numberOfLeadingZeros(tail) >> 3);
                    key = new String(tmpBuf.array(), 0, keyLenBytes, StandardCharsets.UTF_8);
                }
                return key;
            }

            public Entry add(Entry other) {
                int fldOffset = (int) (mem[offset] & 0xFF) + 1;
                int pos = offset + fldOffset;
                int otherPos = other.offset + fldOffset;
                long[] otherMem = other.mem;
                mem[pos + FLD_MIN] = Math.min((int) mem[pos + FLD_MIN], (int) otherMem[otherPos + FLD_MIN]);
                mem[pos + FLD_MAX] = Math.max((int) mem[pos + FLD_MAX], (int) otherMem[otherPos + FLD_MAX]);
                mem[pos + FLD_SUM] += otherMem[otherPos + FLD_SUM];
                mem[pos + FLD_COUNT] += otherMem[otherPos + FLD_COUNT];
                return this;
            }

            public Entry getValue() {
                return this;
            }

            @Override
            public String toString() {
                int pos = offset + (int) (mem[offset] & 0xFF) + 1;
                return round(mem[pos + FLD_MIN])
                        + "/" + round(((double) mem[pos + FLD_SUM]) / mem[pos + FLD_COUNT])
                        + "/" + round(mem[pos + FLD_MAX]);
            }

            private static double round(double value) {
                return Math.round(value) / 10.0;
            }
        }
    }

}
