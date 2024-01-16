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
}

class Aggregator {
    private static final int MAX_STATIONS = 10_000;
    private static final int MAX_STATION_SIZE = (100 * 4) / 8 + 5;
    private static final int INDEX_SIZE = 1024 * 1024;
    private static final int INDEX_MASK = INDEX_SIZE - 1;
    private static final int FLD_MAX = 0;
    private static final int FLD_MIN = 1;
    private static final int FLD_SUM = 2;
    private static final int FLD_COUNT = 3;

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
            int hash = 0;
            while (true) {
                // This is a bit ugly, but it is faster than reading by byte.
                long tmpLong = buf.getLong(pos);
                if ((tmpLong & 0xFF) == ';') {
                    break;
                }
                if (((tmpLong >>> 8) & 0xFF) == ';') {
                    hash = (33 * hash) ^ (int) (tmpLong & 0xFF);
                    pos += 1;
                    break;
                }
                if (((tmpLong >>> 16) & 0xFF) == ';') {
                    hash = (33 * hash) ^ (int) (tmpLong & 0xFFFF);
                    pos += 2;
                    break;
                }
                if (((tmpLong >>> 24) & 0xFF) == ';') {
                    hash = (33 * hash) ^ (int) (tmpLong & 0xFFFFFF);
                    pos += 3;
                    break;
                }
                if (((tmpLong >>> 32) & 0xFF) == ';') {
                    hash = (33 * hash) ^ (int) (tmpLong & 0xFFFFFFFF);
                    pos += 4;
                    break;
                }
                if (((tmpLong >>> 40) & 0xFF) == ';') {
                    hash = ((33 * hash) ^ (int) (tmpLong & 0xFFFFFFFF)) + (int) ((tmpLong >>> 33) & 0xFF);
                    pos += 5;
                    break;
                }
                if (((tmpLong >>> 48) & 0xFF) == ';') {
                    hash = ((33 * hash) ^ (int) (tmpLong & 0xFFFFFFFF)) + (int) ((tmpLong >>> 33) & 0xFFFF);
                    pos += 6;
                    break;
                }
                if (((tmpLong >>> 56) & 0xFF) == ';') {
                    hash = ((33 * hash) ^ (int) (tmpLong & 0xFFFFFFFF)) + (int) ((tmpLong >>> 33) & 0xFFFFFF);
                    pos += 7;
                    break;
                }
                hash = ((33 * hash) ^ (int) (tmpLong & 0xFFFFFFFF)) + (int) ((tmpLong >>> 33) & 0xFFFFFFFF);
                pos += 8;
            }
            hash = (33 * hash) ^ (hash >>> 15);
            int len = pos - start;
            assert (buf.get(pos) == ';') : "Expected ';'";
            pos++;

            int measurement;
            {
                long tmpLong = buf.getLong(pos);
                int sign = 1;
                if ((tmpLong & 0xFF) == '-') {
                    sign = -1;
                    tmpLong >>>= 8;
                    pos++;
                }
                int value;
                if (((tmpLong >>> 8) & 0xFF) == '.') {
                    value = (int) (((tmpLong & 0xFF) - '0') * 10 + (((tmpLong >>> 16) & 0xFF) - '0'));
                    pos += 4;
                }
                else {
                    value = (int) (((tmpLong & 0xFF) - '0') * 100 + (((tmpLong >>> 8) & 0xFF) - '0') * 10 + (((tmpLong >>> 24) & 0xFF) - '0'));
                    pos += 5;
                }
                measurement = sign * value;
            }
            assert (buf.get(pos - 1) == '\n') : "Expected '\\n'";

            add(buf, start, len, hash, measurement);
        }

        return this;
    }

    public Stream<Entry> stream() {
        return Arrays.stream(index)
                .filter(offset -> offset != 0)
                .mapToObj(offset -> new Entry(mem, offset));
    }

    private void add(ByteBuffer buf, int start, int len, int hash, int measurement) {
        int idx = hash & INDEX_MASK;
        while (true) {
            if (index[idx] != 0) {
                int offset = index[idx];
                if (keyEqual(offset, buf, start, len)) {
                    int pos = offset + (len >> 3) + 2;
                    mem[pos + FLD_MIN] = Math.min((int) measurement, (int) mem[pos + FLD_MIN]);
                    mem[pos + FLD_MAX] = Math.max((int) measurement, (int) mem[pos + FLD_MAX]);
                    mem[pos + FLD_SUM] += measurement;
                    mem[pos + FLD_COUNT] += 1;
                    return;
                }
            }
            else {
                index[idx] = create(buf, start, len, hash, measurement);
                return;
            }
            idx = (idx + 1) & INDEX_MASK;
        }
    }

    private int create(ByteBuffer buf, int start, int len, int hash, int measurement) {
        int offset = memUsed;

        mem[offset] = len;

        int memPos = offset + 1;
        int memEndEarly = memPos + (len >> 3);
        int bufPos = start;
        int bufEnd = start + len;
        while (memPos < memEndEarly) {
            mem[memPos] = buf.getLong(bufPos);
            memPos += 1;
            bufPos += 8;
        }
        if (bufPos < bufEnd) {
            int shift = (8 - (len & 7)) << 3; // (8 - (len % 8)) * 8
            long tmpLong = buf.getLong(bufPos) << shift >>> shift;
            mem[memPos] = tmpLong;
        }
        else {
            // "consume" extra long - makes math a bit simpler to calculate
            // fields offset for update.
            mem[memPos] = 0;
        }

        memPos += 1;
        mem[memPos + FLD_MIN] = measurement;
        mem[memPos + FLD_MAX] = measurement;
        mem[memPos + FLD_SUM] = measurement;
        mem[memPos + FLD_COUNT] = 1;
        memUsed = memPos + 4;

        return offset;
    }

    private boolean keyEqual(int offset, ByteBuffer buf, int start, int len) {
        if (len != mem[offset]) {
            return false;
        }
        int memPos = offset + 1;
        int memEndEarly = memPos + (len >> 3);
        int bufPos = start;
        int bufEnd = start + len;
        while (memPos < memEndEarly) {
            if (mem[memPos] != buf.getLong(bufPos)) {
                return false;
            }
            memPos += 1;
            bufPos += 8;
        }
        if (bufPos < bufEnd) {
            int shift = (8 - (len & 7)) << 3; // (8 - (len % 8)) * 8
            long tmpLong = buf.getLong(bufPos) << shift >>> shift;
            if (mem[memPos] != tmpLong) {
                return false;
            }
        }
        return true;
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
                int keyLen = (int) mem[pos++];
                var tmpBuf = ByteBuffer.allocate(keyLen + 8).order(ByteOrder.nativeOrder());
                for (int i = 0; i < keyLen; i += 8) {
                    tmpBuf.putLong(mem[pos++]);
                }
                key = new String(tmpBuf.array(), 0, keyLen, StandardCharsets.UTF_8);
            }
            return key;
        }

        public Entry add(Entry other) {
            int keyLen = (int) mem[offset];
            int fldOffset = (keyLen >> 3) + 2;
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
            int keyLen = (int) mem[offset];
            int pos = offset + (keyLen >> 3) + 2;
            return round(mem[pos + FLD_MIN])
                    + "/" + round(((double) mem[pos + FLD_SUM]) / mem[pos + FLD_COUNT])
                    + "/" + round(mem[pos + FLD_MAX]);
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }
    }
}
