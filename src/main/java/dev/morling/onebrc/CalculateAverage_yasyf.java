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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

public class CalculateAverage_yasyf {
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final int NAME = 100;
    private static final int N_CITIES = 10_000;
    private static final int MAP_SIZE = 1024 * 128;
    private static final int METRICS_SIZE = 6; // min + max + sum + count + length + key2
    private static final int OVERSHOOT = 128;

    // Static allocations:
    private static final Path FILE = Path.of("./measurements.txt");
    private static final BufferedWriter OUT = new BufferedWriter(new OutputStreamWriter(System.out));
    private static final Worker[] WORKERS = new Worker[CORES];
    private static final Thread[] THREADS = new Thread[CORES];
    private static final TreeMap<String, long[]> RESULTS = new TreeMap<>();
    private static final Unsafe UNSAFE;
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        }
        catch (Exception ex) {
            throw new Error(ex);
        }
    }

    private static final void printNum(long num) throws IOException {
        float sign = Math.signum(num);
        long abs = Math.abs(num);
        long whole = abs / 10;
        long fraction = abs % 10;
        if (sign < 0) {
            OUT.write("-");
        }
        OUT.write(whole + "." + fraction);
    }

    private static final void printCity(String name, long[] metrics) throws IOException {
        int mean = (int) Math.round((double) metrics[2] / metrics[3]);

        OUT.write(name);
        OUT.write("=");
        printNum(metrics[0]);
        OUT.write("/");
        printNum(mean);
        OUT.write("/");
        printNum(metrics[1]);
    }

    static final class Worker implements Runnable {
        private final long[][] metrics = new long[MAP_SIZE][METRICS_SIZE];
        private final int[] names = new int[MAP_SIZE];
        private final int[] indices = new int[N_CITIES];
        private short n_indices = 0;

        private final FileChannel channel;
        private MemorySegment buff;
        private long address;

        private final long start;
        private final long end;

        public Worker(FileChannel channel, long start, long end) {
            this.channel = channel;
            this.start = start;
            this.end = end;
        }

        private final boolean mismatch(long a, long b, int length) {
            int i = 0;
            for (; i < length; i++) {
                if (UNSAFE.getByte(a + i) != UNSAFE.getByte(b + i)) {
                    return true;
                }
            }
            return false;
        }

        private final void addCity(long name, int length, int key, int key2, int offset, int measurement) {
            // https://stackoverflow.com/a/6670766

            int idx = ((key & (MAP_SIZE - 1)) + offset) % MAP_SIZE;
            long[] metrics = this.metrics[idx];

            if (metrics[3] == 0) {
                metrics[0] = metrics[1] = metrics[2] = measurement;
                metrics[3] = 1;
                metrics[4] = length;
                metrics[5] = key2;
                names[idx] = (int) (name - address);
                indices[n_indices++] = idx;
                return;
            }
            else if (metrics[4] != length || metrics[5] != key2 || mismatch(name, names[idx] + address, length)) {
                // collision
                addCity(name, length, rehash(key2), rehash(key), offset + 1, measurement);
                return;
            }

            metrics[0] = Math.min(metrics[0], measurement);
            metrics[1] = Math.max(metrics[1], measurement);
            metrics[2] = metrics[2] + measurement;
            metrics[3] = metrics[3] + 1;
        }

        private final long parseOneMeasurement(long start, long city, int len, int key, int key2) {
            int measurement = 0, sign = 1;
            long pos = start;
            byte b = 0;

            if ((b = UNSAFE.getByte(address + pos++)) == '-') {
                sign = -1;
            }
            else {
                measurement = b - '0';
            }

            while ((b = UNSAFE.getByte(address + pos++)) != '\n') {
                if (b != '.') {
                    measurement = (measurement * 10) + (b - '0');
                }
            }

            addCity(city, len, key, key2, 0, measurement * sign);
            return pos;
        }

        private final byte[] _name = new byte[NAME];

        private final long parseOne(long start) {
            int key = 0, key2 = 0, len = 0;
            long pos = start;
            long city = address + pos;
            byte b;

            while ((b = UNSAFE.getByte(address + pos++)) != ';') {
                len++;
                _name[len - 1] = b;
            }

            key = key * 31 + _name[0];
            key2 = _name[0] << 24;
            if (len > 1) {
                key = key * 31 + _name[1];
                key2 += (_name[1] << 16);
            }
            if (len > 2) {
                key = key * 31 + _name[len - 2];
                key2 += (_name[len - 2] << 8);

                key = key * 31 + _name[len - 1];
                key2 += _name[len - 1];
            }
            key = key * 31 + len;
            key2 ^= len;

            return parseOneMeasurement(pos, city, len, key, hash2(key2));
        }

        private final int hash2(int x) {
            x ^= x >> 16;
            x *= 0x7feb352d;
            x ^= x >> 15;
            x *= 0x846ca68b;
            x ^= x >> 16;
            return x;
        }

        static int rehash(int x) {
            x = ((x >>> 16) ^ x) * 0x45d9f3b;
            x = ((x >>> 16) ^ x) * 0x45d9f3b;
            x = (x >>> 16) ^ x;
            return x;
        }

        private final void parse() {
            long trueStart = 0, trueEnd = (end - start);
            long pos = 0;
            long limit = buff.byteSize();

            if (start != 0) {
                while (pos < limit) {
                    trueStart++;
                    if (UNSAFE.getByte(address + pos++) == '\n') {
                        break;
                    }
                }
            }

            pos = trueEnd;
            while (pos < limit) {
                trueEnd++;
                if (UNSAFE.getByte(address + pos++) == '\n') {
                    break;
                }
            }

            pos = trueStart;
            int size = SPECIES.length();

            while (pos + size < trueEnd) {
                final ByteVector bytes = ByteVector.fromMemorySegment(SPECIES, buff, pos, ByteOrder.nativeOrder());
                long city = address + pos;

                long semicolon = bytes.compare(VectorOperators.EQ, ';').toLong();
                int first = Long.numberOfTrailingZeros(semicolon);
                if (first >= size - 1 || first < 3) {
                    // Name too long or short
                    pos = parseOne(pos);
                    continue;
                }
                pos += first + 1;
                int length = first;

                // var mask = SPECIES.indexInRange(0, length).toVector();
                // var ints = bytes.and(mask).reinterpretAsInts();
                // int hash = ints.lanewise(VectorOperators.LSHR, 16)
                // .lanewise(VectorOperators.XOR, ints)
                // .mul(0x7feb352d) // https://github.com/skeeto/hash-prospector
                // .reduceLanes(VectorOperators.XOR);

                char name_start = UNSAFE.getChar(city);
                byte name_1 = (byte) (name_start);
                byte name_2 = (byte) (name_start >> 8);

                long data = UNSAFE.getLong(address + pos - 3);
                // last 2 chars of city, try to hash and if it fails, then

                byte name_3 = (byte) (data);
                byte name_4 = (byte) ((data >> 8));
                // byte 3 is ":"

                int key = 0;
                key = key * 31 + name_1;
                key = key * 31 + name_2;
                key = key * 31 + name_3;
                key = key * 31 + name_4;
                key = key * 31 + length;

                int key2 = 0;
                key2 += (name_1 << 24);
                key2 += (name_2 << 16);
                key2 += (name_3 << 8);
                key2 += name_4;
                key2 ^= length;
                key2 = hash2(key2);

                byte byte_1 = (byte) ((data >> 24));
                int mul = byte_1 == '-' ? -1 : 1;

                byte byte_2 = (byte) ((data >> 32));
                byte byte_3 = (byte) ((data >> 40));
                byte byte_4 = (byte) ((data >> 48));
                byte byte_5 = (byte) ((data >> 56));

                int measurement;
                if (byte_2 == '.') { // 1 digit, no neg
                    measurement = (byte_1 - '0') * 10 + (byte_3 - '0');
                    pos += 3;
                }
                else if (byte_3 == '.' && mul == 1) { // 2 digit, no neg
                    measurement = (byte_1 - '0') * 100 + (byte_2 - '0') * 10 + (byte_4 - '0');
                    pos += 4;
                }
                else if (byte_3 == '.') { // 1 digit, neg
                    measurement = (byte_2 - '0') * 10 + (byte_4 - '0');
                    pos += 4;
                }
                else { // 2 digit, neg
                    measurement = (byte_2 - '0') * 100 + (byte_3 - '0') * 10 + (byte_5 - '0');
                    pos += 5;
                }
                pos++; // skip newline
                addCity(city, length, key, key2, 0, measurement * mul);
            }

            while (pos < trueEnd) {
                pos = parseOne(pos);
            }
        }

        @Override
        public final void run() {
            try {
                this.buff = this.channel.map(MapMode.READ_ONLY, start,
                        Math.min(end - start + OVERSHOOT, channel.size() - start), Arena.global());
                this.address = buff.address();
                this.buff.load();
                parse();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final static long[] mergeResults(long[] m1, long[] m2) {
        return new long[]{
                Math.min(m1[0], m2[0]),
                Math.max(m1[1], m2[1]),
                m1[2] + m2[2],
                m1[3] + m2[3],
                m1[4]
        };
    }

    private final static void merge() {
        for (int idx = 0; idx < N_CITIES; idx++) {
            for (int i = 0; i < CORES; i++) {
                if (idx >= WORKERS[i].n_indices) {
                    continue;
                }
                int cityIdx = WORKERS[i].indices[idx];
                long[] metrics = WORKERS[i].metrics[cityIdx];
                byte[] name = new byte[(int) metrics[4]];
                UNSAFE.copyMemory(null, WORKERS[i].names[cityIdx] + WORKERS[i].address, name,
                        Unsafe.ARRAY_BYTE_BASE_OFFSET,
                        metrics[4]);
                RESULTS.merge(new String(name), metrics, CalculateAverage_yasyf::mergeResults);
            }
        }
    }

    public final static void printResults() throws IOException {
        boolean first = true;
        OUT.write("{");
        for (java.util.Map.Entry<String, long[]> entry : RESULTS.entrySet()) {
            if (first) {
                first = false;
            }
            else {
                OUT.write(", ");
            }
            long[] value = entry.getValue();
            printCity(entry.getKey(), value);
        }
        OUT.write("}\n");
        OUT.flush();
    }

    public final static void main(String[] args) throws IOException, InterruptedException {
        FileChannel channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ);
        long size = channel.size();

        for (int i = 0; i < CORES; i++) {
            long start = (i * size / CORES);
            long end = ((i + 1) * size / CORES);
            WORKERS[i] = new Worker(channel, start, end);
            THREADS[i] = Thread.ofPlatform().start(WORKERS[i]);
        }

        for (int i = 0; i < CORES; i++) {
            THREADS[i].join();
        }

        merge();
        printResults();
    }
}
