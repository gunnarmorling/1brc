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
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

import sun.misc.Unsafe;

public class CalculateAverage_yasyf {
    private static final int CORES = Runtime.getRuntime().availableProcessors() * 2;
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

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
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
        int mean = (int) Math.round((float) metrics[2] / metrics[3]);

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
            for (int i = 0; i < length; i++) {
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
                addCity(name, length, key, key2, offset + 1, measurement);
                return;
            }

            metrics[0] = Math.min(metrics[0], measurement);
            metrics[1] = Math.max(metrics[1], measurement);
            metrics[2] = metrics[2] + measurement;
            metrics[3] = metrics[3] + 1;
        }

        private final void parse() {
            int trueStart = 0, trueEnd = (int) (end - start);
            byte b = 0;
            int pos = 0;
            long limit = buff.byteSize();

            if (start != 0) {
                while (pos < limit) {
                    trueStart++;
                    if ((b = UNSAFE.getByte(address + pos++)) == '\n') {
                        break;
                    }
                }
            }

            pos = trueEnd;
            while (pos < limit) {
                trueEnd++;
                if ((b = UNSAFE.getByte(address + pos++)) == '\n') {
                    break;
                }
            }

            pos = trueStart;
            while (pos < trueEnd) {
                int key = 0, key2 = 0, measurement = 0, len = 0, sign = 1;
                long city = address + pos;

                while ((b = UNSAFE.getByte(address + pos++)) != ';') {
                    len++;
                    // https://stackoverflow.com/a/2351171
                    key = key * 31 + b;
                    key2 = key2 * 37 + b;
                }

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
            }
        }

        @Override
        public final void run() {
            try {
                this.buff = this.channel.map(MapMode.READ_ONLY, start,
                        Math.min(end - start + OVERSHOOT, channel.size() - start), Arena.global());
                this.address = buff.address();
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
