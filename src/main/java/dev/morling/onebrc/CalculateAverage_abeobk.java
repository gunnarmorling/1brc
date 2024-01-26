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
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import sun.misc.Unsafe;

public class CalculateAverage_abeobk {
    private static final boolean SHOW_ANALYSIS = false;
    private static final int CPU_CNT = Runtime.getRuntime().availableProcessors();

    private static final String FILE = "./measurements.txt";
    private static final int BUCKET_SIZE = 1 << 16;
    private static final int BUCKET_MASK = BUCKET_SIZE - 1;
    private static final int MAX_STR_LEN = 100;
    private static final int MAX_STATIONS = 10000;
    private static final long CHUNK_SZ = 1 << 22; // 4MB chunk
    private static final Unsafe UNSAFE = initUnsafe();
    private static final long[] HASH_MASKS = new long[]{
            0x0L,
            0xffL,
            0xffffL,
            0xffffffL,
            0xffffffffL,
            0xffffffffffL,
            0xffffffffffffL,
            0xffffffffffffffL,
            0xffffffffffffffffL, };

    private static AtomicInteger chunk_id = new AtomicInteger(0);
    private static int chunk_cnt;
    private static long start_addr, end_addr;
    private static Stat[][] all_res;

    private static final void debug(String s, Object... args) {
        System.out.println(String.format(s, args));
    }

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (Exception ex) {
            throw new RuntimeException();
        }
    }

    static class Stat {
        Node node;
        String key;

        public final String toString() {
            return (node.min / 10.0) + "/"
                    + (Math.round(((double) node.sum / node.count)) / 10.0) + "/"
                    + (node.max / 10.0);
        }

        Stat(Node n) {
            node = n;
            byte[] sbuf = new byte[MAX_STR_LEN];
            long word = UNSAFE.getLong(n.addr);
            long semipos_code = getSemiPosCode(word);
            int keylen = 0;
            while (semipos_code == 0) {
                keylen += 8;
                word = UNSAFE.getLong(n.addr + keylen);
                semipos_code = getSemiPosCode(word);
            }
            keylen += Long.numberOfTrailingZeros(semipos_code) >>> 3;
            UNSAFE.copyMemory(null, n.addr, sbuf, Unsafe.ARRAY_BYTE_BASE_OFFSET, keylen);
            key = new String(sbuf, 0, keylen, StandardCharsets.UTF_8);
        }
    }

    static class Node {
        long addr;
        long word0;
        long tail;
        long sum;
        int count;
        short min, max;

        Node(long a, long t, short val) {
            addr = a;
            tail = t;
            sum = min = max = val;
            count = 1;
        }

        Node(long a, long w0, long t, short val) {
            addr = a;
            word0 = w0;
            tail = t;
            sum = min = max = val;
            count = 1;
        }

        final void add(short val) {
            sum += val;
            count++;
            if (val >= max) {
                max = val;
                return;
            }
            if (val < min) {
                min = val;
            }
        }

        final void merge(Node other) {
            sum += other.sum;
            count += other.count;
            if (other.max > max) {
                max = other.max;
            }
            if (other.min < min) {
                min = other.min;
            }
        }

        final boolean contentEquals(long other_addr, long other_word0, long other_tail, int keylen) {
            if (word0 != other_word0 || tail != other_tail)
                return false;
            // this is faster than comparision if key is short
            long xsum = 0;
            int n = keylen & 0xF8;
            for (int i = 8; i < n; i += 8) {
                xsum |= (UNSAFE.getLong(addr + i) ^ UNSAFE.getLong(other_addr + i));
            }
            return xsum == 0;
        }
    }

    // idea from royvanrijn
    static final long getSemiPosCode(final long word) {
        long xor_semi = word ^ 0x3b3b3b3b3b3b3b3bL; // xor with ;;;;;;;;
        return (xor_semi - 0x0101010101010101L) & (~xor_semi & 0x8080808080808080L);
    }

    // speed/collision balance
    static final int xxh32(long hash) {
        long h = hash * 37;
        return (int) (h ^ (h >>> 29));
    }

    // great idea from merykitty (Quan Anh Mai)
    static final short parseNum(long num_word, int dot_pos) {
        int shift = 28 - dot_pos;
        long signed = (~num_word << 59) >> 63;
        long dsmask = ~(signed & 0xFF);
        long digits = ((num_word & dsmask) << shift) & 0x0F000F0F00L;
        long abs_val = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        return (short) ((abs_val ^ signed) - signed);
    }

    // Thread pool worker
    static final class Worker extends Thread {
        final int thread_id;

        Worker(int i) {
            thread_id = i;
            this.start();
        }

        @Override
        public void run() {
            var map = new Node[BUCKET_SIZE + MAX_STATIONS]; // extra space for collisions
            int cnt = 0;
            int id;
            int cls = 0;

            // process in small chunk to maintain disk locality (artsiomkorzun trick)
            // but keep going instead of merging
            while ((id = chunk_id.getAndIncrement()) < chunk_cnt) {
                long addr = start_addr + id * CHUNK_SZ;
                long end = Math.min(addr + CHUNK_SZ, end_addr);
                // adjust start
                if (id > 0) {
                    while (UNSAFE.getByte(addr++) != '\n')
                        ;
                }

                // parse loop
                // optimize for contest
                // save as much slow memory access as possible
                // about 50% key < 8chars, 25% key bettween 8-10 chars
                // keylength histogram (%) = [0, 0, 0, 0, 4, 10, 21, 15, 13, 11, 6, 6, 4, 2...
                while (addr < end) {
                    long row_addr = addr;

                    long word0 = UNSAFE.getLong(addr);
                    long semipos_code = getSemiPosCode(word0);

                    // about 50% chance key < 8 chars
                    if (semipos_code != 0) {
                        int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                        addr += semi_pos + 1;
                        long num_word = UNSAFE.getLong(addr);
                        int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                        addr += (dot_pos >>> 3) + 3;

                        long tail = word0 & HASH_MASKS[semi_pos];
                        int bucket = xxh32(tail) & BUCKET_MASK;
                        short val = parseNum(num_word, dot_pos);

                        while (true) {
                            var node = map[bucket];
                            if (node == null) {
                                map[bucket] = new Node(row_addr, tail, val);
                                cnt++;
                                break;
                            }
                            if (node.tail == tail) {
                                node.add(val);
                                break;
                            }
                            bucket++;
                            if (SHOW_ANALYSIS)
                                cls++;
                        }
                        continue;
                    }

                    addr += 8;
                    long word = UNSAFE.getLong(addr);
                    semipos_code = getSemiPosCode(word);
                    // 43% chance
                    if (semipos_code != 0) {
                        int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                        addr += semi_pos + 1;
                        long num_word = UNSAFE.getLong(addr);
                        int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                        addr += (dot_pos >>> 3) + 3;

                        long tail = (word & HASH_MASKS[semi_pos]);
                        int bucket = xxh32(word0 ^ tail) & BUCKET_MASK;
                        short val = parseNum(num_word, dot_pos);

                        while (true) {
                            var node = map[bucket];
                            if (node == null) {
                                map[bucket] = new Node(row_addr, word0, tail, val);
                                cnt++;
                                break;
                            }
                            if (node.word0 == word0 && node.tail == tail) {
                                node.add(val);
                                break;
                            }
                            bucket++;
                            if (SHOW_ANALYSIS)
                                cls++;
                        }
                        continue;
                    }

                    // why not going for more? tested, slower
                    long hash = word0;
                    while (semipos_code == 0) {
                        hash ^= word;
                        addr += 8;
                        word = UNSAFE.getLong(addr);
                        semipos_code = getSemiPosCode(word);
                    }

                    int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                    addr += semi_pos;
                    int keylen = (int) (addr - row_addr);
                    long num_word = UNSAFE.getLong(addr + 1);
                    int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                    addr += (dot_pos >>> 3) + 4;

                    long tail = (word & HASH_MASKS[semi_pos]);
                    int bucket = xxh32(hash ^ tail) & BUCKET_MASK;
                    short val = parseNum(num_word, dot_pos);

                    while (true) {
                        var node = map[bucket];
                        if (node == null) {
                            map[bucket] = new Node(row_addr, word0, tail, val);
                            cnt++;
                            break;
                        }
                        if (node.contentEquals(row_addr, word0, tail, keylen)) {
                            node.add(val);
                            break;
                        }
                        bucket++;
                        if (SHOW_ANALYSIS)
                            cls++;
                    }
                }
            }

            if (SHOW_ANALYSIS) {
                debug("Thread %d collision = %d", thread_id, cls);
            }

            Stat[] stats = new Stat[cnt];
            int i = 0;
            for (var node : map) {
                if (node != null) {
                    stats[i++] = new Stat(node);
                }
            }
            all_res[thread_id] = stats;
        }
    }

    // thomaswue trick
    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder()
                .command(workerCommand)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        // thomaswue trick
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }

        var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
        long file_size = file.size();
        start_addr = file.map(MapMode.READ_ONLY, 0, file.size(), Arena.global()).address();
        end_addr = start_addr + file_size;

        // only use all cpus on large file
        int cpu_cnt = file_size < 1e6 ? 1 : CPU_CNT;
        chunk_cnt = (int) Math.ceilDiv(file_size, CHUNK_SZ);
        all_res = new Stat[cpu_cnt][];

        List<Worker> workers = IntStream.range(0, cpu_cnt).mapToObj(i -> new Worker(i)).toList();
        for (var w : workers)
            w.join();

        // collect all results
        TreeMap<String, Stat> ms = new TreeMap<>();
        for (var res : all_res) {
            for (var s : res) {
                var stat = ms.putIfAbsent(s.key, s);
                if (stat != null)
                    stat.node.merge(s.node);
            }
        }

        // print output
        System.out.println(ms);
        System.out.close();
    }
}