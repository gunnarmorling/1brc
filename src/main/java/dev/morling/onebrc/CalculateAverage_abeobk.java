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
import java.util.Arrays;
import java.util.TreeMap;
import sun.misc.Unsafe;

public class CalculateAverage_abeobk {
    private static final boolean SHOW_ANALYSIS = false;

    private static final String FILE = "./measurements.txt";
    private static final int BUCKET_SIZE = 1 << 16;
    private static final int BUCKET_MASK = BUCKET_SIZE - 1;
    private static final int MAX_STR_LEN = 100;
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

    static class Node {
        long addr;
        long word0;
        long tail;
        int keylen;
        int min, max;
        int count;
        long sum;

        String key() {
            byte[] sbuf = new byte[MAX_STR_LEN];
            UNSAFE.copyMemory(null, addr, sbuf, Unsafe.ARRAY_BYTE_BASE_OFFSET, keylen);
            return new String(sbuf, 0, keylen, StandardCharsets.UTF_8);
        }

        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min * 0.1, sum * 0.1 / count, max * 0.1);
        }

        Node(long a, long t, int val, int kl) {
            addr = a;
            tail = t;
            keylen = kl;
            sum = min = max = val;
            count = 1;
        }

        Node(long a, long t, int val, int kl, long w0) {
            this(a, t, val, kl);
            word0 = w0;
        }

        void add(int val) {
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

        void merge(Node other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        boolean contentEquals(long other_addr, long other_tail) {
            if (tail != other_tail) // compare tail & length at the same time
                return false;
            // this is faster than comparision if key is short
            long xsum = 0;
            int n = keylen & 0xF8;
            for (int i = 0; i < n; i += 8) {
                xsum |= (UNSAFE.getLong(addr + i) ^ UNSAFE.getLong(other_addr + i));
            }
            return xsum == 0;
        }
    }

    // split into chunks
    static long[] slice(long start_addr, long end_addr, long chunk_size, int cpu_cnt) {
        long[] ptrs = new long[cpu_cnt + 1];
        ptrs[0] = start_addr;
        for (int i = 1; i < cpu_cnt; i++) {
            long addr = start_addr + i * chunk_size;
            while (addr < end_addr && UNSAFE.getByte(addr++) != '\n')
                ;
            ptrs[i] = Math.min(addr, end_addr);
        }
        ptrs[cpu_cnt] = end_addr;
        return ptrs;
    }

    // idea from royvanrijn
    static final long getSemiPosCode(final long word) {
        long xor_semi = word ^ 0x3b3b3b3b3b3b3b3bL; // xor with ;;;;;;;;
        return (xor_semi - 0x0101010101010101L) & (~xor_semi & 0x8080808080808080L);
    }

    // speed/collision balance
    static final int xxh32(long hash) {
        final int p1 = 0x85EBCA77; // prime
        int low = (int) hash;
        int high = (int) (hash >>> 33);
        int h = (low * p1) ^ high;
        return h ^ (h >>> 17);
    }

    // great idea from merykitty (Quan Anh Mai)
    static final int parseNum(long num_word, int dot_pos) {
        int shift = 28 - dot_pos;
        long signed = (~num_word << 59) >> 63;
        long dsmask = ~(signed & 0xFF);
        long digits = ((num_word & dsmask) << shift) & 0x0F000F0F00L;
        long abs_val = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        return (int) ((abs_val ^ signed) - signed);
    }

    // optimize for contest
    // save as much slow memory access as possible
    // about 50% key < 8chars, 25% key bettween 8-10 chars
    // keylength histogram (%) = [0, 0, 0, 0, 4, 10, 21, 15, 13, 11, 6, 6, 4, 2...
    static final Node[] parse(int thread_id, long start, long end, int[] cls) {
        long addr = start;
        var map = new Node[BUCKET_SIZE + 10000]; // extra space for collisions
        // parse loop
        while (addr < end) {
            long row_addr = addr;
            long tail = 0;
            long hash = 0;
            int val = 0;
            int bucket = 0;

            long word0 = UNSAFE.getLong(addr);
            long semipos_code = getSemiPosCode(word0);

            // about 50% chance key < 8 chars
            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                addr += semi_pos;
                tail = (word0 & HASH_MASKS[semi_pos]);
                bucket = xxh32(tail) & BUCKET_MASK;
                long num_word = UNSAFE.getLong(++addr);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                val = parseNum(num_word, dot_pos);
                addr += (dot_pos >>> 3) + 3;
                while (true) {
                    var node = map[bucket];
                    if (node == null) {
                        map[bucket] = new Node(row_addr, tail, val, semi_pos);
                        break;
                    }
                    if (node.tail == tail) {
                        node.add(val);
                        break;
                    }
                    bucket++;
                    if (SHOW_ANALYSIS)
                        cls[thread_id]++;
                }
                continue;
            }

            hash ^= word0;
            addr += 8;
            long word = UNSAFE.getLong(addr);
            semipos_code = getSemiPosCode(word);
            // 43% chance
            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                addr += semi_pos;
                tail = (word & HASH_MASKS[semi_pos]);
                hash ^= tail;
                bucket = xxh32(hash) & BUCKET_MASK;
                int keylen = (int) (addr - row_addr);
                long num_word = UNSAFE.getLong(++addr);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                val = parseNum(num_word, dot_pos);
                addr += (dot_pos >>> 3) + 3;

                while (true) {
                    var node = map[bucket];
                    if (node == null) {
                        map[bucket] = new Node(row_addr, tail, val, keylen, word0);
                        break;
                    }
                    if (node.word0 == word0 && node.tail == tail) {
                        node.add(val);
                        break;
                    }
                    bucket++;
                    if (SHOW_ANALYSIS)
                        cls[thread_id]++;
                }
                continue;
            }

            while (semipos_code == 0) {
                hash ^= word;
                addr += 8;
                word = UNSAFE.getLong(addr);
                semipos_code = getSemiPosCode(word);
            }

            int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
            addr += semi_pos;
            tail = (word & HASH_MASKS[semi_pos]);
            hash ^= tail;
            bucket = xxh32(hash) & BUCKET_MASK;
            int keylen = (int) (addr - row_addr);

            long num_word = UNSAFE.getLong(++addr);

            int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
            val = parseNum(num_word, dot_pos);
            addr += (dot_pos >>> 3) + 3;

            while (true) {
                var node = map[bucket];
                if (node == null) {
                    map[bucket] = new Node(row_addr, tail, val, keylen);
                    break;
                }
                if (node.contentEquals(row_addr, tail)) {
                    node.add(val);
                    break;
                }
                bucket++;
                if (SHOW_ANALYSIS)
                    cls[thread_id]++;
            }
        }
        return map;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long start_addr = file.map(MapMode.READ_ONLY, 0, file.size(), Arena.global()).address();
            long file_size = file.size();
            long end_addr = start_addr + file_size;

            // only use all cpus on large file
            int cpu_cnt = file_size < 1e6 ? 1 : Runtime.getRuntime().availableProcessors();
            long chunk_size = Math.ceilDiv(file_size, cpu_cnt);

            // processing
            var threads = new Thread[cpu_cnt];
            var maps = new Node[cpu_cnt][];
            var ptrs = slice(start_addr, end_addr, chunk_size, cpu_cnt);

            int[] cls = new int[cpu_cnt]; // collision
            int[] lenhist = new int[64]; // length histogram

            for (int i = 0; i < cpu_cnt; i++) {
                int thread_id = i;
                (threads[thread_id] = new Thread(() -> {
                    maps[thread_id] = parse(thread_id, ptrs[thread_id], ptrs[thread_id + 1], cls);
                })).start();
            }

            // join all
            for (var thread : threads)
                thread.join();

            if (SHOW_ANALYSIS) {
                for (int i = 0; i < cpu_cnt; i++) {
                    System.out.println("thread-" + i + " collision = " + cls[i]);
                }
            }

            // collect results
            TreeMap<String, Node> ms = new TreeMap<>();
            for (var map : maps) {
                for (var node : map) {
                    if (node == null)
                        continue;
                    if (SHOW_ANALYSIS) {
                        int kl = node.keylen & (lenhist.length - 1);
                        lenhist[kl] += node.count;
                    }
                    var stat = ms.putIfAbsent(node.key(), node);
                    if (stat != null)
                        stat.merge(node);
                }
            }

            if (SHOW_ANALYSIS) {
                System.out.println("total=" + Arrays.stream(lenhist).sum());
                System.out.println("length_histogram = "
                        + Arrays.toString(Arrays.stream(lenhist).map(x -> (int) (x * 1.0e-7)).toArray()));
            }
            else
                System.out.println(ms);
        }
    }

}