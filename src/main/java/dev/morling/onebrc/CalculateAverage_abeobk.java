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
import java.util.TreeMap;
import sun.misc.Unsafe;

public class CalculateAverage_abeobk {
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

    // stat
    private static class Stat {
        private int min;
        private int max;
        private long sum;
        private int count;

        Stat(int v) {
            sum = min = max = v;
            count = 1;
        }

        void add(int val) {
            min = Math.min(val, min);
            max = Math.max(val, max);
            sum += val;
            count++;
        }

        void merge(Stat other) {
            min = Math.min(other.min, min);
            max = Math.max(other.max, max);
            sum += other.sum;
            count += other.count;
        }

        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min * 0.1, sum * 0.1 / count, max * 0.1);
        }
    }

    static class Node {
        long addr;
        int keylen;
        int hash;
        long[] buf = new long[13];
        Stat stat;

        String key() {
            byte[] buf = new byte[MAX_STR_LEN];
            UNSAFE.copyMemory(null, addr, buf, Unsafe.ARRAY_BYTE_BASE_OFFSET, keylen);
            return new String(buf, 0, keylen, StandardCharsets.UTF_8);
        }

        Node(long a, int kl, int h, int v, long[] b) {
            stat = new Stat(v);
            addr = a;
            keylen = kl;
            hash = h;
            System.arraycopy(b, 0, buf, 0, Math.ceilDiv(kl, 8));
        }

        boolean contentEquals(final long[] other_buf) {
            int k = keylen / 8;
            int r = keylen % 8;
            // Since the city name is most likely shorter than 16 characters
            // this should be faster than typical conditional checks
            long sum = 0;
            for (int i = 0; i < k; i++) {
                sum += buf[i] ^ other_buf[i];
            }
            sum += (buf[k] ^ other_buf[k]) & HASH_MASKS[r];
            return sum == 0;
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

    public static void main(String[] args) throws InterruptedException, IOException {
        int cpu_cnt = Runtime.getRuntime().availableProcessors();
        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long start_addr = file.map(MapMode.READ_ONLY, 0, file.size(), Arena.global()).address();
            long file_size = file.size();
            long end_addr = start_addr + file_size;
            long chunk_size = Math.ceilDiv(file_size, cpu_cnt);

            // processing
            var threads = new Thread[cpu_cnt];
            var maps = new Node[cpu_cnt][];
            var ptrs = slice(start_addr, end_addr, chunk_size, cpu_cnt);

            for (int i = 0; i < cpu_cnt; i++) {
                int thread_id = i;
                long start = ptrs[i];
                long end = ptrs[i + 1];
                maps[i] = new Node[BUCKET_SIZE + 16]; // extra space for collisions

                (threads[i] = new Thread(() -> {
                    long addr = start;
                    var map = maps[thread_id];
                    long[] buf = new long[13];
                    // parse loop
                    while (addr < end) {
                        int idx = 0;
                        long hash = 0;
                        long word = 0;
                        long row_addr = addr;
                        int semi_pos = 8;
                        while (semi_pos == 8) {
                            word = UNSAFE.getLong(addr);
                            buf[idx++] = word;
                            // idea from thomaswue & royvanrijn
                            long xor_semi = word ^ 0x3b3b3b3b3b3b3b3bL; // xor with ;;;;;;;;
                            long semipos_code = (xor_semi - 0x0101010101010101L) & ~xor_semi & 0x8080808080808080L;
                            semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                            addr += semi_pos;
                            hash ^= word & HASH_MASKS[semi_pos];
                        }

                        int hash32 = (int) (hash ^ (hash >>> 31));
                        int keylen = (int) (addr - row_addr);

                        // great idea from merykitty (Quan Anh Mai)
                        long num_word = UNSAFE.getLong(++addr);
                        int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                        addr += (dot_pos >>> 3) + 3;

                        int shift = 28 - dot_pos;
                        long signed = (~num_word << 59) >> 63;
                        long dsmask = ~(signed & 0xFF);
                        long digits = ((num_word & dsmask) << shift) & 0x0F000F0F00L;
                        long abs_val = ((digits * 0x640a0001) >>> 32) & 0x3FF;
                        int val = (int) ((abs_val ^ signed) - signed);

                        int bucket = (hash32 & BUCKET_MASK);
                        while (true) {
                            var node = map[bucket];
                            if (node == null) {
                                map[bucket] = new Node(row_addr, keylen, hash32, val, buf);
                                break;
                            }
                            if (node.keylen == keylen && node.hash == hash32 && node.contentEquals(buf)) {
                                node.stat.add(val);
                                break;
                            }
                            bucket++;
                        }
                    }
                })).start();
            }

            // join all
            for (var thread : threads)
                thread.join();

            // collect results
            TreeMap<String, Stat> ms = new TreeMap<>();
            for (var map : maps) {
                for (var node : map) {
                    if (node == null)
                        continue;
                    var stat = ms.putIfAbsent(node.key(), node.stat);
                    if (stat != null)
                        stat.merge(node.stat);
                }
            }

            System.out.println(ms);
        }
    }
}