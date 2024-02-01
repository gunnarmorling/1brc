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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import sun.misc.Unsafe;

public class CalculateAverage_abeobk {
    private static final int CPU_CNT = Runtime.getRuntime().availableProcessors();

    private static final String FILE = "./measurements.txt";
    private static final int BUCKET_SIZE = 1 << 16;
    private static final long BUCKET_MASK = BUCKET_SIZE - 1;
    private static final int MAX_STR_LEN = 100;
    private static final int MAX_STATIONS = 10000;
    private static final long CHUNK_SZ = 1 << 22;
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
    private static AtomicReference<Node[]> mapref = new AtomicReference<>(null);
    private static int chunk_cnt;
    private static long start_addr, end_addr;

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

    /*
     * MAIN FUNCTION
     */
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

        // spawn workers
        for (var w : IntStream.range(0, cpu_cnt).mapToObj(i -> new Worker(i)).toList()) {
            w.join();
        }

        // collect results
        TreeMap<String, Node> ms = new TreeMap<>();
        for (var crr : mapref.get()) {
            if (crr == null)
                continue;
            var prev = ms.putIfAbsent(crr.key(), crr);
            if (prev != null)
                prev.merge(crr);
        }
        // print result
        System.out.println(ms);
        System.out.close();
    }

    /*
     * HELPER FUNCTIONS
     */

    // Get semicolon pos code
    static final long getSemiCode(final long w) {
        long x = w ^ 0x3b3b3b3b3b3b3b3bL; // xor with ;;;;;;;;
        return (x - 0x0101010101010101L) & (~x & 0x8080808080808080L);
    }

    // Get new line pos code
    static final long getLFCode(final long w) {
        long x = w ^ 0x0A0A0A0A0A0A0A0AL; // xor with \n\n\n\n\n\n\n\n
        return (x - 0x0101010101010101L) & (~x & 0x8080808080808080L);
    }

    // Get decimal point pos code
    static final int getDotCode(final long w) {
        return Long.numberOfTrailingZeros(~w & 0x10101000);
    }

    // Convert semicolon pos code to position
    static final int getSemiPos(final long spc) {
        return Long.numberOfTrailingZeros(spc) >>> 3;
    }

    // Find next line address
    static final long nextLF(long addr) {
        long word = UNSAFE.getLong(addr);
        long lfpos_code = getLFCode(word);
        while (lfpos_code == 0) {
            addr += 8;
            word = UNSAFE.getLong(addr);
            lfpos_code = getLFCode(word);
        }
        return addr + (Long.numberOfTrailingZeros(lfpos_code) >>> 3) + 1;
    }

    // Parse number
    // great idea from merykitty (Quan Anh Mai)
    static final long num(long w, int d) {
        int shift = 28 - d;
        long signed = (~w << 59) >> 63;
        long dsmask = ~(signed & 0xFF);
        long digits = ((w & dsmask) << shift) & 0x0F000F0F00L;
        long abs_val = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        return ((abs_val ^ signed) - signed);
    }

    // Hash mixer
    static final long mix(long hash) {
        long h = hash * 37;
        return (h ^ (h >>> 29));
    }

    // Spawn worker (thomaswue trick
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

    final static class Node {
        long addr;
        long hash;
        long word0;
        long sum;
        long min, max;
        int keylen;
        int count;

        public final String toString() {
            return (min / 10.0) + "/"
                    + (Math.round(((double) sum / count)) / 10.0) + "/"
                    + (max / 10.0);
        }

        final String key() {
            byte[] sbuf = new byte[MAX_STR_LEN];
            UNSAFE.copyMemory(null, addr, sbuf, Unsafe.ARRAY_BYTE_BASE_OFFSET, keylen);
            return new String(sbuf, 0, (int) keylen, StandardCharsets.UTF_8);
        }

        Node(long a, long h, int kl, long v) {
            addr = a;
            min = max = v;
            keylen = kl;
            hash = h;
        }

        Node(long a, long h, int kl) {
            addr = a;
            hash = h;
            min = 999;
            max = -999;
            keylen = kl;
        }

        Node(long a, long w0, long h, int kl, long v) {
            addr = a;
            word0 = w0;
            hash = h;
            min = max = v;
            keylen = kl;
        }

        Node(long a, long w0, long h, int kl) {
            addr = a;
            word0 = w0;
            hash = h;
            min = 999;
            max = -999;
            keylen = kl;
        }

        final void add(long val) {
            sum += val;
            count++;
            if (val > max) {
                max = val;
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

        final boolean contentEquals(long other_addr, long other_word0, long other_hash, long kl) {
            if (word0 != other_word0 || hash != other_hash)
                return false;
            // this is faster than comparision if key is short
            long xsum = 0;
            long n = kl & 0xF8;
            for (long i = 8; i < n; i += 8) {
                xsum |= (UNSAFE.getLong(addr + i) ^ UNSAFE.getLong(other_addr + i));
            }
            return xsum == 0;
        }

        final boolean contentEquals(Node other) {
            if (hash != other.hash)
                return false;
            long n = keylen & 0xF8;
            for (long i = 0; i < n; i += 8) {
                if (UNSAFE.getLong(addr + i) != UNSAFE.getLong(other.addr + i))
                    return false;
            }
            return true;
        }
    }

    // Thread pool worker
    static final class Worker extends Thread {
        final int thread_id; // for debug use only

        Worker(int i) {
            thread_id = i;
            this.setPriority(Thread.MAX_PRIORITY);
            this.start();
        }

        @Override
        public void run() {
            var map = new Node[BUCKET_SIZE + MAX_STATIONS]; // extra space for collisions

            int id;
            // process in small chunk to maintain disk locality (artsiomkorzun trick)
            while ((id = chunk_id.getAndIncrement()) < chunk_cnt) {
                long addr = start_addr + id * CHUNK_SZ;
                long end = Math.min(addr + CHUNK_SZ, end_addr);

                // find start of line
                if (id > 0) {
                    addr = nextLF(addr);
                }

                final int num_segs = 3;
                long seglen = (end - addr) / num_segs;

                long a0 = addr;
                long a1 = nextLF(addr + 1 * seglen);
                long a2 = nextLF(addr + 2 * seglen);
                ChunkParser p0 = new ChunkParser(map, a0, a1);
                ChunkParser p1 = new ChunkParser(map, a1, a2);
                ChunkParser p2 = new ChunkParser(map, a2, end);

                while (p0.ok() && p1.ok() && p2.ok()) {
                    long w0 = p0.word();
                    long w1 = p1.word();
                    long w2 = p2.word();
                    long sc0 = getSemiCode(w0);
                    long sc1 = getSemiCode(w1);
                    long sc2 = getSemiCode(w2);
                    Node n0 = p0.key(w0, sc0);
                    Node n1 = p1.key(w1, sc1);
                    Node n2 = p2.key(w2, sc2);
                    long v0 = p0.val();
                    long v1 = p1.val();
                    long v2 = p2.val();
                    n0.add(v0);
                    n1.add(v1);
                    n2.add(v2);
                }

                while (p0.ok()) {
                    long w = p0.word();
                    long sc = getSemiCode(w);
                    Node n = p0.key(w, sc);
                    long v = p0.val();
                    n.add(v);
                }
                while (p1.ok()) {
                    long w = p1.word();
                    long sc = getSemiCode(w);
                    Node n = p1.key(w, sc);
                    long v = p1.val();
                    n.add(v);
                }
                while (p2.ok()) {
                    long w = p2.word();
                    long sc = getSemiCode(w);
                    Node n = p2.key(w, sc);
                    long v = p2.val();
                    n.add(v);
                }
            }

            // merge is cheaper than string casting (artsiomkorzun)
            while (!mapref.compareAndSet(null, map)) {
                var other_map = mapref.getAndSet(null);
                if (other_map != null) {
                    for (int i = 0; i < other_map.length; i++) {
                        var other = other_map[i];
                        if (other == null)
                            continue;
                        int bucket = (int) (other.hash & BUCKET_MASK);
                        while (true) {
                            var node = map[bucket];
                            if (node == null) {
                                map[bucket] = other;
                                break;
                            }
                            if (node.contentEquals(other)) {
                                node.merge(other);
                                break;
                            }
                            bucket++;
                        }
                    }
                }
            }
        }
    }

    static final class ChunkParser {
        long addr;
        long end;
        Node[] map;

        ChunkParser(Node[] m, long a, long e) {
            map = m;
            addr = a;
            end = e;
        }

        final boolean ok() {
            return addr < end;
        }

        final long word() {
            return UNSAFE.getLong(addr);
        }

        final void skip(int n) {
            addr += n;
        }

        final void skip(long n) {
            addr += n;
        }

        final long val0() {
            long w = word();
            int d = getDotCode(w);
            return num(w, d);
        }

        final long val() {
            long w = word();
            int d = getDotCode(w);
            skip((d >>> 3) + 3);
            return num(w, d);
        }

        // optimize for contest
        // save as much slow memory access as possible
        // about 50% key < 8chars, 25% key bettween 8-10 chars
        // keylength histogram (%) = [0, 0, 0, 0, 4, 10, 21, 15, 13, 11, 6, 6, 4, 2...
        final Node key(long word0, long semipos_code) {
            long row_addr = addr;
            // about 50% chance key < 8 chars
            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                skip(semi_pos + 1);
                long tail = word0 & HASH_MASKS[semi_pos];
                long hash = mix(tail);
                int bucket = (int) (hash & BUCKET_MASK);
                while (true) {
                    Node node = map[bucket];
                    if (node == null) {
                        return (map[bucket] = new Node(row_addr, hash, semi_pos));
                    }
                    if (node.hash == hash) {
                        return node;
                    }
                    bucket++;
                }
            }

            skip(8);
            long word = UNSAFE.getLong(addr);
            semipos_code = getSemiCode(word);
            // 43% chance
            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                skip(semi_pos + 1);
                long tail = word0 ^ (word & HASH_MASKS[semi_pos]);
                long hash = mix(tail);
                int bucket = (int) (hash & BUCKET_MASK);
                while (true) {
                    Node node = map[bucket];
                    if (node == null) {
                        return (map[bucket] = new Node(row_addr, word0, hash, semi_pos + 8));
                    }
                    if (node.word0 == word0 && node.hash == hash) {
                        return node;
                    }
                    bucket++;
                }
            }

            // why not going for more? tested, slower
            long hash = word0;
            while (semipos_code == 0) {
                hash ^= word;
                skip(8);
                word = UNSAFE.getLong(addr);
                semipos_code = getSemiCode(word);
            }

            int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
            skip(semi_pos);
            long keylen = addr - row_addr;
            skip(1);
            long tail = hash ^ (word & HASH_MASKS[semi_pos]);
            hash = mix(tail);
            int bucket = (int) (hash & BUCKET_MASK);

            while (true) {
                Node node = map[bucket];
                if (node == null) {
                    return (map[bucket] = new Node(row_addr, word0, hash, (int) keylen));
                }
                if (node.contentEquals(row_addr, word0, hash, keylen)) {
                    return node;
                }
                bucket++;
            }
        }
    }
}