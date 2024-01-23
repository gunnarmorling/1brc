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
import java.util.stream.IntStream;

import sun.misc.Unsafe;

/*
 *  The implementation has been carried out by Jordy Vega.
 *  I hope to contribute significantly with my solution.
 *  A cordial greeting from Guatemala. 🇬🇹
 */

public class CalculateAverage_JordyV3 {
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

    private static void debug(String s, Object... args) {
        System.out.printf(STR."\{s}%n", args);
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

    static class Node {
        long addr;
        long word0;
        long tail;
        long sum;
        int count;
        short min, max;
        int keyless;
        String key;

        void calcKey() {
            byte[] buff = new byte[MAX_STR_LEN];
            UNSAFE.copyMemory(null, addr, buff, Unsafe.ARRAY_BYTE_BASE_OFFSET, keyless);
            key = new String(buff, 0, keyless, StandardCharsets.UTF_8);
        }

        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min * 0.1, sum * 0.1 / count, max * 0.1);
        }

        Node(long a, long t, short val, int kl) {
            addr = a;
            tail = t;
            keyless = kl;
            sum = min = max = val;
            count = 1;
        }

        Node(long a, long w0, long t, short val, int kl) {
            addr = a;
            word0 = w0;
            tail = t;
            keyless = kl;
            sum = min = max = val;
            count = 1;
        }

        void add(short val) {
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
            sum += other.sum;
            count += other.count;
            if (other.max > max) {
                max = other.max;
            }
            if (other.min < min) {
                min = other.min;
            }
        }

        public boolean contentEquals(long otherAddr, long otherWord0, long otherTail) {
            // Quick check for common mismatches:
            if (tail != otherTail || word0 != otherWord0) {
                return false;
            }

            // Optimized comparison using bitwise XOR and a loop for clarity:
            long sum = 0;
            final long baseAddr = addr + 8;  // Start comparison after first word
            final long otherBaseAddr = otherAddr + 8;
            final int limit = (int) (baseAddr + (keyless & 0xF8));  // Calculate end address for clarity

            for (long addr = baseAddr; addr < limit; addr += 8) {
                sum |= UNSAFE.getLong(addr) ^ UNSAFE.getLong(otherBaseAddr + (addr - baseAddr));
            }

            return sum == 0;
        }
    }

    public static long[] slice(long startAddr, long endAddr, long chunkSize, int cpuCount) {
        long[] pointers = new long[cpuCount + 1];
        pointers[0] = startAddr;
        pointers[cpuCount] = endAddr;

        final long maxChunkEnd = Math.min(startAddr + (cpuCount - 1) * chunkSize, endAddr);

        for (int i = 1; i < cpuCount; i++) {
            long chunkStart = startAddr + i * chunkSize;
            long chunkEnd = Math.min(chunkStart + chunkSize, maxChunkEnd);
            long boundaryAddr = findNewlineBoundary(chunkStart, chunkEnd);
            pointers[i] = boundaryAddr;
        }

        return pointers;
    }

    private static long findNewlineBoundary(long startAddr, long endAddr) {
        while (true) {
            if (startAddr >= endAddr || UNSAFE.getByte(startAddr++) == '\n') break;
        }
        return startAddr;
    }

    static long getSemiPosCode(final long word) {
        long xor_semi = word ^ 0x3b3b3b3b3b3b3b3bL;
        return (xor_semi - 0x0101010101010101L) & (~xor_semi & 0x8080808080808080L);
    }

    // speed/collision balance
    static int xxh32(long hash) {
        final int p1 = 0x85EBCA77;
        int low = (int) hash;
        int high = (int) (hash >>> 33);
        int h = (low * p1) ^ high;
        return h ^ (h >>> 17);
    }

    public static short parseNum(long numWord, int dotPos) {
        final short sign = (short) ((numWord >>> 59) & 1);
        long digits = (numWord << 28) >>> (28 - dotPos + 4);
        long absVal = (digits * 0x640a0001L) >>> 32;
        absVal &= 0x3FF;
        return (short) ((absVal ^ sign) - sign);
    }

    // optimize for contest
    static Node[] parse(int thread_id, long start, long end) {
        int cls = 0;
        long addr = start;
        var map = new Node[BUCKET_SIZE + 10000];
        while (addr < end) {
            long row_addr = addr;
            long hash = 0;

            long word0 = UNSAFE.getLong(addr);
            long semipros_code = getSemiPosCode(word0);

            if (semipros_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipros_code) >>> 3;
                addr += semi_pos + 1;
                long num_word = UNSAFE.getLong(addr);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                addr += (dot_pos >>> 3) + 3;

                long tail = (word0 & HASH_MASKS[semi_pos]);
                int bucket = xxh32(tail) & BUCKET_MASK;
                short val = parseNum(num_word, dot_pos);

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
                        cls++;
                }
                continue;
            }

            hash ^= word0;
            addr += 8;
            long word = UNSAFE.getLong(addr);
            semipros_code = getSemiPosCode(word);
            if (semipros_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipros_code) >>> 3;
                addr += semi_pos;
                int keys = (int) (addr - row_addr);
                long num_word = UNSAFE.getLong(addr + 1);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                addr += (dot_pos >>> 3) + 4;

                long tail = (word & HASH_MASKS[semi_pos]);
                hash ^= tail;
                int bucket = xxh32(hash) & BUCKET_MASK;
                short val = parseNum(num_word, dot_pos);

                while (true) {
                    var node = map[bucket];
                    if (node == null) {
                        map[bucket] = new Node(row_addr, word0, tail, val, keys);
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

            while (semipros_code == 0) {
                hash ^= word;
                addr += 8;
                word = UNSAFE.getLong(addr);
                semipros_code = getSemiPosCode(word);
            }

            int semi_pos = Long.numberOfTrailingZeros(semipros_code) >>> 3;
            addr += semi_pos;
            int keyless = (int) (addr - row_addr);
            long num_word = UNSAFE.getLong(addr + 1);
            int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
            addr += (dot_pos >>> 3) + 4;

            long tail = (word & HASH_MASKS[semi_pos]);
            hash ^= tail;
            int bucket = xxh32(hash) & BUCKET_MASK;
            short val = parseNum(num_word, dot_pos);

            while (true) {
                var node = map[bucket];
                if (node == null) {
                    map[bucket] = new Node(row_addr, word0, tail, val, keyless);
                    break;
                }
                if (node.contentEquals(row_addr, word0, tail)) {
                    node.add(val);
                    break;
                }
                bucket++;
                if (SHOW_ANALYSIS)
                    cls++;
            }
        }
        if (SHOW_ANALYSIS) {
            debug("Thread %d collision = %d", thread_id, cls);
        }
        return map;
    }

    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder()
                .command(workerCommand)
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }
    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }

        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long start_addr = file.map(MapMode.READ_ONLY, 0, file.size(), Arena.global()).address();
            long file_size = file.size();
            long end_addr = start_addr + file_size;
            int cpu_cnt = file_size < 1e6 ? 1 : Runtime.getRuntime().availableProcessors();
            long chunk_size = Math.ceilDiv(file_size, cpu_cnt);

            var ptr = slice(start_addr, end_addr, chunk_size, cpu_cnt);

            TreeMap<String, Node> ms = new TreeMap<>();
            int[] linklist = new int[64];

            List<List<Node>> maps = IntStream.range(0, cpu_cnt)
                    .mapToObj(thread_id -> parse(thread_id, ptr[thread_id], ptr[thread_id + 1]))
                    .map(map -> {
                        List<Node> nodes = new ArrayList<>();
                        for (var node : map) {
                            if (node == null)
                                continue;
                            node.calcKey();
                            nodes.add(node);
                        }
                        return nodes;
                    })
                    .parallel()
                    .toList();

            for (var nodes : maps) {
                for (var node : nodes) {
                    if (SHOW_ANALYSIS) {
                        int kl = node.keyless & (linklist.length - 1);
                        linklist[kl] += node.count;
                    }
                    var stat = ms.putIfAbsent(node.key, node);
                    if (stat != null)
                        stat.merge(node);
                }
            }

            if (SHOW_ANALYSIS) {
                debug(STR."Total = \{Arrays.stream(linklist).sum()}");
                debug(STR."Length_histogram = \{Arrays.toString(Arrays.stream(linklist).map(x -> (int) (x * 1.0e-7)).toArray())}");
                return;
            }
            System.out.println(ms);
            System.out.close();
        }
    }
}
