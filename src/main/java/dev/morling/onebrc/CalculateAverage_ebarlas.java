package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.TreeMap;

public class CalculateAverage_ebarlas {

    private static final int HASH_FACTOR = 278;
    private static final int HASH_MOD = 3_487;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: java CalculateAverage <input-file> <partitions>");
            System.exit(1);
        }
        var path = Paths.get(args[0]);
        var numPartitions = Integer.parseInt(args[1]);
        assert numPartitions > 0;
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        var partitionSize = channel.size() / numPartitions;
        var partitions = new Partition[numPartitions];
        var threads = new Thread[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            var pIdx = i;
            var pStart = pIdx * partitionSize;
            var pEnd = pIdx == numPartitions - 1
                    ? channel.size() // last partition might be slightly larger
                    : pStart + partitionSize;
            var pSize = pEnd - pStart;
            Runnable r = () -> {
                try {
                    var buffer = channel.map(FileChannel.MapMode.READ_ONLY, pStart, pSize);
                    partitions[pIdx] = processBuffer(buffer, pIdx == 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }
        for (var thread : threads) {
            thread.join();
        }
        var partitionList = List.of(partitions);
        foldFootersAndHeaders(partitionList);
        var stats = foldStats(partitionList);
        printResults(stats);
    }

    private static void printResults(Stats[] stats) { // adheres to Gunnar's reference code
        var result = new TreeMap<String, String>();
        for (var st : stats) {
            if (st != null) {
                var key = new String(st.key, StandardCharsets.UTF_8);
                result.put(key, format(st));
            }
        }
        System.out.println(result);
    }

    private static String format(Stats st) { // adheres to expected output format
        return round(st.min / 10.0) + "/" + round((st.sum / 10.0) / st.count) + "/" + round(st.max / 10.0);
    }

    private static double round(double value) { // Gunnar's round function
        return Math.round(value * 10.0) / 10.0;
    }

    private static Stats[] foldStats(List<Partition> partitions) { // fold stats from all partitions into first partition
        assert !partitions.isEmpty();
        var target = partitions.getFirst().stats;
        for (int i = 1; i < partitions.size(); i++) {
            var current = partitions.get(i).stats;
            for (int j = 0; j < current.length; j++) {
                if (current[j] != null) {
                    var t = target[j];
                    if (t == null) {
                        target[j] = current[j]; // copy ref from current to target
                    } else {
                        t.min = Math.min(t.min, current[j].min);
                        t.max = Math.max(t.max, current[j].max);
                        t.sum += current[j].sum;
                        t.count += current[j].count;
                    }
                }
            }
        }
        return target;
    }

    private static void foldFootersAndHeaders(List<Partition> partitions) { // fold footers and headers into prev partition
        for (int i = 1; i < partitions.size(); i++) {
            var pNext = partitions.get(i);
            var pPrev = partitions.get(i - 1);
            var merged = mergeFooterAndHeader(pPrev.footer, pNext.header);
            if (merged != null) {
                doProcessBuffer(ByteBuffer.wrap(merged), true, pPrev.stats); // fold into prev partition
            }
        }
    }

    private static byte[] mergeFooterAndHeader(byte[] footer, byte[] header) {
        if (footer == null) {
            return header;
        }
        if (header == null) {
            return footer;
        }
        var merged = new byte[footer.length + header.length];
        System.arraycopy(footer, 0, merged, 0, footer.length);
        System.arraycopy(header, 0, merged, footer.length, header.length);
        return merged;
    }

    private static Partition processBuffer(ByteBuffer buffer, boolean first) {
        return doProcessBuffer(buffer, first, new Stats[HASH_MOD * 2]);
    }

    private static Partition doProcessBuffer(ByteBuffer buffer, boolean first, Stats[] stats) {
        var readingKey = true;
        var keyHash = 0;
        var keyStart = 0;
        var negative = false;
        var val = 0;
        var header = first ? null : readHeader(buffer);
        while (buffer.hasRemaining()) {
            var b = buffer.get();
            if (readingKey) {
                if (b == ';') {
                    var idx = HASH_MOD + keyHash % HASH_MOD;
                    var st = stats[idx];
                    if (st == null) {
                        var pos = buffer.position();
                        var keySize = pos - keyStart - 1;
                        var key = new byte[keySize];
                        buffer.position(keyStart);
                        buffer.get(key);
                        buffer.position(pos);
                        stats[idx] = new Stats(key);
                    }
                    readingKey = false;
                } else {
                    keyHash = HASH_FACTOR * keyHash + b;
                }
            } else {
                if (b == '\n') {
                    var idx = HASH_MOD + keyHash % HASH_MOD;
                    updateStats(stats[idx], negative ? -val : val);
                    readingKey = true;
                    keyHash = 0;
                    val = 0;
                    negative = false;
                    keyStart = buffer.position();
                } else if (b == '-') {
                    negative = true;
                } else if (b == '.') {
                    // skip
                } else {
                    val = val * 10 + (b - '0');
                }
            }
        }
        var footer = keyStart < buffer.position()
                ? readFooter(buffer, keyStart)
                : null;
        return new Partition(header, footer, stats);
    }

    private static void updateStats(Stats st, int val) {
        st.min = Math.min(st.min, val);
        st.max = Math.max(st.max, val);
        st.sum += val;
        st.count++;
    }

    private static byte[] readFooter(ByteBuffer buffer, int lineStart) {
        var footer = new byte[buffer.position() - lineStart];
        var pos = buffer.position();
        buffer.position(lineStart);
        buffer.get(footer);
        buffer.position(pos);
        return footer;
    }

    private static byte[] readHeader(ByteBuffer buffer) {
        while (buffer.hasRemaining() && buffer.get() != '\n') ;
        var header = new byte[buffer.position()];
        var pos = buffer.position();
        buffer.position(0);
        buffer.get(header);
        buffer.position(pos);
        return header;
    }

    record Partition(byte[] header, byte[] footer, Stats[] stats) {}

    private static class Stats {
        final byte[] key;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum;
        long count;

        Stats(byte[] key) {
            this.key = key;
        }
    }

}
