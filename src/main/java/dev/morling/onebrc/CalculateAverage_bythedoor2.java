package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_bythedoor2 {

    private static final String FILE = "./measurements.txt";

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }
        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static final class ByteKey {
        final byte[] data;
        final int len;
        final int hash;

        // For Write (call about count of unqiue station name)
        ByteKey(byte[] src, int len) {
            this.data = Arrays.copyOf(src, len);
            this.len = len;
            this.hash = computeHash(src, len);
        }

        // Read (call about 1 billion) <- use original byte array
        ByteKey(byte[] src, int len, int precomputedHash) {
            this.data = src;
            this.len = len;
            this.hash = precomputedHash;
        }

        static int computeHash(byte[] data, int len) {
            int h = 1;
            for (int i = 0; i < len; i++) {
                h = 31 * h + data[i];
            }
            return h;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            ByteKey other = (ByteKey) o;
            if (len != other.len)
                return false;
            return Arrays.equals(data, 0, len, other.data, 0, other.len);
        }

        String toStation() {
            return new String(data, 0, len);
        }
    }

    public static void main(String[] args) throws IOException {
        // the type of key is String → ByteKey
        Map<ByteKey, MeasurementAggregator> aggs = new HashMap<>();

        try (FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long CHUNK_SIZE = 1L * 1024 * 1024 * 1024;
            long position = 0;

            byte[] lineBytes = new byte[128];
            int lineLen = 0;

            while (position < fileSize) {
                long remaining = fileSize - position;
                long mapSize = Math.min(CHUNK_SIZE, remaining);
                ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, position, mapSize);

                while (buf.hasRemaining()) {
                    byte b = buf.get();
                    if (b == '\n') {
                        if (lineLen > 0) {
                            parseLine(lineBytes, lineLen, aggs);
                            lineLen = 0;
                        }
                    }
                    else {
                        lineBytes[lineLen++] = b;
                    }
                }
                position += mapSize;
            }

            if (lineLen > 0) {
                parseLine(lineBytes, lineLen, aggs);
            }
        }

        // 최종 출력 시에만 String 변환 — 약 400번
        Map<String, ResultRow> measurements = new TreeMap<>();
        aggs.forEach((key, agg) -> measurements.put(key.toStation(), new ResultRow(
                agg.min,
                (Math.round(agg.sum * 10.0) / 10.0) / agg.count,
                agg.max)));
        System.out.println(measurements);
    }

    private static void parseLine(byte[] line, int len, Map<ByteKey, MeasurementAggregator> aggs) {
        int sep = -1;
        int hash = 1;
        for (int i = 0; i < len; i++) {
            if (line[i] == ';') {
                sep = i;
                break;
            }
            hash = 31 * hash + line[i];
        }
        if (sep == -1)
            return;

        ByteKey lookupKey = new ByteKey(line, sep, hash);

        MeasurementAggregator agg = aggs.get(lookupKey);
        if (agg == null) {
            agg = new MeasurementAggregator();
            aggs.put(new ByteKey(line, sep), agg);
        }

        double value = parseTemperature(line, sep + 1, len);
        agg.min = Math.min(agg.min, value);
        agg.max = Math.max(agg.max, value);
        agg.sum += value;
        agg.count++;
    }

    private static double parseTemperature(byte[] buf, int start, int end) {
        boolean negative = false;
        int i = start;

        if (buf[i] == '-') {
            negative = true;
            i++;
        }

        int intPart = buf[i++] - '0';
        if (buf[i] != '.') {
            intPart = intPart * 10 + (buf[i++] - '0');
        }
        i++; // '.' skip

        int fracPart = buf[i] - '0';

        double value = (intPart * 10 + fracPart) / 10.0;
        return negative ? -value : value;
    }
}