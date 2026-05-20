package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
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

    private static final class StationMap {
        private static final int CAPACITY = 1 << 14; // 16384
        private static final int MASK = CAPACITY - 1;

        // 슬롯별 데이터를 배열로 직접 관리 — 객체 없음
        private final byte[][] keys = new byte[CAPACITY][];
        private final int[] hashes = new int[CAPACITY];
        private final double[] mins = new double[CAPACITY];
        private final double[] maxs = new double[CAPACITY];
        private final double[] sums = new double[CAPACITY];
        private final long[] counts = new long[CAPACITY];

        StationMap() {
            Arrays.fill(mins, Double.POSITIVE_INFINITY);
            Arrays.fill(maxs, Double.NEGATIVE_INFINITY);
        }

        void update(byte[] line, int sep, int hash, double value) {
            int slot = hash & MASK;

            // open addressing — 빈 슬롯 또는 같은 key 찾을 때까지 선형 탐색
            while (true) {
                if (keys[slot] == null) {
                    // 빈 슬롯 → 신규 station 등록 (~400번만)
                    keys[slot] = Arrays.copyOf(line, sep);
                    hashes[slot] = hash;
                    mins[slot] = value;
                    maxs[slot] = value;
                    sums[slot] = value;
                    counts[slot] = 1;
                    return;
                }

                // 해시 먼저 비교 (빠름) → 같으면 byte 비교
                if (hashes[slot] == hash
                        && keys[slot].length == sep
                        && Arrays.equals(keys[slot], 0, sep, line, 0, sep)) {
                    // 기존 station 업데이트
                    if (value < mins[slot])
                        mins[slot] = value;
                    if (value > maxs[slot])
                        maxs[slot] = value;
                    sums[slot] += value;
                    counts[slot]++;
                    return;
                }

                // 충돌 → 다음 슬롯 (linear probing)
                slot = (slot + 1) & MASK;
            }
        }

        // 결과 수집 (~400번)
        void forEach(java.util.function.BiConsumer<String, ResultRow> consumer) {
            for (int i = 0; i < CAPACITY; i++) {
                if (keys[i] != null) {
                    String station = new String(keys[i]);
                    ResultRow row = new ResultRow(
                            mins[i],
                            (Math.round(sums[i] * 10.0) / 10.0) / counts[i],
                            maxs[i]);
                    consumer.accept(station, row);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        StationMap stationMap = new StationMap();

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
                            parseLine(lineBytes, lineLen, stationMap);
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
                parseLine(lineBytes, lineLen, stationMap);
            }
        }

        Map<String, ResultRow> measurements = new TreeMap<>();
        stationMap.forEach(measurements::put);
        System.out.println(measurements);
    }

    private static void parseLine(byte[] line, int len, StationMap stationMap) {
        // ';' 탐색 + hash 계산 한 루프 (3순위에서 유지)
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

        double value = parseTemperature(line, sep + 1, len);

        // ByteKey 객체 생성 없이 직접 전달
        stationMap.update(line, sep, hash, value);
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