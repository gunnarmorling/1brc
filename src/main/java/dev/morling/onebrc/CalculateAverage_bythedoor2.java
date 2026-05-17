package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    // public static void main(String[] args) throws IOException {
    // Map<String, MeasurementAggregator> aggs = new java.util.HashMap<>();
    //
    // try (FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
    //
    // ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    //
    // byte[] lineBytes = new byte[128];
    // int lineLen = 0;
    //
    // while (buf.hasRemaining()) {
    // byte b = buf.get();
    // if (b == '\n') {
    // if (lineLen > 0) {
    // parseLine(lineBytes, lineLen, aggs);
    // lineLen = 0;
    // }
    // }
    // else {
    // lineBytes[lineLen++] = b;
    // }
    // }
    // if (lineLen > 0) {
    // parseLine(lineBytes, lineLen, aggs);
    // }
    // }
    //
    // Map<String, ResultRow> measurements = new TreeMap<>();
    // aggs.forEach((station, agg) -> measurements.put(station, new ResultRow(
    // agg.min,
    // (Math.round(agg.sum * 10.0) / 10.0) / agg.count,
    // agg.max)));
    //
    // System.out.println(measurements);
    // }

    public static void main(String[] args) throws IOException {
        Map<String, MeasurementAggregator> aggs = new java.util.HashMap<>();

        try (FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long CHUNK_SIZE = 1L * 1024 * 1024 * 1024; // 1GB씩 청크
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

                // 청크 경계에서 줄이 잘렸는지 확인
                // lineLen > 0 이면 다음 청크에서 이어서 처리 (lineLen 초기화 안 함)
            }

            // 마지막 줄 처리
            if (lineLen > 0) {
                parseLine(lineBytes, lineLen, aggs);
            }
        }

        // 결과 출력
        Map<String, ResultRow> measurements = new TreeMap<>();
        aggs.forEach((station, agg) -> measurements.put(station, new ResultRow(
                agg.min,
                (Math.round(agg.sum * 10.0) / 10.0) / agg.count,
                agg.max)));
        System.out.println(measurements);
    }

    private static void parseLine(byte[] line, int len, Map<String, MeasurementAggregator> aggs) {
        // ';' 위치 탐색 — split() 대신 byte 직접 탐색
        // split()이 제거되는 건 2순위이지만, byte 파싱 구조상 여기서 같이 제거됨
        int sep = -1;
        for (int i = 0; i < len; i++) {
            if (line[i] == ';') {
                sep = i;
                break;
            }
        }
        if (sep == -1)
            return;

        // station: 여전히 String 필요 (Map 키) — 불가피한 변환
        String station = new String(line, 0, sep);

        // value: 아직 Double.parseDouble 사용 (2순위에서 제거 예정)
        double value = Double.parseDouble(new String(line, sep + 1, len - sep - 1));

        MeasurementAggregator agg = aggs.get(station);
        if (agg == null) {
            agg = new MeasurementAggregator();
            aggs.put(station, agg);
        }
        agg.min = Math.min(agg.min, value);
        agg.max = Math.max(agg.max, value);
        agg.sum += value;
        agg.count++;
    }
}