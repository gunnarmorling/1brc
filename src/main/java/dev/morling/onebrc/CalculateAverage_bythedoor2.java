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

    public static void main(String[] args) throws IOException {
        Map<String, MeasurementAggregator> aggs = new java.util.HashMap<>();

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

        Map<String, ResultRow> measurements = new TreeMap<>();
        aggs.forEach((station, agg) -> measurements.put(station, new ResultRow(
                agg.min,
                (Math.round(agg.sum * 10.0) / 10.0) / agg.count,
                agg.max)));
        System.out.println(measurements);
    }

    private static void parseLine(byte[] line, int len, Map<String, MeasurementAggregator> aggs) {
        int sep = -1;
        for (int i = 0; i < len; i++) {
            if (line[i] == ';') {
                sep = i;
                break;
            }
        }
        if (sep == -1)
            return;

        String station = new String(line, 0, sep);

        double value = parseTemperature(line, sep + 1, len);

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

    private static double parseTemperature(byte[] buf, int start, int end) {
        boolean negative = false;
        int i = start;

        if (buf[i] == '-') {
            negative = true;
            i++;
        }

        int intPart = buf[i++] - '0';
        if (buf[i] != '.') { // 두 자리 정수 ("12.x")
            intPart = intPart * 10 + (buf[i++] - '0');
        }
        i++;

        int fracPart = buf[i] - '0';

        double value = (intPart * 10 + fracPart) / 10.0;

        return negative ? -value : value;
    }

    private class ByteKey {

    }
}