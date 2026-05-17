package dev.morling.onebrc;

import static java.util.stream.Collectors.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public class CalculateAverage_bythedoor2 {

    private static final String FILE = "./measurements.txt";

    private static long parseLong(CharSequence cs) {
        long r = 0;
        boolean is_minus = cs.charAt(0) == '-';

        for (int i = is_minus ? 1 : 0; i < cs.length(); i++) {
            r = r * 10 + (cs.charAt(i) - '0') * (is_minus ? -1 : 1);
        }

        return r;
    }

    // 1 decimal
    private static short parseShort(CharSequence cs) {
        return (short) (cs.charAt(0) - '0');
    }

    private static record Measurement(String station, long v, short d) {
        private Measurement(
                CharSequence station,
                CharSequence temperature
        ) {
            this(
                    station.toString(),
                    parseLong(temperature.subSequence(0,temperature.length()-2)),
                    parseShort(temperature.subSequence(temperature.length()-1,temperature.length()))
            );
        }
    }

    private record ResultRow(
            long minV, short minD,
            long maxV, short maxD,
            long sumV, long sumD, long count
    ) {

        public String toString() {
            double sum = (double) sumD + sumV / 10.0;
            double mean = (round(sum * 10.0) / 10.0) / count;

            return minD + "." + minV + "/" + mean + "/" + maxD + "." + maxV;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private long minV = Long.MAX_VALUE;
        private short minD = Short.MAX_VALUE;
        private long maxV = Long.MIN_VALUE;
        private short maxD = Short.MIN_VALUE;
        private long sumV = 0;
        private long sumD = 0;
        private long count = 0;

        public MeasurementAggregator aggregateMin(Measurement m) {
            if (this.minV > m.v) {
                this.minV = m.v;
                this.minD = m.d;
            }
            else if (this.minV == m.v && this.minD > m.d) {
                this.minD = m.d;
            }

            return this;
        }

        public MeasurementAggregator aggregateMin(MeasurementAggregator a) {
            if (this.minV > a.minV) {
                this.minV = a.minV;
                this.minD = a.minD;
            }
            else if (this.minV == a.minV && this.minD > a.minD) {
                this.minD = a.minD;
            }

            return this;
        }

        public MeasurementAggregator aggregateMax(Measurement m) {
            if (this.maxV < m.v) {
                this.maxV = m.v;
                this.maxD = m.d;
            }
            else if (this.maxV == m.v && this.maxD < m.d) {
                this.maxD = m.d;
            }

            return this;
        }

        public MeasurementAggregator aggregateMax(MeasurementAggregator a) {
            if (this.maxV < a.maxV) {
                this.maxV = a.maxV;
                this.maxD = a.maxD;
            }
            else if (this.maxV == a.maxV && this.maxD < a.maxD) {
                this.maxD = a.maxD;
            }

            return this;
        }

        public MeasurementAggregator aggregateSum(Measurement m) {
            this.sumV += m.v;
            this.sumD += m.d;

            this.count++;

            return this;
        }

        public MeasurementAggregator aggregateSum(MeasurementAggregator a) {
            this.sumV += a.sumV;
            this.sumD += a.sumD;

            this.count += a.count;

            return this;
        }
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> a.aggregateMin(m)
                        .aggregateMax(m)
                        .aggregateSum(m),
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();

                    return res
                            .aggregateMin(agg1).aggregateMin(agg2)
                            .aggregateMax(agg1).aggregateMax(agg2)
                            .aggregateSum(agg1).aggregateSum(agg2);
                },
                agg -> new ResultRow(agg.minV, agg.minD, agg.maxV, agg.maxD, agg.sumV, agg.sumD, agg.count));

        // 8MB
        int bufferSize = 8 * 1024 * 1024;

        try (
                FileReader fr = new FileReader(FILE, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(fr, bufferSize)) {
            Map<String, ResultRow> measurements = new TreeMap<>(br.lines()
                    .map(l -> {
                        int splitIndex = l.indexOf(';');

                        return new Measurement(
                                l.subSequence(0, splitIndex), l.subSequence(splitIndex + 1, l.length()));
                    })
                    .collect(groupingBy(m -> m.station(), collector)));

            System.out.println(measurements);
        }
    }
}
