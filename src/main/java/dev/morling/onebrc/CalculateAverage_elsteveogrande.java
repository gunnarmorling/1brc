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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class CalculateAverage_elsteveogrande {

    private static final String FILE = "./measurements.txt";

    long hash(byte[] bytes) {
        assert (bytes.length % 8 == 0);
        long ret = 0x7654_3210_fedc_ba98L;
        for (int b = 0; b < bytes.length; b += 8) {
            for (int i = b; i < b + 8; i++) {
                // noinspection PointlessBitwiseExpression
                ret += (0L
                        | ((((long) (bytes[i])) & 0xff) << 56)
                        | ((((long) (bytes[i])) & 0xff) << 48)
                        | ((((long) (bytes[i])) & 0xff) << 40)
                        | ((((long) (bytes[i])) & 0xff) << 32)
                        | ((((long) (bytes[i])) & 0xff) << 24)
                        | ((((long) (bytes[i])) & 0xff) << 16)
                        | ((((long) (bytes[i])) & 0xff) << 8)
                        | ((((long) (bytes[i])) & 0xff) << 0));
            }
        }
        return ret;
    }

    final class Bucket {
        final String stationString;
        final byte[] station;
        final long hash;
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        float total = 0;
        int count = 0;

        Bucket(String station) {
            this.stationString = station;
            this.station = new byte[32];
            var bytes = station.getBytes(StandardCharsets.UTF_8);
            assert (bytes.length <= 32);
            System.arraycopy(bytes, 0, this.station, 0, bytes.length);
            this.hash = hash(this.station);
        }

        void update(float val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            total += val;
            count++;
        }

        @Override
        public String toString() {
            return this.stationString
                    + '='
                    + String.format("%.1f", this.min)
                    + '/'
                    + String.format("%.1f", this.total / this.count)
                    + '/'
                    + String.format("%.1f", this.max);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(this.hash);
        }
    }

    SortedMap<String, Bucket> allBuckets = new ConcurrentSkipListMap<>();

    final class Task extends Thread {
        final List<String> list = new LinkedList<>();
        final TreeMap<String, Bucket> buckets = new TreeMap<>();

        Bucket getBucket(String station) {
            return buckets.computeIfAbsent(
                    station,
                    s -> {
                        var ret = new Bucket(s);
                        allBuckets.put(s, ret);
                        return ret;
                    });
        }

        void update(String station, float val) {
            var bucket = getBucket(station);
            bucket.update(val);
        }

        void update(String line) {
            int semi = line.indexOf(';');
            var station = line.substring(0, semi);
            var val = Float.parseFloat(line.substring(semi + 1));
            update(station, val);
        }

        @Override
        public void run() {
            for (var line : list) {
                update(line);
            }
        }
    }

    private void run() throws Exception {
        Task[] tasks = new Task[8];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new Task();
        }

        try (var lines = Files.lines(Paths.get(FILE))) {
            lines.forEach(line -> {
                var index = ((int) line.charAt(0)) % tasks.length;
                var task = tasks[index];
                task.list.add(line);
            });
        }

        for (Task task : tasks) {
            task.start();
        }

        for (Task task : tasks) {
            task.join();
        }

        final long firstHash = allBuckets.firstEntry().getValue().hash;
        System.out.print('{');
        allBuckets.values().forEach(b -> {
            if (b.hash != firstHash) {
                System.out.print(", ");
            }
            System.out.println(b); ////////////////////////////////////////////////////////
            // System.out.print(b);
        });
        System.out.println('}');
    }

    public static void main(String[] args) throws Exception {
        (new CalculateAverage_elsteveogrande()).run();
    }

    //
    //
    // private static record Measurement(String station, double value) {
    // private Measurement(String[] parts) {
    // this(parts[0], Double.parseDouble(parts[1]));
    // }
    // }
    //
    // private static record ResultRow(double min, double mean, double max) {
    // public String toString() {
    // return round(min) + "/" + round(mean) + "/" + round(max);
    // }
    //
    // private double round(double value) {
    // return Math.round(value * 10.0) / 10.0;
    // }
    // };
    //
    // private static class MeasurementAggregator {
    // private double min = Double.POSITIVE_INFINITY;
    // private double max = Double.NEGATIVE_INFINITY;
    // private double sum;
    // private long count;
    // }
    //
    // public static void main(String[] args) throws IOException {
    // // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
    // // .map(l -> l.split(";"))
    // // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
    // //
    // // measurements1 = new TreeMap<>(measurements1.entrySet()
    // // .stream()
    // // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
    // // System.out.println(measurements1);
    //
    // Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
    // MeasurementAggregator::new,
    // (a, m) -> {
    // a.min = Math.min(a.min, m.value);
    // a.max = Math.max(a.max, m.value);
    // a.sum += m.value;
    // a.count++;
    // },
    // (agg1, agg2) -> {
    // var res = new MeasurementAggregator();
    // res.min = Math.min(agg1.min, agg2.min);
    // res.max = Math.max(agg1.max, agg2.max);
    // res.sum = agg1.sum + agg2.sum;
    // res.count = agg1.count + agg2.count;
    //
    // return res;
    // },
    // agg -> {
    // return new ResultRow(agg.min, agg.sum / agg.count, agg.max);
    // });
    //
    // Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
    // .map(l -> new Measurement(l.split(";")))
    // .collect(groupingBy(m -> m.station(), collector)));
    //
    // System.out.println(measurements);
    // }
}
