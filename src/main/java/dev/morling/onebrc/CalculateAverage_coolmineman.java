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

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_coolmineman {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        void merge(MeasurementAggregator o) {
            min = Math.min(min, o.min);
            max = Math.max(max, o.max);
            sum += o.sum;
            count += o.count;
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static class BytesKey implements Comparable<BytesKey> {
        byte[] value;
        int hashcode;

        BytesKey(byte[] value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return new String(value, StandardCharsets.UTF_8);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof BytesKey bk) {
                return Arrays.equals(value, bk.value);
            }
            return false;
        }

        @Override
        public int compareTo(BytesKey o) {
            return Arrays.compare(value, o.value);
        }

    }

    static void parse(ByteBuffer a, int as, ByteBuffer b, int bs, boolean isFirst, HashMap<BytesKey, MeasurementAggregator> map) {
        var aa = a.array();
        var ba = b.array();
        int pos = 0;
        int nameEnd = 0;
        boolean parseDouble = false;
        int doubleStart = 0;
        int doubleEnd = 0;
        if (!isFirst) {
            while (aa[pos] != (byte) '\n') {
                pos++;
            }
            pos++;
        }
        int nameStart = pos;
        while (pos < as) {
            if (parseDouble) {
                if (aa[pos] == '\n') {
                    doubleEnd = pos;

                    BytesKey station = new BytesKey(Arrays.copyOfRange(aa, nameStart, nameEnd));
                    double value = parseDouble(aa, doubleStart, doubleEnd);
                    MeasurementAggregator ma = map.get(station);
                    if (ma == null)
                        map.put(station, ma = new MeasurementAggregator());
                    ma.add(value);

                    parseDouble = false;
                    nameStart = pos + 1;
                    nameEnd = 0;
                    doubleStart = 0;
                    doubleEnd = 0;
                }
            }
            else {
                if (aa[pos] == ';') {
                    nameEnd = pos;
                    parseDouble = true;
                    doubleStart = pos + 1;
                }
            }

            pos++;
        }
        if (bs <= 0)
            return;
        for (;;) {
            if (parseDouble) {
                if (ba[pos - as] == '\n') {
                    doubleEnd = pos;

                    BytesKey station = new BytesKey(ofRange(a, as, b, bs, nameStart, nameEnd));
                    double value = parseDouble(ofRange(a, as, b, bs, doubleStart, doubleEnd), 0, doubleEnd - doubleStart);
                    map.computeIfAbsent(station, k -> new MeasurementAggregator()).add(value);

                    return;
                }
            }
            else {
                if (ba[pos - as] == ';') {
                    nameEnd = pos;
                    parseDouble = true;
                    doubleStart = pos + 1;
                }
            }

            pos++;
        }
    }

    static byte[] ofRange(ByteBuffer a, int as, ByteBuffer b, int bs, int start, int end) {
        var aa = a.array();
        var ba = b.array();
        byte[] r = new byte[end - start];
        for (int i = 0; i < r.length; i++) {
            int pos = start + i;
            if (pos < as) {
                r[i] = aa[pos];
            }
            else {
                r[i] = ba[pos - as];
            }
        }
        return r;
    }

    static double parseDouble(byte[] b, int start, int end) {
        boolean negative = false;
        if (b[start] == (byte) '-') {
            negative = true;
            start += 1;
        }
        double result = 0;
        for (int i = start; i < end; i++) {
            if (b[i] != (byte) '.') {
                result *= 10;
                result += (b[i] & 0xFF) - '0';
            }
        }
        if (negative)
            result *= -1;
        return result * .1;
    }

    public static void main(String[] args) throws Exception {
        int pageSize = 1600000;
        long pos = 0;
        try (AsynchronousFileChannel fc = AsynchronousFileChannel.open(Paths.get(FILE), Set.of(StandardOpenOption.READ), Executors.newCachedThreadPool())) {
            var cp = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors());

            var bbs = new ByteBuffer[Runtime.getRuntime().availableProcessors() * 8];
            for (int i = 0; i < bbs.length; i++) {
                bbs[i] = ByteBuffer.allocate(pageSize);
            }

            Future<Integer>[] futures = new Future[bbs.length];
            HashMap<BytesKey, MeasurementAggregator>[] maps = new HashMap[bbs.length];
            Future[] tasks = new Future[bbs.length];

            for (int i = 0; i < futures.length; i++) {
                futures[i] = fc.read(bbs[i], pos);
                maps[i] = new HashMap<>();
                pos += pageSize;
            }

            boolean first = true;
            l: for (;;) {
                for (int i = 0; i < bbs.length; i++) {
                    int nextIndex = (i + 1) % bbs.length;
                    if (tasks[nextIndex] != null) {
                        tasks[nextIndex].get();
                        bbs[nextIndex].position(0);
                        futures[nextIndex] = fc.read(bbs[nextIndex], pos);
                        pos += pageSize;
                    }
                    var fa = futures[i];
                    var fb = futures[(i + 1) % futures.length];
                    var isFirst = first;
                    int ra = fa.get();
                    if (ra < 0)
                        break l;
                    int rb = Math.max(0, fb.get());
                    var iLol = i;
                    tasks[i] = cp.submit(() -> {
                        parse(bbs[iLol], ra, bbs[nextIndex], rb, isFirst, maps[iLol]);
                    });

                    first = false;
                }
            }

            for (Future t : tasks) {
                if (t != null)
                    t.get();
            }

            for (int i = 1; i < maps.length; i++) {
                merge(maps[0], maps[i]);
            }

            System.out.print('{');
            boolean[] firstE = new boolean[]{ true };
            maps[0].entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).forEach(e -> {
                if (!firstE[0]) {
                    System.out.print(',');
                    System.out.print(' ');
                }
                firstE[0] = false;
                System.out.print(e.toString());
            });
            System.out.println('}');
            System.exit(0);
        }
    }

    static void merge(HashMap<BytesKey, MeasurementAggregator> a, HashMap<BytesKey, MeasurementAggregator> b) {
        for (var e : b.entrySet()) {
            if (a.containsKey(e.getKey())) {
                a.get(e.getKey()).merge(e.getValue());
            }
            else {
                a.put(e.getKey(), e.getValue());
            }
        }
    }
}
