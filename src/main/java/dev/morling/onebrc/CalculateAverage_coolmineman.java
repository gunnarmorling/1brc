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

import static java.util.stream.Collectors.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;

public class CalculateAverage_coolmineman {

    private static final String FILE = "./measurements.txt";

    // TODO maybe just write a byte arraylist
    static class ByteArrayOutputStreamEx extends ByteArrayOutputStream {
        byte[] buf() {
            return buf;
        }

        void shrink(int i) {
            count -= i;
        }
    }

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

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    static HashMap<BytesKey, MeasurementAggregator> back2basics = new HashMap<>();

    static class BytesKey {
        byte[] value;

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

    }

    static void parse(ByteBuffer a, int as, ByteBuffer b, int bs, boolean isFirst) {
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
                    double value = Double.parseDouble(new String(aa, doubleStart, doubleEnd - doubleStart, StandardCharsets.UTF_8));
                    back2basics.computeIfAbsent(station, k -> new MeasurementAggregator()).add(value);

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
        // TODO fix boundry case
        for (;;) {
            if (parseDouble) {
                if (ba[pos - as] == '\n') {
                    doubleEnd = pos;

                    BytesKey station = new BytesKey(ofRange(a, as, b, bs, nameStart, nameEnd));
                    // System.out.println(new String(station.value, StandardCharsets.UTF_8));
                    double value = Double.parseDouble(new String(ofRange(a, as, b, bs, doubleStart, doubleEnd), StandardCharsets.UTF_8));
                    // System.out.println(value);
                    back2basics.computeIfAbsent(station, k -> new MeasurementAggregator()).add(value);

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

    public static void main(String[] args) throws Exception {
        int pageSize = 80000;
        long pos = 0;
        try (AsynchronousFileChannel fc = AsynchronousFileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            var bbs = new ByteBuffer[4];
            for (int i = 0; i < bbs.length; i++) {
                bbs[i] = ByteBuffer.allocate(pageSize);
            }

            Future<Integer>[] futures = new Future[bbs.length];

            for (int i = 0; i < futures.length; i++) {
                futures[i] = fc.read(bbs[i], pos);
                pos += pageSize;
            }

            boolean first = true;
            l: for (;;) {
                for (int i = 0; i < bbs.length; i++) {
                    int ra = futures[i].get();
                    if (ra < 0)
                        break l;
                    int rb = futures[(i + 1) % futures.length].get();
                    if (rb < 0)
                        rb = 0;
                    parse(bbs[i], ra, bbs[(i + 1) % bbs.length], rb, first);
                    first = false;
                    bbs[i].position(0);
                    futures[i] = fc.read(bbs[i], pos);
                    pos += pageSize;
                }
            }
        }

        System.out.print('{');
        boolean[] first = new boolean[]{ true };
        back2basics.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).forEach(e -> {
            if (!first[0]) {
                System.out.print(',');
                System.out.print(' ');
            }
            first[0] = false;
            System.out.print(e.toString());
        });
        System.out.print('}');
    }
}
