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

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_moysesb {

    private static final String FILE = "./measurements.txt";

    static class ByteArray {
        final byte[] value;
        final int hashCode;

        private ByteArray(byte[] val, int hashCode) {
            this.value = val;
            this.hashCode = hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof ByteArray other) {
                return this == other || hashCode == other.hashCode && Arrays.equals(value, other.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    static final Map<ByteArray, double[]> allResults = new HashMap<>(512);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int ncpus = Runtime.getRuntime().availableProcessors();
        ExecutorCompletionService<Map<ByteArray, double[]>> exec = new ExecutorCompletionService<>(Executors.newFixedThreadPool(ncpus));
        var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
        File f = new File(FILE);
        long fileSize = f.length();
        long split = 0;
        long chunkSize = Math.min(1 << 28,  fileSize < 1 << 28 ? fileSize : (fileSize / ncpus));
        List<Future<Map<ByteArray, double[]>>> tasks = new ArrayList<>();

        while (split < fileSize) {
            final long[] offset = {split};
            var task = exec.submit(() -> {
                try {
                    final Map<ByteArray, double[]> results = new HashMap<>(512);
                    file:
                    for (; ; ) {
                        long chunk = Math.min(fileSize - offset[0], chunkSize);
                        if (chunk == 0) {
                            return results;
                        }
                        MemorySegment mm = file.map(FileChannel.MapMode.READ_ONLY, offset[0], chunk, Arena.ofConfined());

                        int i = 0;
                        if (offset[0] > 0) {
                            while (mm.get(ValueLayout.JAVA_BYTE, i) != '\n') {
                                i++;
                            }
                            i++;
                        }
                        while (i < chunk) {
                            try {
                                byte[] city = new byte[32];
                                byte[] valb = new byte[16];
                                byte[] target = city;
                                double val = 0d;
                                int dotOffset = 0;
                                int l = 0;
                                int nameHash = 0;
                                for (; ; ) {
                                    byte b = mm.get(ValueLayout.OfByte.JAVA_BYTE, i++);
                                    if (b == ';') {
                                        target = valb;
                                        l = 0;
                                    } else if (b == '\n') {
                                        int integral = 0;
                                        int mult = 1;
                                        int frac = 0;
                                        for (int ii = 0; ii < dotOffset; ii++) {
                                            if (valb[ii] == '-') {
                                                mult = -1;
                                            } else {
                                                integral = integral * 10 + (valb[ii] - '0');
                                            }
                                        }

                                        for (int ii = dotOffset+1; ii < l; ii++) {
                                            frac = frac * 10 + (valb[ii] - '0');
                                        }
                                        val = integral;
                                        if (frac > 0)
                                            val += (frac/10d);

                                        val *= mult;
                                        var ba = new ByteArray(city, nameHash);

                                        var r = results.computeIfAbsent(ba, _s -> new double[]{1000, -1000, 0, 0});
                                        r[0] = Math.min(r[0], val);
                                        r[1] = Math.max(r[1], val);
                                        r[2]++;
                                        r[3] += val;
                                        break;
                                    } else {
                                        if (target == city) {
                                            nameHash = nameHash * 31 + b;
                                        } else if (b == '.') {
                                            dotOffset = l;
                                        }
                                        target[l++] = b;

                                    }
                                }
                            } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
                                //happens on the last segment after EOF
                                break file;
                            }
                        }
                        offset[0] += i;
                    }
                    return results;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            tasks.add(task);
            split += chunkSize;
        }

        int taken = 0;
        while (taken < tasks.size()) {
            Future<Map<ByteArray, double[]>> fut = exec.take();
            var map = fut.get();
            taken++;
            if (map == null) {
                continue;
            }
            for (Map.Entry<ByteArray, double[]> e : map.entrySet()) {
                var ba = e.getKey();
                var val = e.getValue();
                allResults.merge(ba, val, (old, _new) -> {
                    old[0] = Math.min(old[0], _new[0]);
                    old[1] = Math.max(old[1], _new[1]);
                    old[2] += _new[2];
                    old[3] += _new[3];
                    return old;
                });
            }
        }

        SortedMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<ByteArray, double[]> e : allResults.entrySet()) {
            byte[] utf8 = e.getKey().value;
            int strlen = 0;
            while (utf8[strlen] != '\0') strlen++;
            String city = new String(utf8, 0,  strlen, StandardCharsets.UTF_8);
            double[] r = e.getValue();
            String fmt = FormatProcessor.FMT."%.1f\{round(r[0])}/%.1f\{round(r[3]/r[2])}/%.1f\{round(r[1])}";
            sorted.put(city, fmt);
        }

        System.out.println(sorted);

        System.exit(0);
    }

    private static double round(double d) {
        return Math.round(d * 10.0) / 10.0;
    }
}
