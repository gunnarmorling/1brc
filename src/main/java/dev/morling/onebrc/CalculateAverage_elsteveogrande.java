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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class CalculateAverage_elsteveogrande {

    private static final String FILE = "./measurements.txt";

    static final class Station {
        final String name;
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        float total = 0;
        int count = 0;

        Station(String name) {
            this.name = name;
        }

        void update(float val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            total += val;
            count++;
        }

        public void update(Station that) {
            min = Math.min(this.min, that.min);
            max = Math.max(this.max, that.max);
            this.total += that.total;
            this.count += that.count;
        }

        public String toString() {
            return STR.
                    "\{String.format("%.1f", this.min)}\{'/'}\{String.format("%.1f", this.total / this.count)}\{'/'}\{String.format("%.1f", this.max)}";
        }
    }

    static final int NPROCS = Runtime.getRuntime().availableProcessors();

    static class Task extends Thread {
        final MappedByteBuffer mmap;
        final Map<Integer, Station> stations = new HashMap<>();

        Task(MappedByteBuffer mmap) {
            this.mmap = mmap;
        }

        @Override
        public void run() {
            int n = mmap.capacity();

            byte[] stationBytes = new byte[32];
            byte[] valBytes = new byte[8];
            int i = 0;
            while (i < n) {
                int s = 0;
                int v = 0;
                byte b;
                while ((b = mmap.get(i++)) != ';') {
                    stationBytes[s++] = b;
                }
                while ((b = mmap.get(i++)) != '\n') {
                    valBytes[v++] = b;
                }

                final var ss = s;
                var station = stations.computeIfAbsent(
                        key(stationBytes, s),
                        _ -> {
                            var name = new String(stationBytes, 0, ss);
                            return new Station(name);
                        });
                var val = parseFloatDot1(valBytes);
                station.update(val);
            }
        }

        private int key(byte[] bytes, int len) {
            int i = 0;
            int ret = 0;
            byte b;
            len = Math.min(4, len);
            while (i < len) {
                b = bytes[i++];
                ret = (59 * ret) + b;
            }
            return ret;
        }

        private float parseFloatDot1(byte[] s) {
            byte b;
            int i = 0;
            float ret = 0.0f;
            boolean neg = false;
            if (s[i] == '-') {
                ++i;
                neg = true;
            }
            while ((b = s[i++]) != '.') {
                ret = (ret * 10.0f) + (float) (b - '0');
            }
            ret += ((b - '0') / 10.0f);
            return neg ? -ret : ret;
        }
    }

    private void consumeFiles() throws IOException {
        Task[] tasks = new Task[NPROCS];

        try (var file = new RandomAccessFile(new File(FILE), "r")) {
            long[] offsets = new long[NPROCS + 1];

            var size = file.length();
            for (int i = 1; i < NPROCS; i++) {
                long offset = (long) (size * ((float) i) / NPROCS);
                file.seek(offset);
                // noinspection StatementWithEmptyBody
                while (file.readByte() != '\n') {
                }
                offsets[i] = file.getFilePointer();
            }
            offsets[NPROCS] = size;

            for (int i = 0; i < NPROCS; i++) {
                tasks[i] = new Task(
                        file.getChannel().map(
                                FileChannel.MapMode.READ_ONLY,
                                offsets[i],
                                offsets[i + 1] - offsets[i]));
                tasks[i].start();
            }

            for (int i = 0; i < NPROCS; i++) {
                while (true) {
                    try {
                        tasks[i].join();
                        break;
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        final SortedMap<String, Station> stationsByName = new TreeMap<>();
        for (var task : tasks) {
            task.stations.forEach((_, station) -> {
                var s = stationsByName.computeIfAbsent(station.name, Station::new);
                s.update(station);
            });
        }

        System.out.println(stationsByName);
    }

    private void run() throws Exception {
        consumeFiles();
    }

    public static void main(String[] args) throws Exception {
        (new CalculateAverage_elsteveogrande()).run();
    }
}
