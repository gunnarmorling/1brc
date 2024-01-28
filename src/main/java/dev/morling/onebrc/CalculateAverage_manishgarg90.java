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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_manishgarg90 {

    private static final String FILE = "./measurements.txt";
    private static int nProcessors = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException {
        try (FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long chunkSize = (fileSize + nProcessors - 1) / nProcessors;
            long pos = 0;

            List<MappedByteBuffer> buffers = new ArrayList<>(nProcessors);

            for (int i = 0; i < nProcessors; i++) {
                long endPosition = getEndPosition(channel, pos + chunkSize);
                long size = endPosition - pos;
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
                pos = pos + size;
                buffers.add(buffer);
            }

            Map<String, Stat> s = readBufferAndCalculateMeauremenst(buffers);
            Map<String, Stat> tm = new TreeMap<String, Stat>(s);
            System.out.println(tm);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static Map<String, Stat> readBufferAndCalculateMeauremenst(List<MappedByteBuffer> chunks) {
        return chunks.parallelStream().map(buffer -> {
            Map<String, Stat> map = new HashMap<>(10_000, 1);
            int lineStart = 0;
            int doubleStart = 0;
            int length = buffer.limit();
            String station = null;
            for (int i = 0; i < length; ++i) {
                byte b = buffer.get(i);
                if (b == ';') {
                    byte[] stationBuffer = new byte[i - lineStart];
                    buffer.position(lineStart);
                    buffer.get(stationBuffer);
                    station = new String(stationBuffer, StandardCharsets.UTF_8);
                    doubleStart = i + 1;
                }
                else if (b == '\n') {
                    byte[] doubleBuffer = new byte[i - doubleStart];
                    buffer.position(doubleStart);
                    buffer.get(doubleBuffer);
                    Double temperature = Double.parseDouble(new String(doubleBuffer));
                    lineStart = i + 1;

                    // I have station name and temp
                    Stat s = map.get(station);
                    if (s == null) {
                        map.put(station, new Stat(temperature));
                    }
                    else {
                        s.update(temperature);
                    }
                }
            }
            return map;
        }).reduce(new HashMap<>(), (map1, map2) -> {
            Stat s = new Stat();
            s.merge(map1);
            s.merge(map2);
            return s.getResultMap();
        });

    }

    private static long getEndPosition(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        while (position < channel.size()) {
            channel.read(buffer, position);

            if (buffer.get(0) == '\n') {
                return position + 1;
            }
            position++;
            buffer.clear();
        }
        return channel.size();
    }

    private static final class Stat {

        private Double min = Double.MAX_VALUE;
        private Double max = Double.MIN_VALUE;
        private Double sum = 0d;
        private long count = 0L;

        private Map<String, Stat> resultMap = null;

        public Stat() {
            this.resultMap = new HashMap<>(10_000, 1);
        }

        public Stat(Double value) {
            this.min = value;
            this.max = value;
            this.sum += value;
            this.count++;
        }

        private void update(Double value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum = round(this.sum + value);
            this.count++;
        }

        private void merge(Map<String, Stat> result) {
            result.forEach((city, resultRow) -> resultMap.merge(city, resultRow, (existing, incoming) -> {
                existing.min = Math.min(existing.min, incoming.min);
                existing.max = Math.max(existing.max, incoming.max);
                existing.sum += incoming.sum;
                existing.count += incoming.count;
                return existing;
            }));
        }

        public Map<String, Stat> getResultMap() {
            return resultMap;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }
    }
}
