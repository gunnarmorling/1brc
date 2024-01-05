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
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class CalculateAverage_padreati {

    private static final VectorSpecies<Byte> species = ByteVector.SPECIES_PREFERRED;
    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_SIZE = 256 * 1024 * 1024;
    private static final int THREADS = Runtime.getRuntime().availableProcessors() * 3;
    private static final ByteVector indexes = ByteVector.broadcast(species, 0).addIndex(1);
    private static final NameRegistry stations = new NameRegistry();
    private static final int MAX_STATIONS = 10_000;

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {

        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator(double seed) {
            this.min = seed;
            this.max = seed;
            this.sum = seed;
            this.count = 1L;
        }

        public MeasurementAggregator merge(double v) {
            this.min = Math.min(min, v);
            this.max = Math.max(max, v);
            this.sum += v;
            this.count++;
            return this;
        }

        public MeasurementAggregator merge(MeasurementAggregator b) {
            this.min = Math.min(min, b.min);
            this.max = Math.max(max, b.max);
            this.sum += b.sum;
            this.count += b.count;
            return this;
        }

        public ResultRow toResultRow() {
            return new ResultRow(min, sum / count, max);
        }
    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_padreati().run();
    }

    private record Segment(File source, long start, long end) {
    }

    private void run() throws IOException {
        var segments = computeSplits();

        CountDownLatch latch = new CountDownLatch(segments.size());
        try (var executor = Executors.newFixedThreadPool(THREADS)) {

            var tasks = segments.stream().map(s -> executor.submit(() -> chunkProcessor(latch, s))).toList();
            tasks = new ArrayList<>(tasks);

            MeasurementAggregator[] aggregate = new MeasurementAggregator[MAX_STATIONS];

            while (latch.getCount() > 0) {
                var it = tasks.iterator();
                while (it.hasNext()) {
                    var future = it.next();
                    if (future.isDone()) {
                        var results = future.get();
                        for (int i = 0; i < results.length; i++) {
                            if (aggregate[i] == null) {
                                aggregate[i] = results[i];
                            }
                            else {
                                aggregate[i].merge(results[i]);
                            }
                        }
                        it.remove();
                    }
                }
            }
            TreeMap<String, ResultRow> measurements = new TreeMap<>();
            for (int i = 0; i < aggregate.length; i++) {
                if (aggregate[i] == null) {
                    continue;
                }
                measurements.put(stations.getStation(i), aggregate[i].toResultRow());
            }
            System.out.println(measurements);

        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Segment> computeSplits() throws IOException {

        List<Segment> segments = new ArrayList<>();
        long last = 0L;

        File file = new File(FILE);
        long next = CHUNK_SIZE;
        while (next < file.length()) {
            try (FileInputStream fis = new FileInputStream(file)) {
                long skip = fis.skip(next);
                if (skip != next) {
                    throw new RuntimeException();
                }
                // find first new line
                while (fis.read() != '\n') {
                    next++;
                }
                segments.add(new Segment(file, last, next + 1));
                last = next + 1;
                next += CHUNK_SIZE;
            }
        }
        return segments;
    }

    private MeasurementAggregator[] chunkProcessor(CountDownLatch latch, Segment s) throws IOException {
        var map = new MeasurementAggregator[MAX_STATIONS];

        try (var channel = FileChannel.open(s.source.toPath(), StandardOpenOption.READ)) {
            var ms = channel.map(FileChannel.MapMode.READ_ONLY, s.start, s.end - s.start, Arena.ofConfined());
            int len = (int) (s.end - s.start);

            ListByte listByte = new ListByte(CHUNK_SIZE / 5);

            int loopBound = species.loopBound(len);
            int i = 0;
            int last = 0;

            byte[] sbuff = new byte[species.length()];
            for (; i < loopBound; i += species.length()) {
                ByteVector v = ByteVector.fromMemorySegment(species, ms, i, ByteOrder.nativeOrder());
                var mask1 = v.compare(VectorOperators.EQ, '\n');
                var mask2 = v.compare(VectorOperators.EQ, ';');
                indexes.compress(mask1.or(mask2)).intoArray(sbuff, 0);
                last = pushIndexes(last, listByte, sbuff);
            }
            for (; i < len; i++) {
                if (ms.get(ValueLayout.JAVA_BYTE, i) == '\n' || ms.get(ValueLayout.JAVA_BYTE, i) == ';') {
                    indexes.add((byte) (i - loopBound + last + 1));
                    last = -i + loopBound - 1;
                }
            }

            int startLine = 0;
            for (int j = 0; j < listByte.size(); j += 2) {
                int commaLen = listByte.get(j);
                int nlLen = listByte.get(j + 1);
                int id = stations.getOrRegisterId(ms, startLine, commaLen - 1);
                startLine += commaLen;
                double value = parseDouble(ms, startLine, nlLen - 1);
                if (map[id] == null) {
                    map[id] = new MeasurementAggregator(value);
                }
                else {
                    map[id].merge(value);
                }
                startLine += nlLen;
            }
            latch.countDown();
        }
        return map;
    }

    private double parseDouble(MemorySegment ms, int start, int len) {
        int sign = 1;
        if (ms.get(ValueLayout.JAVA_BYTE, start) == '-') {
            sign = -1;
            start++;
            len--;
        }
        double value = 0;
        switch (len) {
            case 5:
                value += (ms.get(ValueLayout.JAVA_BYTE, start + len - 5) - '0') * 100;
            case 4:
                value += (ms.get(ValueLayout.JAVA_BYTE, start + len - 4) - '0') * 10;
            case 3:
                value += (ms.get(ValueLayout.JAVA_BYTE, start + len - 3) - '0');
            default:
                value += (ms.get(ValueLayout.JAVA_BYTE, start + len - 1) - '0') / 10.;
        }
        return sign * value;
    }

    private int pushIndexes(int prev, ListByte listByte, byte[] sbuff) {
        for (int i = 0; i < species.length(); i++) {
            listByte.add((byte) (sbuff[i] + 1 + prev));
            prev = -sbuff[i] - 1;
            if (sbuff[i] > sbuff[i + 1]) {
                break;
            }
        }
        return species.length() + prev;
    }

    private static final class ListByte {

        private final byte[] values;
        private int len;

        public ListByte(int capacity) {
            values = new byte[capacity];
            len = 0;
        }

        public void add(byte value) {
            values[len++] = value;
        }

        public int size() {
            return len;
        }

        public byte get(int pos) {
            return values[pos];
        }
    }

    private static final class NameRegistry {

        private final AtomicInteger idGenerator = new AtomicInteger(0);
        private final ConcurrentHashMap<Integer, List<Station>> hashMap = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, String> idMap = new ConcurrentHashMap<>();

        private NameRegistry() {
        }

        private int hash(MemorySegment ms, int start, int len) {
            len = Math.min(4, len);
            int end = start + len;
            int h = 1;
            for (int i = start; i < end; i++) {
                h = 31 * h + ms.get(ValueLayout.JAVA_BYTE, i);
            }
            return h;
        }

        private int getId(int hash, MemorySegment ms, int offset, int length) {
            List<Station> stations = hashMap.get(hash);
            if (stations == null) {
                return -1;
            }
            for (var station : stations) {
                if (station.sameBuffer(ms, offset, length)) {
                    return station.id;
                }
            }
            return -1;
        }

        private synchronized int registerId(int hash, MemorySegment ms, int offset, int length) {
            int id = getId(hash, ms, offset, length);
            if (id != -1) {
                return id;
            }
            id = idGenerator.getAndIncrement();
            byte[] b = ms.asSlice(offset, length).toArray(ValueLayout.JAVA_BYTE);
            if (!hashMap.containsKey(hash)) {
                hashMap.put(hash, new ArrayList<>());
            }
            idMap.put(id, new String(b));
            hashMap.get(hash).add(new Station(id, b));
            return id;
        }

        public int getOrRegisterId(MemorySegment ms, int offset, int length) {
            int hash = hash(ms, offset, length);
            int id = getId(hash, ms, offset, length);
            if (id < 0) {
                id = registerId(hash, ms, offset, length);
            }
            return id;
        }

        public String getStation(int i) {
            return idMap.get(i);
        }

        private record Station(int id, byte[] bytes) {

            public boolean sameBuffer(MemorySegment ms, int start, int len) {
                if (bytes.length != len) {
                    return false;
                }
                for (int i = 0; i < len; i++) {
                    if(bytes[i]!=ms.get(ValueLayout.JAVA_BYTE, start+i)) {
                        return false;
                    }
                }
                return true;
            }
        }
    }
}
