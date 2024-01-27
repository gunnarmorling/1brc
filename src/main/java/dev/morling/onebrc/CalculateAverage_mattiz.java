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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public class CalculateAverage_mattiz {
    private static final int TWO_BYTE_TO_INT = 480 + 48; // 48 is the ASCII code for '0'
    private static final int THREE_BYTE_TO_INT = 4800 + 480 + 48;
    private static final String FILE = "./measurements.txt";
    public static final int PARTS = 8;

    public static void main(String[] args) throws Exception {
        var result = new CalculateAverage_mattiz().calculate(FILE, PARTS);
        System.out.println(result);
    }

    StationList calculate(String file, int numParts) throws Exception {
        var buffers = createBuffers(Paths.get(file), numParts);

        return buffers
                .parallelStream()
                .map(this::aggregate)
                .reduce(StationList::merge)
                .orElseThrow();
    }

    record BufferAndSize(ByteBuffer buffer, long size) {
    }

    List<ByteBuffer> createBuffers(Path file, int numParts) throws IOException {
        FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ);

        var fileSize = fileChannel.size();

        if (fileSize < (1024 * 1024)) { // Only one core for small files
            numParts = 1;
        }

        var chunkSize = fileSize / numParts;
        var buffers = new ArrayList<ByteBuffer>();
        long filePointer = 0;

        for (int i = 0; i < numParts; i++) {
            if (i != numParts - 1) { // not last element
                var adjustedChunkSize = getBuffer(fileChannel, filePointer, chunkSize, true);
                buffers.add(adjustedChunkSize.buffer());
                filePointer += adjustedChunkSize.size();
            }
            else {
                var adjustedChunkSize = getBuffer(fileChannel, filePointer, fileSize - filePointer, false);
                buffers.add(adjustedChunkSize.buffer());
            }
        }

        return buffers;
    }

    BufferAndSize getBuffer(FileChannel fileChannel, long start, long size, boolean adjust) throws IOException {
        MappedByteBuffer buffer = fileChannel.map(READ_ONLY, start, size);

        var actualSize = ((int) size);

        if (adjust) {
            while (buffer.get(actualSize - 1) != '\n') {
                actualSize--;
            }
        }

        buffer.limit(actualSize);

        return new BufferAndSize(buffer, actualSize);
    }

    private StationList aggregate(ByteBuffer buffer) {
        var measurements = new StationList();

        while (buffer.hasRemaining()) {
            int startPos = buffer.position();

            byte b;
            int hash = 0;
            while ((b = buffer.get()) != ';') {
                hash = ((hash << 5) - hash) + b;
            }

            if (hash < 0) {
                hash = -hash;
            }

            int length = buffer.position() - startPos - 1;
            byte[] station = new byte[length];
            buffer.get(startPos, station);

            int value = readValue(buffer);

            measurements.update(station, length, hash, value);
        }

        return measurements;
    }

    /*
     * Read decimal number from ascii characters (copied from arjenw)
     *
     * Example:
     * If you have the decimal number 1.4,
     * then byte 1 contain 49 (ascii code for '1')
     * and byte 3 contain 52 (ascii code for '4')
     * Subtract 480 + 48 (48 is the ASCII code for '0')
     * to move number from ascii number to int
     *
     * 49 * 10 + 52 - 528 = 14
     */
    private static int readValue(ByteBuffer buffer) {
        int value;
        byte b1 = buffer.get();
        byte b2 = buffer.get();
        byte b3 = buffer.get();
        byte b4 = buffer.get();

        if (b2 == '.') {// value is n.n
            value = (b1 * 10 + b3 - TWO_BYTE_TO_INT);
        }
        else {
            if (b4 == '.') { // value is -nn.n
                value = -(b2 * 100 + b3 * 10 + buffer.get() - THREE_BYTE_TO_INT);
            }
            else if (b1 == '-') { // value is -n.n
                value = -(b2 * 10 + b4 - TWO_BYTE_TO_INT);
            }
            else { // value is nn.n
                value = (b1 * 100 + b2 * 10 + b4 - THREE_BYTE_TO_INT);
            }
            buffer.get(); // new line
        }
        return value;
    }
}

class CustomMap {
    private static final int SIZE = 1024 * 64;
    private final Station[] stationList = new Station[SIZE];

    public void addOrUpdate(byte[] stationName, int length, int hash, int value) {
        int slot = hash & (SIZE - 1);
        var station = stationList[slot];

        while (station != null
                && station.getHash() != hash
                && !Arrays.equals(
                        station.getName(), 0, station.getName().length,
                        stationName, 0, length)) {

            slot = (slot + 1) & (SIZE - 1);
            station = stationList[slot];
        }

        if (station == null) {
            stationList[slot] = new Station(stationName, hash);
        }

        stationList[slot].add(value);
    }

    public Station get(byte[] stationName) {
        return stationList[findSlot(stationName)];
    }

    public void put(byte[] stationName, Station newStation) {
        stationList[findSlot(stationName)] = newStation;
    }

    private int findSlot(byte[] stationName) {
        int hash = getHash(stationName);
        int slot = hash & (SIZE - 1);
        var station = stationList[slot];

        while (station != null
                && station.getHash() != hash
                && !Arrays.equals(station.getName(), stationName)) {

            slot = (slot + 1) & (SIZE - 1);
            station = stationList[slot];
        }

        return slot;
    }

    private int getHash(byte[] key) {
        int hash = 0;

        for (byte b : key) {
            hash = hash * 31 + b;
        }

        if (hash < 0) {
            hash = -hash;
        }

        return hash;
    }

    public Set<Map.Entry<byte[], Station>> entrySet() {
        var sorted = new HashMap<byte[], Station>();

        for (var s : stationList) {
            if (s != null) {
                sorted.put(s.getName(), s);
            }
        }

        return sorted.entrySet();
    }

    public Map<String, Station> sorted() {
        var sorted = new TreeMap<String, Station>();

        for (var s : stationList) {
            if (s != null) {
                sorted.put(new String(s.getName(), StandardCharsets.UTF_8), s);
            }
        }

        return sorted;
    }
}

class StationList {
    private final CustomMap stations = new CustomMap();

    public void update(byte[] stationName, int length, int hash, int value) {
        stations.addOrUpdate(stationName, length, hash, value);
    }

    public StationList merge(StationList other) {
        for (var aggregator : other.stations.entrySet()) {
            var agg = stations.get(aggregator.getKey());

            if (agg == null) {
                stations.put(aggregator.getKey(), aggregator.getValue());
            }
            else {
                agg.merge(aggregator.getValue());
            }
        }

        return this;
    }

    @Override
    public String toString() {
        return stations.sorted().toString();
    }
}

class Station {
    private final byte[] name;
    private final int hash;
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private int sum;
    private int count;

    public Station(byte[] name, int hash) {
        this.name = name;
        this.hash = hash;
    }

    public void add(int max, int min, int sum, int count) {
        this.max = Math.max(this.max, max);
        this.min = Math.min(this.min, min);
        this.sum += sum;
        this.count += count;
    }

    public void add(int value) {
        this.max = Math.max(this.max, value);
        this.min = Math.min(this.min, value);
        this.sum += value;
        this.count++;
    }

    public void merge(Station other) {
        this.max = Math.max(this.max, other.max);
        this.min = Math.min(this.min, other.min);
        this.sum += other.sum;
        this.count += other.count;
    }

    public String toString() {
        return (min / 10.0) + "/" + (Math.round(((double) sum) / count)) / 10.0 + "/" + (max / 10.0);
    }

    public byte[] getName() {
        return name;
    }

    public int getHash() {
        return hash;
    }
}