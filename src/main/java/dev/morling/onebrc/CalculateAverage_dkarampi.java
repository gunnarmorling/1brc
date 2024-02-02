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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public class CalculateAverage_dkarampi {
    private static final String FILE = "./measurements.txt";
    private static final int BUFFER_SIZE = (1 << 29); // 500mb
    private static final int HT_SIZE = nextPowerOfTwo(10000);
    private static final int NUM_THREADS = 8;
    private final List<Station[]> stationHashTables = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        new CalculateAverage_dkarampi().runFast();
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static boolean areEqual(byte[] a, int aLen, byte[] b, int bLen) {
        if (aLen != bLen) {
            return false;
        }
        for (byte i = 0; i < aLen; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static int nextPowerOfTwo(int n) {
        for (int i = 1; i < 32; i <<= 1) {
            n |= n >> i;
        }
        return n + 1;
    }

    private void runFast() throws Exception {
        createStationHashTables();
        FileChannel channel = FileChannel.open(Path.of(FILE));

        List<List<Buffer>> buffersList = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            buffersList.add(new ArrayList<>());
        }

        List<Buffer> buffers = createBuffers(channel);
        for (int i = 0; i < buffers.size(); i++) {
            buffersList.get(i % NUM_THREADS).add(buffers.get(i));
        }

        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(new Task(stationHashTables.get(i), buffersList.get(i)));
        }

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        Future<?>[] futures = new Future<?>[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            futures[i] = executorService.submit(tasks.get(i));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        sortAndPrint();

        executorService.shutdown();
        channel.close();
    }

    private void createStationHashTables() {
        for (int i = 0; i < NUM_THREADS; i++) {
            Station[] stationsHashTable = new Station[HT_SIZE];
            for (int j = 0; j < HT_SIZE; j++) {
                stationsHashTable[j] = new Station();
            }
            stationHashTables.add(stationsHashTable);
        }
    }

    private List<Buffer> createBuffers(FileChannel channel) throws Exception {
        List<Buffer> buffers = new ArrayList<>();
        long size = channel.size();
        int lastByte;
        for (long offset = 0; offset < size; offset += lastByte + 1) {
            long sizeToMap = Math.min(size - offset, BUFFER_SIZE);
            MappedByteBuffer buffer = channel.map(READ_ONLY, offset, sizeToMap);
            lastByte = (int) sizeToMap - 1;
            while (buffer.get(lastByte) != '\n')
                --lastByte;
            buffers.add(new Buffer(buffer, lastByte + 1));
        }
        return buffers;
    }

    private void sortAndPrint() {
        TreeMap<String, Station> sortedStations = new TreeMap<>();

        for (Station[] stationHashTable : stationHashTables) {
            for (Station station : stationHashTable) {
                if (station.freq == 0) {
                    continue;
                }
                String key = new String(station.name, 0, station.nameLen);
                Station st = sortedStations.get(key);
                if (st == null) {
                    sortedStations.put(key, station);
                }
                else {
                    st.min = Math.min(st.min, station.min);
                    st.max = Math.max(st.max, station.max);
                    st.sum += station.sum;
                    st.freq += station.freq;
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<String, Station> entry : sortedStations.entrySet()) {
            String name = entry.getKey();
            Station station = entry.getValue();
            sb.append(name);
            sb.append("=");
            sb.append(round(station.min));
            sb.append("/");
            sb.append(round(round(station.sum) / station.freq));
            sb.append("/");
            sb.append(round(station.max));
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("}");
        System.out.println(sb);
    }

    private record Buffer(ByteBuffer byteBuffer, int length) {
    }

    private static class Station {
        double sum;
        double min = 100;
        double max = -100;
        int freq;
        short nameLen;
        byte[] name = new byte[nextPowerOfTwo(100)];
    }

    private record Task(Station[] stations, List<Buffer> buffers) implements Runnable {

    @Override
    public void run() {
        for (Buffer buffer : buffers) {
            process(buffer);
        }
    }

    private void process(Buffer buffer) {
        short nameLen = 0;
        int hash = 5381;
        int temperature;
        byte[] name = new byte[100];

        for (int i = 0; i < buffer.length; i++) {
            byte c = buffer.byteBuffer.get(i);
            if (c == ';') {
                int sign = 1;
                c = buffer.byteBuffer.get(++i);
                if (c == '-') {
                    sign = -1;
                    c = buffer.byteBuffer.get(++i);
                    temperature = (c - '0') * 10;
                    c = buffer.byteBuffer.get(++i);
                    if (c == '.') {
                        c = buffer.byteBuffer.get(++i);
                        temperature = temperature + c - '0';
                    }
                    else {
                        temperature = temperature + c - '0';
                        ++i; // dot
                        c = buffer.byteBuffer.get(++i);
                        temperature = temperature * 10 + c - '0';
                    }
                }
                else {
                    temperature = (c - '0') * 10;
                    c = buffer.byteBuffer.get(++i);
                    if (c == '.') {
                        c = buffer.byteBuffer.get(++i);
                        temperature = temperature + c - '0';
                    }
                    else {
                        temperature = temperature + c - '0';
                        ++i; // dot
                        c = buffer.byteBuffer.get(++i);
                        temperature = temperature * 10 + c - '0';
                    }
                }
                hash = hash & 0x7FFFFFFF;
                updateStations(hash, name, nameLen, sign * (double) temperature / 10);
                ++i; // For '\n'
                nameLen = 0;
                hash = 5383;
            }
            else {
                name[nameLen++] = c;
                hash = ((hash << 5) + hash) + c;
            }
        }
    }

    private void updateStations(int hash, byte[] name, short nameLen, double temperature) {
        int idx;
        for (idx = hash % HT_SIZE; stations[idx].freq != 0; idx = (idx + 1) % HT_SIZE) {
            if (areEqual(stations[idx].name, stations[idx].nameLen, name, nameLen)) {
                stations[idx].sum += temperature;
                stations[idx].min = Math.min(stations[idx].min, temperature);
                stations[idx].max = Math.max(stations[idx].max, temperature);
                ++stations[idx].freq;
                return;
            }
        }
        stations[idx].sum = temperature;
        stations[idx].min = temperature;
        stations[idx].max = temperature;
        stations[idx].nameLen = nameLen;
        System.arraycopy(name, 0, stations[idx].name, 0, nameLen);
        stations[idx].freq = 1;
    }
}}
