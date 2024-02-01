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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class CalculateAverage_yglcode {
    public static final int NumStation = 10240; // max num of stations
    public static final int MaxDataLen = 32; // max bytes for station name or temperature data
    public static final int MinChunkSize = 16 * 1024 * 1024; // min chunk size
    private static final String FILE = "./measurements.txt";

    // hashmap<byte[],int>: byte[] keys are stored externally
    // use this to map station names to data index
    private static class BytesToIntMap {
        private byte[] keyStore; // external key store
        private int mapSize;
        private int numEntries;
        private int ktail; // index just past last key
        private int[] kstart, kend; // [start,end] indices of keys in store
        private int[] values; // int values of keys
        private int[] res = new int[2]; // getOrPut() return value

        public BytesToIntMap(int maxSize, byte[] keyStore) {
            this.keyStore = keyStore;
            // this map is not expandable and insert-only, add enough space
            mapSize = maxSize * 4;
            numEntries = 0;
            kstart = new int[mapSize];
            kend = new int[mapSize];
            values = new int[mapSize];
            for (int i = 0; i < mapSize; i++)
                values[i] = -1;
        }

        // return: int[0] - mapped index, int[1]: -1 - new index, otherwise existing
        public int[] getOrPut(byte[] keys, int start, int end, int hash) {
            int slot = hash & (mapSize - 1);
            int ks = kstart[slot], ke = kend[slot];
            // Linear probe for open slot
            while (values[slot] >= 0 && ((ke - ks) != (end - start) || !Arrays.equals(keyStore, ks, ke, keys, start, end))) {
                slot = (slot + 1) & (mapSize - 1);
                ks = kstart[slot];
                ke = kend[slot];
            }
            res[1] = 1; // use 1 mark existing key by def
            if (values[slot] < 0) {
                values[slot] = numEntries++;
                if (keyStore != keys) { // diff keyStores, happend when merge
                    int len = end - start;
                    System.arraycopy(keys, start, keyStore, ktail, len);
                    start = ktail;
                    end = start + len;
                }
                ktail = end;
                kstart[slot] = start;
                kend[slot] = end;
                // use -1 mark a new entry in result
                res[1] = -1;
            }
            res[0] = values[slot];
            return res;
        }

        public record Entry(int start, int end, int value) {
        };

        // get all entries
        public List<Entry> getEntries() {
            var res = new ArrayList<Entry>(numEntries);
            for (int i = 0; i < mapSize; i++) {
                if (values[i] >= 0) {
                    res.add(new Entry(kstart[i], kend[i], values[i]));
                }
            }
            return res;
        }
    }

    // a processor runs at a core processing a chunk of file
    private static class Processor implements Runnable {
        // input data: assigned file chunk
        MemorySegment mbbuf;
        long head, tail;
        int numStation; // max num of stations
        // processing state
        // all temperature metrics
        long[] data;
        // local buffer of scratch + keys;
        byte[] buf;
        // indices mapping key/name to index into data array
        BytesToIntMap indices;

        public Processor(MemorySegment mbbuf, long head, long tail, int numStation) {
            this.mbbuf = mbbuf;
            this.head = head;
            this.tail = tail;
            this.numStation = numStation;

            // init processor state
            data = new long[numStation * 4];
            for (int i = 0; i < numStation; i++) {
                int idx = i * 4;
                data[idx] = Integer.MAX_VALUE;
                data[idx + 1] = Integer.MIN_VALUE;
            }
            buf = new byte[MaxDataLen * (numStation + 1)];
            indices = new BytesToIntMap(numStation, buf);
        }

        public void addMetric(int idx, long temperature) {
            int hd = idx * 4;
            data[hd] = Math.min(data[hd], temperature);
            data[hd + 1] = Math.max(data[hd + 1], temperature);
            data[hd + 2] += temperature;
            data[hd + 3] += 1;
        }

        public void mergeMetric(int idx, long[] data2, int idx2) {
            int hd = idx * 4;
            int hd2 = idx2 * 4;
            data[hd] = Math.min(data[hd], data2[hd2]);
            data[hd + 1] = Math.max(data[hd + 1], data2[hd2 + 1]);
            data[hd + 2] += data2[hd2 + 2];
            data[hd + 3] += data2[hd2 + 3];
        }

        // Process all lines in buffer [head, end)
        public void run() {
            int start = 0, pos = 0; // [start,pos]: buf area where mbbuf bytes are checked
            int idx = -1;
            long offset = head; // next byte in mbbuf
            while (offset < tail) {
                int hash = 1;
                while (offset < tail && (buf[pos] = mbbuf.get(ValueLayout.JAVA_BYTE, offset++)) != ';') {
                    hash = 31 * hash + buf[pos];
                    pos++;
                }
                var res = indices.getOrPut(buf, start, pos, hash);
                idx = res[0];
                if (res[1] < 0) { // new entry to add, move to next
                    start = pos;
                }
                else { // reset scratch
                    pos = start;
                }
                // read temperature
                // credit: CalculateAverage_merykitty.java
                short value = 0;
                byte b;
                boolean negative = false;
                if (mbbuf.get(ValueLayout.JAVA_BYTE, offset) == '-') {
                    negative = true;
                    offset++;
                }
                while (offset < tail && (b = mbbuf.get(ValueLayout.JAVA_BYTE, offset)) != '\n') {
                    if (b == '.') {
                        b = mbbuf.get(ValueLayout.JAVA_BYTE, offset + 1);
                        value = (short) (value * 10 + (b - '0'));
                        offset += 3;
                        break;
                    }
                    value = (short) (value * 10 + (short) (b - '0'));
                    offset++;
                }
                value = (short) (negative ? -value : value);
                addMetric(idx, value);
                pos = start; // reset scratch
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        int processorCnt = Runtime.getRuntime().availableProcessors();
        TreeMap<String, Metrics> res;
        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
                var arena = Arena.ofShared()) {
            var data = file.map(MapMode.READ_ONLY, 0, file.size(), arena);
            long chunkSize = Math.ceilDiv(data.byteSize(), processorCnt);
            if (chunkSize < MinChunkSize) {
                processorCnt = 1;
                chunkSize = data.byteSize();
            }
            var threadList = new Thread[processorCnt];
            var procList = new Processor[processorCnt];
            // start/end of each chunk
            long start = 0, end = 0;
            for (int i = 0; i < processorCnt; i++) {
                start = end;
                end = Math.min((i + 1) * chunkSize, data.byteSize());
                while (data.get(ValueLayout.JAVA_BYTE, end - 1) != '\n') {
                    --end;
                }
                procList[i] = new Processor(data, start, end, NumStation);
                threadList[i] = new Thread(procList[i]);
                threadList[i].start();
            }

            for (var thread : threadList) {
                thread.join();
            }
            res = merge(procList);
        }
        System.out.println(res);
    }

    private record Metrics(long min,long max,long sum,long count) {
        public String toString() {
            return round(min / 10.) + "/" + round(sum / (double) (10 * count)) + "/" + round(max / 10.);
        }
        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static int hashCode(byte[] data, int hd, int tl) {
        int res = 1;
        for (int i = hd; i < tl; i++) {
            res = 31 * res + data[i];
        }
        return res;
    }

    private static TreeMap<String, Metrics> merge(Processor[] procList) {
        var proc0 = procList[0];
        for (int i = 1; i < procList.length; i++) {
            var proc1 = procList[i];
            for (var ent : proc1.indices.getEntries()) {
                int hd = ent.value();
                int hash = hashCode(proc1.buf, ent.start(), ent.end());
                var res = proc0.indices.getOrPut(proc1.buf, ent.start(), ent.end(), hash);
                proc0.mergeMetric(res[0], proc1.data, hd);
            }
        }
        var res = new TreeMap<String, Metrics>();
        for (var ent : proc0.indices.getEntries()) {
            int hd = ent.value() * 4;
            res.put(new String(proc0.buf, ent.start(), ent.end() - ent.start()),
                    new Metrics(proc0.data[hd], proc0.data[hd + 1], proc0.data[hd + 2], proc0.data[hd + 3]));
        }
        return res;
    }
}
