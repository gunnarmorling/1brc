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

import org.radughiorma.Arguments;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_seijikun {

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public void printInto(PrintStream out) {
            out.printf("%.1f/%.1f/%.1f", min, (sum / (double) count), max);
        }
    }

    public static class StationIdent implements Comparable<StationIdent> {
        private final int nameLength;
        private final String name;
        private final int nameHash;

        public StationIdent(byte[] name, int nameHash) {
            this.nameLength = name.length;
            this.name = new String(name);
            this.nameHash = nameHash;
        }

        @Override
        public int hashCode() {
            return nameHash;
        }

        @Override
        public boolean equals(Object obj) {
            var other = (StationIdent) obj;
            if (other.nameLength != nameLength) {
                return false;
            }
            return name.equals(other.name);
        }

        @Override
        public int compareTo(StationIdent o) {
            return name.compareTo(o.name);
        }
    }

    public static class ChunkReader implements Runnable {
        RandomAccessFile file;

        // Start offset of this chunk
        private final long startOffset;
        // end offset of this chunk
        private final long endOffset;

        // state
        private MappedByteBuffer buffer = null;
        private int ptr = 0;
        private TreeMap<StationIdent, MeasurementAggregator> workSet;

        public ChunkReader(RandomAccessFile file, long startOffset, long endOffset) {
            this.file = file;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        private StationIdent readStationName() {
            int startPtr = ptr;
            int hashCode = 0;
            int hashBytePtr = 0;
            byte c;
            while ((c = buffer.get(ptr++)) != ';') {
                hashCode ^= ((int) c) << (hashBytePtr * 8);
                hashBytePtr = (hashBytePtr + 1) % 4;
            }
            byte[] stationNameBfr = new byte[ptr - startPtr - 1];
            buffer.get(startPtr, stationNameBfr);
            return new StationIdent(stationNameBfr, hashCode);
        }

        private double readTemperature() {
            double ret = 0, div = 1;
            byte c = buffer.get(ptr++);
            boolean neg = (c == '-');
            if (neg)
                c = buffer.get(ptr++);

            do {
                ret = ret * 10 + c - '0';
            } while ((c = buffer.get(ptr++)) >= '0' && c <= '9');

            if (c == '.') {
                while ((c = buffer.get(ptr++)) != '\n') {
                    ret += (c - '0') / (div *= 10);
                }
            }

            if (neg)
                return -ret;
            return ret;
        }

        @Override
        public void run() {
            workSet = new TreeMap<>();
            int chunkSize = (int) (endOffset - startOffset);
            try {
                buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, startOffset, chunkSize);

                while (ptr < chunkSize) {
                    var station = readStationName();
                    var temp = readTemperature();
                    var stationWorkSet = workSet.get(station);
                    if (stationWorkSet == null) {
                        stationWorkSet = new MeasurementAggregator();
                        workSet.put(station, stationWorkSet);
                    }
                    stationWorkSet.min = Math.min(temp, stationWorkSet.min);
                    stationWorkSet.max = Math.max(temp, stationWorkSet.max);
                    stationWorkSet.sum += temp;
                    stationWorkSet.count += 1;
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RandomAccessFile file = new RandomAccessFile(Arguments.measurmentsFilename(args), "r");

        int jobCnt = Runtime.getRuntime().availableProcessors();

        var chunks = new ChunkReader[jobCnt];
        long chunkSize = file.length() / jobCnt;
        long chunkStartPtr = 0;
        byte[] tmpBuffer = new byte[128];
        for (int i = 0; i < jobCnt; ++i) {
            long chunkEndPtr = chunkStartPtr + chunkSize;
            if (i != (jobCnt - 1)) { // align chunks to newlines
                file.seek(chunkEndPtr - 1);
                file.read(tmpBuffer);
                int offset = 0;
                while (tmpBuffer[offset] != '\n') {
                    offset += 1;
                }
                chunkEndPtr += offset;
            }
            else { // last chunk ends at file end
                chunkEndPtr = file.length();
            }
            chunks[i] = new ChunkReader(file, chunkStartPtr, chunkEndPtr);
            chunkStartPtr = chunkEndPtr;
        }

        try (var executor = Executors.newFixedThreadPool(jobCnt)) {
            for (int i = 0; i < jobCnt; ++i) {
                executor.submit(chunks[i]);
            }
            executor.shutdown();
            var ignored = executor.awaitTermination(1, TimeUnit.DAYS);
        }

        // merge chunks
        var result = chunks[0].workSet;
        for (int i = 1; i < jobCnt; ++i) {
            chunks[i].workSet.forEach((ident, otherStationWorkSet) -> {
                var stationWorkSet = result.get(ident);
                if (stationWorkSet == null) {
                    result.put(ident, otherStationWorkSet);
                }
                else {
                    stationWorkSet.min = Math.min(stationWorkSet.min, otherStationWorkSet.min);
                    stationWorkSet.max = Math.max(stationWorkSet.max, otherStationWorkSet.max);
                    stationWorkSet.sum += otherStationWorkSet.sum;
                    stationWorkSet.count += otherStationWorkSet.count;
                }
            });
        }

        // print in required format
        System.out.write('{');
        var iterator = result.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            System.out.print(entry.getKey().name);
            System.out.write('=');
            entry.getValue().printInto(System.out);
            if (iterator.hasNext()) {
                System.out.print(", ");
            }
        }
        System.out.println('}');
    }
}
