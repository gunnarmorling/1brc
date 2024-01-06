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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_seijikun {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        // final long startTs = System.currentTimeMillis();
        private long sum = 0;
        private long count = 0;

        private double mean = 0;

        public void finish() {
            double sum = this.sum / 10.0;
            mean = sum / (double) count;
        }

        public void printInto(PrintStream out) {
            double min = (double) this.min / 10.0;
            double max = (double) this.max / 10.0;
            out.printf("%.1f/%.1f/%.1f", min, mean, max);
        }
    }

    public static class StationIdent {
        private final byte[] name;
        private final int nameHash;

        public StationIdent(byte[] name, int nameHash) {
            this.name = name;
            this.nameHash = nameHash;
        }

        @Override
        public int hashCode() {
            return nameHash;
        }

        @Override
        public boolean equals(Object obj) {
            var other = (StationIdent) obj;
            if (other.name.length != name.length) {
                return false;
            }
            return Arrays.equals(name, other.name);
        }
    }

    public static class ChunkReader implements Runnable {
        RandomAccessFile file;

        // Start offset of this chunk
        private final long startOffset;
        // end offset of this chunk
        private final long endOffset;

        // state
        private int chunkSize = 0;
        private MappedByteBuffer buffer = null;
        private MemorySegment memorySegment = null;
        private int ptr = 0;
        private HashMap<StationIdent, MeasurementAggregator> workSet;

        public ChunkReader(RandomAccessFile file, long startOffset, long endOffset) {
            this.file = file;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        // private StationIdent readStationName() {
        // int startPtr = ptr;
        // int hashCode = 0;
        // int hashBytePtr = 0;
        // byte c;
        // while ((c = buffer.get(ptr++)) != ';') {
        // hashCode ^= ((int) c) << (hashBytePtr * 8);
        // hashBytePtr = (hashBytePtr + 1) % 4;
        // }
        // byte[] stationNameBfr = new byte[ptr - startPtr - 1];
        // buffer.get(startPtr, stationNameBfr);
        // return new StationIdent(stationNameBfr, hashCode);
        // }

        private StationIdent readStationName() {
            final var VECTOR_SPECIES = ByteVector.SPECIES_256;

            if (chunkSize - ptr - 100 < VECTOR_SPECIES.length()) { // fallback
                int startPtr = ptr;
                while (buffer.get(ptr++) != ';') {
                }
                byte[] stationNameBfr = new byte[ptr - startPtr - 1];
                buffer.get(startPtr, stationNameBfr);
                return new StationIdent(stationNameBfr, Arrays.hashCode(stationNameBfr) ^ stationNameBfr.length);
            }
            else { // SIMD
                int sepIdx = 0;

                while (true) {
                    ByteVector tmp = ByteVector.fromMemorySegment(VECTOR_SPECIES, memorySegment, ptr + sepIdx, ByteOrder.LITTLE_ENDIAN);
                    final var cmpResult = tmp.compare(VectorOperators.EQ, ';');
                    if (cmpResult.anyTrue()) {
                        sepIdx += cmpResult.firstTrue();
                        break;
                    }
                    else {
                        sepIdx += tmp.length();
                    }
                }

                int endPtr = ptr + sepIdx;
                byte[] stationNameBfr = new byte[endPtr - ptr];
                buffer.get(ptr, stationNameBfr);
                ptr = endPtr + 1;
                return new StationIdent(stationNameBfr, Arrays.hashCode(stationNameBfr) ^ stationNameBfr.length);
            }
        }

        private int readTemperature() {
            int ret = 0;
            byte c = buffer.get(ptr++);
            final boolean neg = (c == '-');
            if (neg) {
                c = buffer.get(ptr++);
            }

            do {
                if (c != '.') {
                    ret = ret * 10 + c - '0';
                }
            } while ((c = buffer.get(ptr++)) != '\n');

            if (neg)
                return -ret;
            return ret;
        }

        @Override
        public void run() {
            workSet = new HashMap<>();
            if (endOffset - startOffset > Integer.MAX_VALUE) {
                throw new RuntimeException("Mapping a block larger than 2GB is not possible with Java! Welcome to 2024 :)");
            }
            chunkSize = (int) (endOffset - startOffset);
            try {
                buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, startOffset, chunkSize);
                memorySegment = MemorySegment.ofBuffer(buffer);

                while (ptr < chunkSize) {
                    var station = readStationName();
                    int temp = readTemperature();
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
            catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private static void printWorkSet(TreeMap<String, MeasurementAggregator> result, PrintStream out) {
        out.write('{');
        final var iterator = result.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            out.print(entry.getKey());
            out.write('=');
            entry.getValue().printInto(out);
            if (iterator.hasNext()) {
                out.print(", ");
            }
        }
        out.println('}');
    }

    private static int createChunks(final RandomAccessFile file, final ChunkReader[] chunks) throws IOException {
        final long fileEndPtr = file.length();
        final long chunkSize = Math.max(1, fileEndPtr / chunks.length);

        int jobCnt = 0;
        long chunkStartPtr = 0;
        final byte[] tmpBuffer = new byte[128];
        while (chunkStartPtr < fileEndPtr) {
            long chunkEndPtr = Math.min(chunkStartPtr + chunkSize, fileEndPtr);

            // Seek into file at the calculated chunk end ptr, then extend it until the next
            // new-line or EOF
            if (chunkEndPtr < fileEndPtr) {
                file.seek(Math.max(0, chunkEndPtr - 1));
                file.read(tmpBuffer);
                int offset = 0;
                while (tmpBuffer[offset] != '\n') {
                    offset += 1;
                }
                chunkEndPtr += offset;
            }

            chunks[jobCnt] = new ChunkReader(file, chunkStartPtr, chunkEndPtr);
            jobCnt += 1;
            chunkStartPtr = chunkEndPtr;
        }
        return jobCnt;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final RandomAccessFile file = new RandomAccessFile(FILE, "r");

        int jobCnt = Runtime.getRuntime().availableProcessors();

        final var chunks = new ChunkReader[jobCnt];
        jobCnt = createChunks(file, chunks);

        try (final var executor = Executors.newFixedThreadPool(jobCnt)) {
            for (int i = 0; i < jobCnt; ++i) {
                executor.submit(chunks[i]);
            }
            executor.shutdown();
            final var ignored = executor.awaitTermination(1, TimeUnit.DAYS);
        }

        // merge chunks
        final var result = new TreeMap<String, MeasurementAggregator>();
        for (int i = 0; i < jobCnt; ++i) {
            chunks[i].workSet.forEach((ident, otherStationWorkSet) -> {
                final var identStr = new String(ident.name);
                final var stationWorkSet = result.get(identStr);
                if (stationWorkSet == null) {
                    result.put(identStr, otherStationWorkSet);
                }
                else {
                    stationWorkSet.min = Math.min(stationWorkSet.min, otherStationWorkSet.min);
                    stationWorkSet.max = Math.max(stationWorkSet.max, otherStationWorkSet.max);
                    stationWorkSet.sum += otherStationWorkSet.sum;
                    stationWorkSet.count += otherStationWorkSet.count;
                }
            });
        }
        result.forEach((ignored, meas) -> meas.finish());

        // print in required format
        printWorkSet(result, System.out);
    }
}
