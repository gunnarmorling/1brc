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
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CalculateAverage_gnabyl {

    private static final String FILE = "./measurements.txt";

    private static final int NB_CHUNKS = Runtime.getRuntime().availableProcessors();

    private static Map<Integer, String> stationNameMap = new ConcurrentHashMap<>(10000, 0.9f, NB_CHUNKS);

    private static record Chunk(int bytesCount, MappedByteBuffer mappedByteBuffer) {
    }

    private static int reduceSizeToFitLineBreak(FileChannel channel, long startPosition, int startSize)
            throws IOException {
        long currentPosition = startPosition + startSize - 1;
        int realSize = startSize;

        if (currentPosition >= channel.size()) {
            currentPosition = channel.size() - 1;
            realSize = (int) (currentPosition - startPosition);
        }

        while (currentPosition >= startPosition) {
            channel.position(currentPosition);
            byte byteValue = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition, 1).get();
            if (byteValue == '\n') {
                // found line break
                break;
            }

            realSize--;
            currentPosition--;
        }
        return realSize;
    }

    private static List<Chunk> readChunks(int nbChunks) throws IOException {
        RandomAccessFile file = new RandomAccessFile(FILE, "rw");
        List<Chunk> res = new ArrayList<>(nbChunks);
        FileChannel channel = file.getChannel();
        long bytesCount = channel.size();
        long bytesPerChunk = bytesCount / nbChunks;

        // Memory map the file in read-only mode
        // TODO: Optimize using threads
        long currentPosition = 0;
        int startSize;
        int realSize;
        for (int i = 0; i < nbChunks; i++) {
            startSize = (int) bytesPerChunk;
            realSize = startSize;

            if (i == nbChunks - 1) {
                realSize = (int) (bytesCount - currentPosition);
                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
                        realSize);

                res.add(new Chunk(realSize, mappedByteBuffer));
                break;
            }

            // Adjust size so that it ends on a newline
            realSize = reduceSizeToFitLineBreak(channel, currentPosition, startSize);

            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
                    realSize);

            res.add(new Chunk(realSize, mappedByteBuffer));
            currentPosition += realSize;
        }

        channel.close();
        file.close();

        return res;
    }

    private static class StationData {
        private float sum, min, max;
        private int count;

        public StationData(float value) {
            this.count = 1;
            this.sum = value;
            this.min = value;
            this.max = value;
        }

        public void update(float value) {
            this.count++;
            this.sum += value;
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
        }

        public float getMean() {
            return sum / count;
        }

        public float getMin() {
            return min;
        }

        public float getMax() {
            return max;
        }

        public void mergeWith(StationData other) {
            this.sum += other.sum;
            this.count += other.count;
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
        }

    }

    static float round(float value) {
        return Math.round(value * 10.0f) * 0.1f;
    }

    private static class ChunkResult {
        private Map<Integer, StationData> data;

        public ChunkResult() {
            data = new HashMap<>();
        }

        public StationData getData(int hash) {
            return data.get(hash);
        }

        public void addStation(int hash, float value) {
            this.data.put(hash, new StationData(value));
        }

        public void print() {
            PrintWriter out = new PrintWriter(System.out);
            out.println(
                    this.data.keySet().parallelStream()
                            .map(hash -> {
                                var stationData = data.get(hash);
                                var name = stationNameMap.get(hash);
                                return String.format("%s=%.1f/%.1f/%.1f", name, round(stationData.getMin()),
                                        round(stationData.getMean()),
                                        round(stationData.getMax()));
                            })
                            .sorted((a, b) -> a.split("=")[0].compareTo(b.split("=")[0]))
                            .collect(Collectors.joining(", ", "{", "}")));
            out.flush();
        }

        public void mergeWith(ChunkResult other) {
            for (Map.Entry<Integer, StationData> entry : other.data.entrySet()) {
                int stationName = entry.getKey();
                StationData otherStationData = entry.getValue();
                StationData thisStationData = this.data.get(stationName);

                if (thisStationData == null) {
                    this.data.put(stationName, otherStationData);
                }
                else {
                    thisStationData.mergeWith(otherStationData);
                }
            }
        }
    }

    private static ChunkResult processChunk(Chunk chunk) {
        ChunkResult result = new ChunkResult();

        // Perform processing on the chunk data
        byte[] data = new byte[chunk.bytesCount()];
        chunk.mappedByteBuffer().get(data);

        // Process each line
        float value;
        int iSplit, iEol;
        StationData stationData;
        int negative;
        int hash, prime = 31;
        Set<Integer> seenHashes = new HashSet<>(10000, 0.9f);
        for (int offset = 0; offset < data.length; offset++) {
            // Find station name
            hash = 0;
            for (iSplit = offset; data[iSplit] != ';'; iSplit++) {
                hash = (hash << 5) - hash + (data[iSplit] & 0xFF);
            }
            if (!seenHashes.contains(hash)) {
                seenHashes.add(hash);
                stationNameMap.put(hash, new String(data, offset, iSplit - offset, StandardCharsets.UTF_8));
            }

            // Find value
            iSplit++;
            negative = 1;
            value = 0;
            for (iEol = iSplit; data[iEol] != '\n'; iEol++) {
                if (data[iEol] == '-') {
                    negative = -1;
                    continue;
                }
                if (data[iEol] == '.') {
                    value = value + (data[iEol + 1] - 48) * 0.1f;
                    iEol += 2;
                    break;
                }
                value = value * 10 + data[iEol] - 48;
            }
            value *= negative;

            // Init & count
            stationData = result.getData(hash);

            if (stationData == null) {
                result.addStation(hash, value);
            }
            else {
                stationData.update(value);
            }

            offset = iEol;
        }

        return result;
    }

    private static ChunkResult processAllChunks(List<Chunk> chunks) throws InterruptedException, ExecutionException {
        return chunks.parallelStream().map(CalculateAverage_gnabyl::processChunk).collect(ChunkResult::new,
                ChunkResult::mergeWith, ChunkResult::mergeWith);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        var chunks = readChunks(NB_CHUNKS);
        var result = processAllChunks(chunks);
        result.print();
    }
}
