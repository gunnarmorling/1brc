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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CalculateAverage_gnabyl {

    private static final String FILE = "./measurements.txt";

    private static final int NB_CHUNKS = Runtime.getRuntime().availableProcessors();

    private static Map<Long, String> stationNameMap = new ConcurrentHashMap<>();

    private static record Chunk(long start, int bytesCount, MappedByteBuffer mappedByteBuffer) {
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

    private static List<Chunk> readChunks(long nbChunks) throws IOException {
        RandomAccessFile file = new RandomAccessFile(FILE, "rw");
        List<Chunk> res = new ArrayList<>();
        FileChannel channel = file.getChannel();
        long bytesCount = channel.size();
        long bytesPerChunk = bytesCount / nbChunks;

        // Memory map the file in read-only mode
        // TODO: Optimize using threads
        long currentPosition = 0;
        for (int i = 0; i < nbChunks; i++) {
            int startSize = (int) bytesPerChunk;
            int realSize = startSize;

            if (i == nbChunks - 1) {
                realSize = (int) (bytesCount - currentPosition);
                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
                        realSize);

                res.add(new Chunk(currentPosition, realSize, mappedByteBuffer));
                break;
            }

            // Adjust size so that it ends on a newline
            realSize = reduceSizeToFitLineBreak(channel, currentPosition, startSize);

            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
                    realSize);

            res.add(new Chunk(currentPosition, realSize, mappedByteBuffer));
            currentPosition += realSize;
        }

        channel.close();
        file.close();

        return res;
    }

    private static class StationData {
        private double sum, min, max;
        private long count;

        public StationData(double value) {
            this.count = 1;
            this.sum = value;
            this.min = value;
            this.max = value;
        }

        public void update(double value) {
            this.count++;
            this.sum += value;
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
        }

        public double getMean() {
            return sum / count;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public void mergeWith(StationData other) {
            this.sum += other.sum;
            this.count += other.count;
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
        }

    }

    static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static class ChunkResult {
        private Map<Long, StationData> data;

        public ChunkResult() {
            data = new HashMap<>();
        }

        public StationData getData(long hash) {
            return data.get(hash);
        }

        public void addStation(long hash, double value) {
            this.data.put(hash, new StationData(value));
        }

        public void print() {
            System.out.println(
                    this.data.keySet().stream()
                            .map(hash -> {
                                var stationData = data.get(hash);
                                var name = stationNameMap.get(hash);
                                return String.format("%s=%.1f/%.1f/%.1f", name, round(stationData.getMin()),
                                        round(stationData.getMean()),
                                        round(stationData.getMax()));
                            })
                            .sorted((a, b) -> a.split("=")[0].compareTo(b.split("=")[0]))
                            .collect(Collectors.joining(", ", "{", "}")));
        }

        public void mergeWith(ChunkResult other) {
            for (Map.Entry<Long, StationData> entry : other.data.entrySet()) {
                long stationName = entry.getKey();
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
        String stationName;
        double value;
        int iSplit, iEol;
        StationData stationData;
        long negative;
        long hash, prime = 131;
        for (int offset = 0; offset < data.length; offset++) {
            // Find station name
            hash = 0;
            for (iSplit = offset; data[iSplit] != ';'; iSplit++) {
                hash = hash * prime + (data[iSplit] & 0xFF);
            }
            if (!stationNameMap.containsKey(hash)) {
                stationName = new String(data, offset, iSplit - offset, StandardCharsets.UTF_8);
                stationNameMap.put(hash, stationName);
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
                    value = value + (data[iEol + 1] - 48) / 10.0;
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
