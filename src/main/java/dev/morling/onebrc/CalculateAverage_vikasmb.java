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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

public class CalculateAverage_vikasmb {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        // Instant startTime = Instant.now();
        var numProcessors = Runtime.getRuntime().availableProcessors();
        int numPartitions = Math.max(numProcessors, 8);
        FileChunker fileChunker = new FileChunker(numPartitions, Path.of(FILE));
        ConcurrentHashMap<String, StationSummary> stationStats = new ConcurrentHashMap<>(100);
        IntStream.range(0, numPartitions)
                .parallel()
                .forEach(idx -> {
                    ChunkReader ckReader = new ChunkReader(stationStats, fileChunker.getChunk(idx));
                    ckReader.processBuffer();
                });
        String[] stations = stationStats.keySet().toArray(new String[0]);
        Arrays.sort(stations);
        System.out.print("{");
        for (int i = 0; i < stations.length; i++) {
            System.out.print(stations[i]);
            System.out.print("=");
            System.out.print(stationStats.get(stations[i]));
            if (i != stations.length - 1) {
                System.out.print(", ");
            }
        }
        System.out.print("}");
        System.out.println();
        // Instant endTime = Instant.now();
        // Duration elapsedTime = Duration.between(startTime, endTime);
        // System.out.println(STR."Elapsed time \{elapsedTime}");
    }

    private static class FileChunker {
        private final int numPartitions;
        private final Path filePath;
        private ByteBuffer[] chunks;

        public ByteBuffer getChunk(int index) {
            return chunks[index];
        }

        private FileChunker(int numPartitions, Path filePath) {
            this.numPartitions = numPartitions;
            this.chunks = new ByteBuffer[numPartitions];
            this.filePath = filePath;
            initChunks();
        }

        private void initChunks() {
            long fileSize = 0L;
            try {
                fileSize = Files.size(filePath);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                // Map the entire file into memory
                var partitionSize = fileSize / numPartitions;
                long pos = 0L;
                for (int i = 0; i < numPartitions; i++) {
                    var chunkedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, pos, partitionSize);
                    pos = adjustPos(chunkedBuffer, pos, (int) partitionSize - 1);
                    chunks[i] = chunkedBuffer;
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private long adjustPos(ByteBuffer chunkedBuffer, long pos, int relativePos) {
            // Adjust relativePos in case we read beyond new line in the mapped buffer.
            while (chunkedBuffer.get(relativePos) != '\n') {
                relativePos--;
            }
            // Cut short buffer to only have content upto '\n'
            chunkedBuffer.limit(relativePos + 1);
            // Return next position to read from in the file.
            return pos + relativePos + 1;
        }
    }

    private static class ChunkReader {
        private final ConcurrentMap<String, StationSummary> perStationStats;
        private final ByteBuffer buffer;

        private ChunkReader(ConcurrentMap<String, StationSummary> perStationStats, ByteBuffer buffer) {
            this.perStationStats = perStationStats;
            this.buffer = buffer;
        }

        public void processBuffer() {
            byte b;
            final byte[] keyStr = new byte[256];
            for (int pos = 0, limit = buffer.limit(); buffer.hasRemaining(); buffer.position(pos)) {
                // Parse station name
                int offset = 0;
                while ((b = buffer.get(pos++)) != ';') {
                    keyStr[offset++] = b;
                }

                // parse temperature (value) as int
                int temp = 0;
                int negative = 1;
                while (pos != limit && (b = buffer.get(pos++)) != '\n') {
                    switch (b) {
                        case '-':
                            negative = -1;
                        case '.':
                            continue;
                        default:
                            temp = 10 * temp + (b - '0');
                    }
                }
                temp *= negative;

                var stationName = new String(keyStr, 0, offset);
                int finalTemp = temp;
                perStationStats.compute(stationName,
                        (k, existingVal) -> {
                            if (existingVal == null) {
                                return new StationSummary(finalTemp, finalTemp, finalTemp, 1);
                            }
                            else {
                                existingVal.maxTemp = Math.max(existingVal.maxTemp, finalTemp);
                                existingVal.minTemp = Math.min(existingVal.minTemp, finalTemp);
                                existingVal.tempSum += finalTemp;
                                existingVal.tempCount++;
                                return existingVal;
                            }
                        });

            }
        }
    }

    private static class StationSummary {
        @Override
        public String toString() {
            double realMin = round(minTemp * 0.1);
            double realMax = round(maxTemp * 0.1);
            double realSum = tempSum * 0.1;
            double mean = round(realSum / tempCount);
            return String.format("%.1f/%.1f/%.1f", realMin, mean, realMax);
        }

        private int minTemp;
        private int maxTemp;
        private int tempSum;
        private int tempCount;

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public StationSummary(int minTemp, int maxTemp, int tempSum, int tempCount) {
            this.minTemp = minTemp;
            this.maxTemp = maxTemp;
            this.tempSum = tempSum;
            this.tempCount = tempCount;
        }

        public int getTempCount() {
            return tempCount;
        }

        public void setTempCount(int tempCount) {
            this.tempCount = tempCount;
        }

        public int getTempSum() {
            return tempSum;
        }

        public void setTempSum(int tempSum) {
            this.tempSum = tempSum;
        }

        public int getMaxTemp() {
            return maxTemp;
        }

        public void setMaxTemp(int maxTemp) {
            this.maxTemp = maxTemp;
        }

        public int getMinTemp() {
            return minTemp;
        }

        public void setMinTemp(int minTemp) {
            this.minTemp = minTemp;
        }
    }
}
