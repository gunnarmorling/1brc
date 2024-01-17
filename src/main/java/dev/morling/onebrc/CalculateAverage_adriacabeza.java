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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class calculates average measurements from a file in a parallelized manner.
 */
public class CalculateAverage_adriacabeza {

    private static final Path FILE_PATH = Paths.get("./measurements.txt");
    public static final int CITY_NAME_MAX_CHARACTERS = 128;

    /**
     * Represents result containing a HashMap with city as key and ResultRow as value.
     */
    private static class Result {
        private static class StationData {
            private int min, sum, count, max;

            public StationData(int value) {
                this.count = 1;
                this.sum = value;
                this.min = value;
                this.max = value;
            }

            public void update(int value) {
                this.count++;
                this.sum += value;
                this.min = Math.min(this.min, value);
                this.max = Math.max(this.max, value);
            }

            public String toString() {
                return "%.1f/%.1f/%.1f".formatted(min / 10.0, sum / 10.0 / count, max / 10.0);
            }

        }

        private final Map<String, StationData> resultMap;

        public Result() {
            this.resultMap = new HashMap<>();
        }

        public Map<String, StationData> getResultMap() {
            return resultMap;
        }

        public void addMeasurement(String city, int value) {
            resultMap.compute(city, (_, resultRow) -> {
                if (resultRow == null) {
                    return new StationData(value);
                }
                else {
                    resultRow.update(value);
                    return resultRow;
                }
            });
        }

        public void merge(Result other) {
            other.getResultMap().forEach((city, resultRow) -> resultMap.merge(city, resultRow, (existing, incoming) -> {
                existing.min = Math.min(existing.min, incoming.min);
                existing.max = Math.max(existing.max, incoming.max);
                existing.sum += incoming.sum;
                existing.count += incoming.count;
                return existing;
            }));
        }

        public String toString() {
            return this.resultMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> "%s=%s".formatted(entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(", ", "{", "}"));
        }
    }

    /**
     * Finds the ending position in the file, ensuring it ends at the beginning of a line.
     *
     * @param channel  File channel
     * @param position Current position in the file
     * @return Ending position at the beginning of a line
     * @throws IOException If an I/O error occurs
     */
    private static long findEndPosition(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);

        // Iterate over the file from the given position to find the next newline character
        while (position < channel.size()) {
            channel.read(buffer, position);

            // Check if the current byte is a newline character
            if (buffer.get(0) == '\n') {
                return position + 1; // Return the position immediately after the newline
            }

            position++;
            buffer.clear();
        }

        return channel.size(); // Return the end of the file if no newline is found after the current position
    }

    /**
     * Gets the mapped byte buffers for parallel processing.
     *
     * @param nProcessors Number of processors for parallelization
     * @return List of MappedByteBuffers
     * @throws IOException If an I/O error occurs
     */
    private static List<MappedByteBuffer> getMappedByteBuffers(int nProcessors) throws IOException {
        try (FileChannel channel = FileChannel.open(FILE_PATH, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long chunkSize = (fileSize + nProcessors - 1) / nProcessors;
            long pos = 0;

            List<MappedByteBuffer> buffers = new ArrayList<>(nProcessors);
            for (int i = 0; i < nProcessors; i++) {
                long endPosition = findEndPosition(channel, pos + chunkSize);
                long size = endPosition - pos;
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
                pos = pos + size;
                buffers.add(buffer);
            }
            return buffers;
        }
    }

    /**
     * Calculates average measurements from the file.
     *
     * @return Result containing min/mean/max values for each city
     */
    private static Result calculateAverageMeasurements(List<MappedByteBuffer> chunks) {
        // Process each buffer in parallel
        return chunks.parallelStream()
                .map(buffer -> {
                    Result partialResult = new Result();
                    var limit = buffer.limit();
                    var field = new byte[CITY_NAME_MAX_CHARACTERS];
                    while (buffer.position() < limit) {
                        var fieldCurrentIndex = 0;
                        field[fieldCurrentIndex++] = buffer.get();
                        while (buffer.position() < limit) {
                            var fieldByte = buffer.get();
                            if (fieldByte == ';')
                                break;
                            field[fieldCurrentIndex++] = fieldByte;
                        }
                        var fieldStr = new String(field, 0, fieldCurrentIndex);
                        var number = 0;
                        var sign = 1;
                        while (buffer.position() < limit) {
                            var numberByte = buffer.get();
                            if (numberByte == '-')
                                sign = -1;
                            else if (numberByte == '\n')
                                break;
                            else if (numberByte != '.')
                                number = number * 10 + (numberByte - '0');
                        }
                        partialResult.addMeasurement(fieldStr, sign * number);
                    }
                    return partialResult;
                }).reduce(new Result(), (partialResult1, partialResult2) -> {
                    Result result = new Result();
                    result.merge(partialResult1);
                    result.merge(partialResult2);
                    return result;
                });
    }

    /**
     * The main method to run the average measurements calculations program.
     *
     * @param args Command line arguments. Not utilized in this program.
     */
    public static void main(String[] args) {
        try {
            // Get the MappedByteBuffers by splitting the file evenly across available processors
            var buffers = getMappedByteBuffers(Runtime.getRuntime().availableProcessors());

            // Calculate the average measurements from the buffers obtained
            var measurements = calculateAverageMeasurements(buffers);

            // Print the measurements result to the console.
            System.out.println(measurements);

        } catch (IOException e) {
            // Handle any potential I/O exceptions by printing the error message to the console
            System.err.println(STR."Error processing file: \{e.getMessage()}");
        }
    }
}
