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
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_felix19350 {
    private static final String FILE = "./measurements.txt";
    private static final int NEW_LINE_SEEK_BUFFER_LEN = 128;

    private static final int EXPECTED_MAX_NUM_CITIES = 15_000; // 10K cities + a buffer no to trigger the load factor

    private static class CityRef {

        final int length;
        final int fingerprint;
        final byte[] stringBytes;

        public CityRef(ByteBuffer byteBuffer, int startIdx, int length, int fingerprint) {
            this.length = length;
            this.stringBytes = new byte[length];
            byteBuffer.get(startIdx, this.stringBytes, 0, this.stringBytes.length);
            this.fingerprint = fingerprint;
        }

        public String cityName() {
            return new String(stringBytes, StandardCharsets.UTF_8);
        }

        @Override
        public int hashCode() {
            return fingerprint;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof CityRef otherRef) {
                if (fingerprint != otherRef.fingerprint) {
                    return false;
                }

                if (this.length != otherRef.length) {
                    return false;
                }

                for (var i = 0; i < this.length; i++) {
                    if (this.stringBytes[i] != otherRef.stringBytes[i]) {
                        return false;
                    }
                }
                return true;
            }
            else {
                return false;
            }
        }

    }

    private static class ResultRow {

        private int min;
        private int max;
        private int sum;
        private int count;

        public ResultRow(int initialValue) {
            this.min = initialValue;
            this.max = initialValue;
            this.sum = initialValue;
            this.count = 1;
        }

        public void mergeValue(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count += 1;
        }

        public String toString() {
            return round(min / 10.0) + "/" + round(sum / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public void mergeResult(ResultRow value) {
            min = Math.min(min, value.min);
            max = Math.max(max, value.max);
            sum += value.sum;
            count += value.count;
        }
    }

  private record AverageAggregatorTask(ByteBuffer byteBuffer) {
    private static final int HASH_FACTOR = 31; // Mersenne prime


    public static Stream<AverageAggregatorTask> createStreamOf(List<ByteBuffer> byteBuffers) {
      return byteBuffers.stream().map(AverageAggregatorTask::new);
    }

    public Map<CityRef, ResultRow> processChunk() {
      final var measurements = new HashMap<CityRef, ResultRow>(EXPECTED_MAX_NUM_CITIES);
      var lineStart = 0;
      // process line by line playing with the fact that a line is no longer than 106 bytes
      // 100 bytes for city name + 1 byte for separator + 1 bytes for negative sign + 4 bytes for number
      while (lineStart < byteBuffer.limit()) {
        lineStart = this.processLine(measurements, byteBuffer, lineStart);
      }
      return measurements;
    }

    private int processLine(Map<CityRef, ResultRow> measurements, ByteBuffer byteBuffer, int start) {
      var fingerPrint = 0;
      var separatorIdx = -1;
      var sign = 1;
      var value = 0;
      var lineEnd = -1;
      // Lines are processed in two stages:
      // 1 - prior do the city name separator
      // 2 - after the separator
      // this ensures less if clauses

      // stage 1 loop
      {
        for (int i = 0; i < NEW_LINE_SEEK_BUFFER_LEN; i++) {
          final var currentByte = byteBuffer.get(start + i);
          if (currentByte == ';') {
            separatorIdx = i;
            break;
          } else {
            fingerPrint = HASH_FACTOR * fingerPrint + currentByte;
          }
        }
      }

      // stage 2 loop:
      {
        for (int i = separatorIdx + 1; i < NEW_LINE_SEEK_BUFFER_LEN; i++) {
          final var currentByte = byteBuffer.get(start + i);
          switch (currentByte) {
            case '-':
              sign = -1;
              break;
            case '.':
              break;
            case '\n':
              lineEnd = start + i + 1;
              break;
            default:
              // only digits are expected here
              value = value * 10 + (currentByte - '0');
          }

          if (lineEnd != -1) {
            break;
          }
        }
      }

      assert (separatorIdx > 0);
      final var cityRef = new CityRef(byteBuffer, start, separatorIdx,fingerPrint);
      value = sign * value;

      final var existingMeasurement = measurements.get(cityRef);
      if (existingMeasurement == null) {
        measurements.put(cityRef, new ResultRow(value));
      } else {
        existingMeasurement.mergeValue(value);
      }

      return lineEnd; //to account for the line end
    }
  }

    public static void main(String[] args) throws IOException {
        // memory map the files and divide by number of cores
        final var numProcessors = Runtime.getRuntime().availableProcessors();
        final var byteBuffers = calculateMemorySegments(numProcessors);
        final var tasks = AverageAggregatorTask.createStreamOf(byteBuffers);
        assert (byteBuffers.size() <= numProcessors);
        assert (!byteBuffers.isEmpty());

        try (var pool = Executors.newFixedThreadPool(numProcessors)) {
            final Map<CityRef, ResultRow> aggregatedCities = tasks
                    .parallel()
                    .map(task -> CompletableFuture.supplyAsync(task::processChunk, pool))
                    .map(CompletableFuture::join)
                    .reduce(new HashMap<>(EXPECTED_MAX_NUM_CITIES), (currentMap, accumulator) -> {
                        currentMap.forEach((key, value) -> {
                            final var prev = accumulator.get(key);
                            if (prev == null) {
                                accumulator.put(key, value);
                            }
                            else {
                                prev.mergeResult(value);
                            }
                        });
                        return accumulator;
                    });

            var results = new HashMap<String, ResultRow>(EXPECTED_MAX_NUM_CITIES);
            aggregatedCities.forEach((key, value) -> results.put(key.cityName(), value));

            System.out.print("{");
            String output = results.keySet()
                    .stream()
                    .sorted()
                    .map(key -> key + "=" + results.get(key).toString())
                    .collect(Collectors.joining(", "));
            System.out.print(output);
            System.out.println("}");
        }
    }

    private static List<ByteBuffer> calculateMemorySegments(int numChunks) throws IOException {
        try (FileChannel fc = FileChannel.open(Paths.get(FILE))) {
            var memMappedFile = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fc.size(), Arena.ofAuto());
            var result = new ArrayList<ByteBuffer>(numChunks);

            var fileSize = fc.size();
            var chunkSize = fileSize / numChunks; // TODO: if chunksize > MAX INT we will need to adjust
            var previousChunkEnd = 0L;

            for (int i = 0; i < numChunks; i++) {
                if (previousChunkEnd >= fileSize) {
                    // There is a scenario for very small files where the number of chunks may be greater than
                    // the number of lines.
                    break;
                }
                var chunk = new long[]{ previousChunkEnd, 0 };
                if (i == (numChunks - 1)) {
                    // ensure the last chunk covers the full file
                    chunk[1] = fileSize;
                }
                else {
                    // all other chunks are end at a new line (\n)
                    var theoreticalEnd = Math.min(previousChunkEnd + chunkSize, fileSize);
                    var newLineOffset = 0;
                    for (int j = 0; j < NEW_LINE_SEEK_BUFFER_LEN; j++) {
                        var candidateOffset = theoreticalEnd + j;
                        if (candidateOffset >= fileSize) {
                            break;
                        }
                        byte b = memMappedFile.get(ValueLayout.OfByte.JAVA_BYTE, candidateOffset);
                        newLineOffset += 1;
                        if ((char) b == '\n') {
                            break;
                        }
                    }
                    chunk[1] = Math.min(fileSize, theoreticalEnd + newLineOffset);
                    previousChunkEnd = chunk[1];
                }

                assert (chunk[1] > chunk[0]);
                assert (chunk[1] <= fileSize);

                result.add(memMappedFile.asSlice(chunk[0], (chunk[1] - chunk[0])).asByteBuffer());
            }
            return result;
        }
    }
}
