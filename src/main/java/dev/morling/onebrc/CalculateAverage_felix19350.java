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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_felix19350 {
    private static final String FILE = "./measurements.txt";
    private static final int NEW_LINE_SEEK_BUFFER_LEN = 128;

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

  private record AverageAggregatorTask(MemorySegment memSegment) {

    public static Stream<AverageAggregatorTask> createStreamOf(List<MemorySegment> memorySegments) {
      return memorySegments.stream().map(AverageAggregatorTask::new);
    }

    public Map<String, ResultRow> processChunk() {
      final var result = new TreeMap<String, ResultRow>();
      var offset = 0L;
      var lineStart = 0L;
      while (offset < memSegment.byteSize()) {
        byte nextByte = memSegment.get(ValueLayout.OfByte.JAVA_BYTE, offset);
        if ((char) nextByte == '\n') {
          this.processLine(result, memSegment.asSlice(lineStart, (offset - lineStart)).asByteBuffer());
          lineStart = offset + ValueLayout.JAVA_BYTE.byteSize();
        }
        offset += ValueLayout.OfByte.JAVA_BYTE.byteSize();
      }

      return result;
    }

    private void processLine(Map<String, ResultRow> result, ByteBuffer lineBytes) {
      var separatorIdx = -1;
      for (int i = 0; i < lineBytes.limit(); i++) {
        if ((char) lineBytes.get() == ';') {
          separatorIdx = i;
          lineBytes.clear();
          break;
        }
      }
      assert (separatorIdx > 0);

      var valueCapacity = lineBytes.capacity() - (separatorIdx + 1);
      var cityBytes = new byte[separatorIdx];
      var valueBytes = new byte[valueCapacity];
      lineBytes.get(cityBytes, 0, separatorIdx);
      lineBytes.get(separatorIdx + 1, valueBytes);

      var city = new String(cityBytes, StandardCharsets.UTF_8);
      var value = parseInt(valueBytes);

      var latestValue = result.get(city);
      if (latestValue != null) {
        latestValue.mergeValue(value);
      } else {
        result.put(city, new ResultRow(value));
      }
    }

    private static int parseInt(byte[] valueBytes) {
      int multiplier = 1;
      int digitValue = 0;
      var numDigits = valueBytes.length-1; // there is always one decimal place
      var ds = new int[]{1,10,100};

      for (byte valueByte : valueBytes) {
        switch ((char) valueByte) {
          case '-':
            multiplier = -1;
            numDigits -= 1;
            break;
          case '.':
            break;
          default:
            digitValue += ((int) valueByte - 48) * (ds[numDigits - 1]);
            numDigits -= 1;
            break;// TODO continue here
        }
      }
      return multiplier*digitValue;
    }
  }

    public static void main(String[] args) throws IOException {
        // memory map the files and divide by number of cores
        var numProcessors = Runtime.getRuntime().availableProcessors();
        var memorySegments = calculateMemorySegments(numProcessors);
        var tasks = AverageAggregatorTask.createStreamOf(memorySegments);
        assert (memorySegments.size() == numProcessors);

        try (var pool = Executors.newFixedThreadPool(numProcessors)) {
            var results = tasks
                    .parallel()
                    .map(task -> CompletableFuture.supplyAsync(task::processChunk, pool))
                    .map(CompletableFuture::join)
                    .reduce(new TreeMap<>(), (partialMap, accumulator) -> {
                        partialMap.forEach((key, value) -> {
                            var prev = accumulator.get(key);
                            if (prev == null) {
                                accumulator.put(key, value);
                            }
                            else {
                                prev.mergeResult(value);
                            }
                        });
                        return accumulator;
                    });

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

    private static List<MemorySegment> calculateMemorySegments(int numChunks) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            var result = new ArrayList<MemorySegment>(numChunks);
            var chunks = new ArrayList<long[]>(numChunks);

            var fileSize = raf.length();
            var chunkSize = fileSize / numChunks;

            for (int i = 0; i < numChunks; i++) {
                var previousChunkEnd = i == 0 ? 0L : chunks.get(i - 1)[1];
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
                    var theoreticalEnd = previousChunkEnd + chunkSize;
                    var buffer = new byte[NEW_LINE_SEEK_BUFFER_LEN];
                    raf.seek(theoreticalEnd);
                    raf.read(buffer, 0, NEW_LINE_SEEK_BUFFER_LEN);

                    var newLineOffset = 0;
                    for (byte b : buffer) {
                        newLineOffset += 1;
                        if ((char) b == '\n') {
                            break;
                        }
                    }
                    chunk[1] = Math.min(fileSize, theoreticalEnd + newLineOffset);
                }

                assert (chunk[0] >= 0L);
                assert (chunk[0] <= fileSize);
                assert (chunk[1] > chunk[0]);
                assert (chunk[1] <= fileSize);

                var memMappedFile = raf.getChannel()
                        .map(FileChannel.MapMode.READ_ONLY, chunk[0], (chunk[1] - chunk[0]), Arena.ofAuto());
                memMappedFile.load();
                chunks.add(chunk);
                result.add(memMappedFile);
            }
            return result;
        }
    }
}
