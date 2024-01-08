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
import java.util.Arrays;
import java.util.Hashtable;
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
    private static final int[] INT_PARSE_FACTORS = new int[]{1,10,100};
    private static final int HASH_FACTOR = 433; // Mersenne prime
    private static final int EXPECTED_MAX_NUM_CITIES = 15_000; // 10K cities + a buffer no to trigger the load factor

    public static Stream<AverageAggregatorTask> createStreamOf(List<MemorySegment> memorySegments) {
      return memorySegments.stream().map(AverageAggregatorTask::new);
    }

    public Map<String, ResultRow> processChunk() {
      final var measurements = new Hashtable<Integer, ResultRow>(EXPECTED_MAX_NUM_CITIES);
      final var cityNames = new Hashtable<Integer, String>(EXPECTED_MAX_NUM_CITIES);
      var offset = 0L;
      var lineStart = 0L;
      // process line by line
      while (offset < memSegment.byteSize()) {
        byte nextByte = memSegment.get(ValueLayout.OfByte.JAVA_BYTE, offset);
        if ((char) nextByte == '\n') {
          this.processLine(measurements, cityNames, memSegment.asSlice(lineStart, (offset - lineStart)).asByteBuffer());
          lineStart = offset + ValueLayout.JAVA_BYTE.byteSize();
        }
        offset += ValueLayout.OfByte.JAVA_BYTE.byteSize();
      }

      // at the end merge the city names with the measurements:
      final var result = new Hashtable<String, ResultRow>(EXPECTED_MAX_NUM_CITIES);
      cityNames.forEach((key, value) -> result.put(value, measurements.get(key)));
      return result;
    }

    private void processLine(Map<Integer, ResultRow> measurements, Map<Integer, String> cities, ByteBuffer lineBytes) {
      // Find separator
      var fingerPrint = 0;
      var separatorIdx = -1;
      for (int i = 0; i < lineBytes.limit(); i++) {
        final var currentByte = (char) lineBytes.get();
        if (currentByte == ';') {
          separatorIdx = i;
          lineBytes.clear();
          break;
        }else{
          fingerPrint = HASH_FACTOR * fingerPrint + currentByte;
        }
      }
      assert (separatorIdx > 0);

      lineBytes.clear();
      var bytes = new byte[separatorIdx];
      lineBytes.get(bytes);
      var city = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(city + " - " + city.hashCode() + " | " + fingerPrint);
      lineBytes.clear();
      //TODO failing because of collisions in the hashcode here "igButeboJuršinciKoaniImdinaNova VasDestrnikVarvarinSkomunGornji PetrovciRibnicaKon TumŠavnikPoul"
      final var value = parseInt(lineBytes, separatorIdx+1, lineBytes.capacity());
      final var latestValue = measurements.get(fingerPrint);
      if (latestValue != null) {
        latestValue.mergeValue(value);
      } else {
        measurements.put(fingerPrint, new ResultRow(value));
        lineBytes.clear();
        final var cityNameBytes = new byte[separatorIdx];
        lineBytes.get(cityNameBytes);
        final var cityName = new String(cityNameBytes, StandardCharsets.UTF_8);
        cities.put(fingerPrint, cityName);
      }
    }

    private static int parseInt(ByteBuffer valueBytes, int start, int end) {
      int multiplier = 1;
      int digitValue = 0;
      int numDigits = end-start-1; // there is always one decimal place

      valueBytes.position(start);
      while(valueBytes.hasRemaining()){
        var valueByte = valueBytes.get();
        switch ((char) valueByte) {
          case '-':
            multiplier = -1;
            numDigits -= 1;
            break;
          case '.':
            break;
          default:
            digitValue += ((int) valueByte - '0') * (INT_PARSE_FACTORS[numDigits - 1]);
            numDigits -= 1;
            break;
        }
      }
      return multiplier*digitValue;
    }
  }

    public static void main(String[] args) throws IOException {
        // memory map the files and divide by number of cores
        final var numProcessors = Runtime.getRuntime().availableProcessors();
        final var memorySegments = calculateMemorySegments(numProcessors);
        final var tasks = AverageAggregatorTask.createStreamOf(memorySegments);
        assert (memorySegments.size() == numProcessors);

        try (var pool = Executors.newFixedThreadPool(numProcessors)) {
            final var results = tasks
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
