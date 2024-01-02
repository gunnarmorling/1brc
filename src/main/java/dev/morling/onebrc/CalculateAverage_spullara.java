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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CalculateAverage_spullara {
  private static final String FILE = "./measurements.txt";

  static final class Result {
    double min;
    double max;
    double sum;
    double count;

    Result(double value) {
      this.min = value;
      this.max = value;
      this.sum = value;
      this.count = 1;
    }

    @Override
    public String toString() {
      return round(min) +
              "/" + round(sum / count) +
              "/" + round(max);
    }

    double round(double v) {
      return Math.round(v * 10.0) / 10.0;
    }
  }

  /*
   * My results on this computer:
   *
   * CalculateAverage: 2m37.788s
   * CalculateAverage_royvanrijn: 0m29.639s
   * CalculateAverage_spullara: 0m6.278s
   *
   */

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    String filename = args.length == 0 ? FILE : args[0];
    File file = new File(filename);

    record FileSegment(long start, long end) {
    }

    int numberOfSegments = Runtime.getRuntime().availableProcessors();
    long fileSize = file.length();
    long segmentSize = fileSize / numberOfSegments;

    long start = System.currentTimeMillis();

    List<FileSegment> segments = new ArrayList<>();
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      for (int i = 0; i < numberOfSegments; i++) {
        long segStart = i * segmentSize;
        long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;

        if (i != 0) {
          randomAccessFile.seek(segStart);
          while (segStart < segEnd) {
            segStart++;
            if (randomAccessFile.read() == '\n') break;
          }
        }

        if (i != numberOfSegments - 1) {
          randomAccessFile.seek(segEnd);
          while (segEnd < fileSize) {
            segEnd++;
            if (randomAccessFile.read() == '\n') break;
          }
        }

        segments.add(new FileSegment(segStart, segEnd));
      }

      try (ExecutorService es = Executors.newFixedThreadPool(numberOfSegments)) {
        List<Future<Map<String, Result>>> futures = new ArrayList<>();
        AtomicInteger totalLines = new AtomicInteger();
        for (FileSegment segment : segments) {
          futures.add(es.submit(() -> {
            Map<String, Result> resultMap = new HashMap<>();
            MappedByteBuffer bb;
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(filename), StandardOpenOption.READ)) {
              bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start, segment.end - segment.start);
              byte[] buffer = new byte[64];
              int lines = 0;
              int startLine;
              int limit = bb.limit();
              while ((startLine = bb.position()) < limit) {
                int currentPosition = startLine;
                byte b;
                int offset = 0;
                while (currentPosition != segment.end && (b = bb.get(currentPosition++)) != ';') {
                  buffer[offset++] = b;
                }
                String city = new String(buffer, 0, offset);
                int temp = 0;
                int negative = 1;
                while (currentPosition != segment.end && (b = bb.get(currentPosition++)) != '\n') {
                  if (b == '-') {
                    negative = -1;
                    continue;
                  }
                  if (b == '.') {
                    continue;
                  }
                  if (b == '\r') {
                    break;
                  }
                  temp = 10 * temp + (b - '0');
                }
                temp *= negative;
                double finalTemp = temp / 10.0;
                Result measurement = resultMap.get(city);
                if (measurement == null) {
                  measurement = new Result(finalTemp);
                  resultMap.put(city, measurement);
                } else {
                  measurement.min = Math.min(measurement.min, finalTemp);
                  measurement.max = Math.max(measurement.max, finalTemp);
                  measurement.sum += finalTemp;
                  measurement.count += 1;
                }
                lines++;
                bb.position(currentPosition);
              }
              totalLines.addAndGet(lines);
              return resultMap;
            }
          }));
        }

        Map<String, Result> resultMap = new TreeMap<>();
        for (Future<Map<String, Result>> future : futures) {
          Map<String, Result> partition = future.get();
          for (Map.Entry<String, Result> entry : partition.entrySet()) {
            resultMap.compute(entry.getKey(), (k, v) -> {
              if (v == null) return entry.getValue();
              Result value = entry.getValue();
              v.min = Math.min(v.min, value.min);
              v.max = Math.max(v.max, value.max);
              v.sum += value.sum;
              v.count += value.count;
              return v;
            });
          }
        }

        System.out.println("Time: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println("Lines processed: " + totalLines);
        System.out.println(resultMap);
      }
    }
  }
}

