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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CalculateAverage_spullara {
    private static final String FILE = "./measurements.txt";

    /*
     * My results on this computer:
     *
     * CalculateAverage: 2m37.788s
     * CalculateAverage_royvanrijn: 0m29.639s
     * CalculateAverage_spullara: 0m2.013s
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

    List<FileSegment> segments = new ArrayList<>();try(
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r"))
    {
        for (int i = 0; i < numberOfSegments; i++) {
            long segStart = i * segmentSize;
            long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;

            if (i != 0) {
                randomAccessFile.seek(segStart);
                while (segStart < segEnd) {
                    segStart++;
                    if (randomAccessFile.read() == '\n')
                        break;
                }
            }

            if (i != numberOfSegments - 1) {
                randomAccessFile.seek(segEnd);
                while (segEnd < fileSize) {
                    segEnd++;
                    if (randomAccessFile.read() == '\n')
                        break;
                }
            }

            segments.add(new FileSegment(segStart, segEnd));
        }

        try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<ByteArrayToResultMap>> futures = new ArrayList<>();
            AtomicInteger totalLines = new AtomicInteger();
            for (FileSegment segment : segments) {
                futures.add(es.submit(() -> {
                    var resultMap = new ByteArrayToResultMap();
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
                            Result measurement = resultMap.get(buffer, 0, offset);
                            if (measurement == null) {
                                measurement = new Result(finalTemp);
                                resultMap.put(buffer, 0, offset, measurement);
                            }
                            else {
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
            for (Future<ByteArrayToResultMap> future : futures) {
                var partition = future.get();
                for (var entry : partition.getAll()) {
                    String key = new String(entry.key());
                    resultMap.compute(key, (k, v) -> {
                        if (v == null)
                            return entry.value();
                        Result value = entry.value();
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
}}

class Result {
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

    record Pair(int slot, Result slotValue) {
    }

    record Entry(byte[] key, Result value) {
    }

class ByteArrayToResultMap {
  public static final int MAPSIZE = 4096;
  Result[] slots = new Result[MAPSIZE];
  byte[][] keys = new byte[MAPSIZE][];

  public void put(byte[] key, int offset, int size, Result value) {
    Pair result = getPair(key, offset, size);
    if (result.slotValue() == null) {
      slots[result.slot()] = value;
      byte[] bytes = new byte[size];
      System.arraycopy(key, offset, bytes, 0, size);
      keys[result.slot()] = bytes;
    } else {
      result.slotValue().min = Math.min(result.slotValue().min, value.min);
      result.slotValue().max = Math.max(result.slotValue().max, value.max);
      result.slotValue().sum += value.sum;
      result.slotValue().count += value.count;
    }
  }

  private int hashCode(byte[] a, int fromIndex, int length) {
    int result = 0;
    int end = fromIndex + length;
    for (int i = fromIndex; i < end; i++) {
      result = 31 * result + a[i];
    }
    return result;
  }

  private Pair getPair(byte[] key, int offset, int size) {
    int hash = hashCode(key, offset, size);;
    int slot = hash & (slots.length - 1);
    Result slotValue = slots[slot];
    // Linear probe for open slot
    while (slotValue != null && (keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
      slot = (slot + 1) & (slots.length - 1);
      slotValue = slots[slot];
    }
    return new Pair(slot, slotValue);
  }


  public Result get(byte[] key, int offset, int size) {
    return getPair(key, offset, size).slotValue();
  }

  // Get all pairs
  public List<Entry> getAll() {
    List<Entry> result = new ArrayList<>();
    for (int i = 0; i < slots.length; i++) {
      Result slotValue = slots[i];
      if (slotValue != null) {
        result.add(new Entry(keys[i], slotValue));
      }
    }
    return result;
  }
}