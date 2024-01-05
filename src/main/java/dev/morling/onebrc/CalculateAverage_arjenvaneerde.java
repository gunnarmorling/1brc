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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_arjenvaneerde {

  static class Measure {
    byte[] station;
    int    count;
    int    minTemp;
    int    maxTemp;
    long   sumTemp;

    Measure(final byte[] station, int count, int minTemp, int maxTemp, long sumTemp) {
      this.station = station;
      this.count   = count;
      this.minTemp = minTemp;
      this.maxTemp = maxTemp;
      this.sumTemp = sumTemp;
    }

    Measure(byte[] bytes, int startPos, int endPos, int temp) {
      this.station = new byte[endPos - startPos];
      System.arraycopy(bytes, startPos, this.station, 0, this.station.length);
      this.count   = 1;
      this.minTemp = temp;
      this.maxTemp = temp;
      this.sumTemp = temp;
    }

    void add(int temp) {
      this.count++;
      this.minTemp = Math.min(this.minTemp, temp);
      this.maxTemp = Math.max(this.maxTemp, temp);
      this.sumTemp += temp;
    }

    public static Measure merge(Measure m1, Measure m2) {
      return new Measure(m1.station, m1.count + m2.count, Math.min(m1.minTemp, m2.minTemp), Math.max(m1.maxTemp, m2.maxTemp), m1.sumTemp + m2.sumTemp);
    }

    public String toString() {
      return String.format("%.1f/%.1f/%.1f", this.minTemp / 10.0, this.sumTemp / (10.0 * this.count), this.maxTemp / 10.0);
    }
  }

  static class Measures {
    static final int HASH_TABLE_SIZE = 2048;

    Measure[][] measureHashTable;

    Measures() {
      this.measureHashTable = new Measure[HASH_TABLE_SIZE][20];
    }

    void add(byte[] bytes, int startPos, int endPos, int temp) {
      int len       = endPos - startPos;
      int lenBits   = (len - 2) & 0x1f;
      int char0Bits = bytes[startPos + 0] & 0x1f;
      int char1Bits = bytes[startPos + 1] & 0x1f;
      int index = (lenBits |                 // 4 bits of the length
                   (char0Bits << 4) |   // 5 bits of first char
                   (char1Bits << 9)     // 5 bits of second char
                  ) & (HASH_TABLE_SIZE - 1);
      Measure[] arr   = this.measureHashTable[index];
      int       i     = 0;
      boolean   found = false;
      while (i < arr.length) {
        Measure m = arr[i];
        if (m == null) {
          // Not found. Add new entry.
          arr[i] = new Measure(bytes, startPos, endPos, temp);
          return;
        }
        if (m.station.length == len) {
          switch (len) {
            case 1:
              if (m.station[0] == bytes[startPos + 0]) {
                found = true;
              }
              break;
            case 2:
              if (m.station[0] == bytes[startPos + 0] &&
                  m.station[1] == bytes[startPos + 1]) {
                found = true;
              }
              break;
            case 3:
              if (m.station[0] == bytes[startPos + 0] &&
                  m.station[1] == bytes[startPos + 1] &&
                  m.station[2] == bytes[startPos + 2]) {
                found = true;
              }
              break;
            default:
              int mismatch = Arrays.mismatch(m.station, 0, len, bytes, startPos, endPos);
              found = mismatch == -1;
              break;
          }
          if (found) {
            // Add info.
            m.add(temp);
            return;
          }
        }
        i++;
      }
//      throw new RuntimeException("Reached end of Measures array.");
      Measure[] newArr = new Measure[arr.length * 2];
      System.arraycopy(arr, 0, newArr, 0, arr.length);
      newArr[i]                    = new Measure(bytes, startPos, endPos, temp);
      this.measureHashTable[index] = newArr;
    }
  }

  private static final String                FILE             = "./measurements.txt";
  private static final int                   NUM_THREADS      = 8;
  private static final int                   BYTE_BUFFER_SIZE = 16 * 1024 * 1024;
  private static final ExecutorService       threads          = Executors.newFixedThreadPool(NUM_THREADS);
  private static final List<Future<Integer>> futures          = new ArrayList<>(NUM_THREADS);
  private static final Measures[]            measures         = new Measures[NUM_THREADS];

  private static class BytesProcessor implements Runnable {
    final Measures measures;
    final byte[]   buffer;
    long absoluteStartPos;
    int  startPos;
    int  endPos;

    BytesProcessor(byte[] buffer, long absoluteStartPos, int startPos, int endPos, final Measures measures) {
      this.buffer           = buffer;
      this.absoluteStartPos = absoluteStartPos;
      this.startPos         = startPos;
      this.endPos           = endPos;
      this.measures         = measures;
    }

    @Override
    public void run() {
      int startOfLinePos = startPos;
      int endOfLinePos   = startOfLinePos;
      int sepPos;
      // Process all lines
      while (endOfLinePos < endPos) {
        while (buffer[endOfLinePos] != ';') {
          endOfLinePos++;
        }
        sepPos = endOfLinePos;
        while (buffer[endOfLinePos] != 0x0A) {
          endOfLinePos++;
        }
        if (endOfLinePos < endPos) {
          int  temperature = 0;
          int  sign        = 1;
          int  digitPos    = sepPos + 1;
          byte lineByte;
          while (digitPos < endOfLinePos) {
            lineByte = buffer[digitPos];
            if (lineByte >= '0' && lineByte <= '9') {
              temperature = temperature * 10 + (lineByte - '0');
            } else if (lineByte == '-') {
              sign = -1;
            }
            digitPos++;
          }
          measures.add(buffer, startOfLinePos, sepPos, sign * temperature);
          startOfLinePos = ++endOfLinePos;
        }
      }
    }
  }

  private static class MeasuresMergeProcessor implements Runnable {
    final Measures[]                             measures;
    final int                                    startIndex;
    final int                                    endIndex;
    final ConcurrentSkipListMap<String, Measure> results;

    MeasuresMergeProcessor(final Measures[] measures,
                           final int startIndex,
                           final int endIndex,
                           final ConcurrentSkipListMap<String, Measure> results) {
      this.measures   = measures;
      this.startIndex = startIndex;
      this.endIndex   = endIndex;
      this.results    = results;
    }

    @Override
    public void run() {
      for (int hashIdx = this.startIndex; hashIdx < this.endIndex; hashIdx++) {
        for (int mIdx = 0; mIdx < this.measures.length; mIdx++) {
          Measure[] mArr = this.measures[mIdx].measureHashTable[hashIdx];
          for (Measure measure : mArr) {
            if (measure != null) {
              this.results.compute(new String(measure.station, StandardCharsets.UTF_8), (k, v) -> v == null ? measure : Measure.merge(v, measure));
            }
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < NUM_THREADS; i++) {
      measures[i] = new Measures();
    }
    File file = new File(FILE);
    try (RandomAccessFile raFile = new RandomAccessFile(file, "r");
         FileChannel inChannel = raFile.getChannel()) {

      ByteBuffer buffer          = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
      byte[]     fileBytes       = buffer.array();
      long       numBytesRead    = 0;
      long       numTotBytesRead = 0;
      while ((numBytesRead = inChannel.read(buffer)) > 0) {
        numTotBytesRead += numBytesRead;
//        System.out.println((double) numTotBytesRead/(1024*1024));
        buffer.flip();
        //
        int chunckSize      = buffer.limit() / NUM_THREADS;
        int startOfChunkPos = 0;
        int endOfChunkPos   = 0;
        for (int chunk = 0; chunk < NUM_THREADS; chunk++) {
          endOfChunkPos = Math.min((chunk + 1) * chunckSize, buffer.limit() - 1);
          while (endOfChunkPos > startOfChunkPos &&
                 fileBytes[endOfChunkPos] != 0x0A) {
            endOfChunkPos--;
          }
          endOfChunkPos++;
          Future<Integer> f = threads.submit(new BytesProcessor(fileBytes, numTotBytesRead - numBytesRead, startOfChunkPos, endOfChunkPos, measures[chunk]), chunk);
          futures.add(f);
          startOfChunkPos = endOfChunkPos;
        }
        //
        for (Future<Integer> future : futures) {
          future.get();
        }
        futures.clear();
        buffer.position(endOfChunkPos);
        buffer.compact();
      }
    }
    ConcurrentSkipListMap<String, Measure> measurements = new ConcurrentSkipListMap<>();
    int                                    chunkSize    = Measures.HASH_TABLE_SIZE / measures.length;
    for (int i = 0; i < measures.length; i++) {
      Future<Integer> f = threads.submit(new MeasuresMergeProcessor(measures, i * chunkSize, (i + 1) * chunkSize, measurements), i);
      futures.add(f);
    }
    for (Future<Integer> future : futures) {
      future.get();
    }
    futures.clear();
    threads.shutdown();
    threads.awaitTermination(1, TimeUnit.MILLISECONDS);
    System.out.println(measurements);
    long endTime = System.currentTimeMillis();
    System.out.printf("Duration : %.3f\n%n", (endTime - startTime) / 1000.0);
  }

}
