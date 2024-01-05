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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_vemana {

    public static void main(String[] args) throws Exception {
        Path measurementsFile = Path.of("measurements.txt");
        ParallelQueue.doIt(measurementsFile);
    }

    private static class ParallelQueue {

        public static void doIt(Path measurementFile) throws InterruptedException, ExecutionException {
            int processors = Runtime.getRuntime().availableProcessors();
            DataQueue dataQueue = new DataQueue();

            List<Future<Result>> results = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(processors);
            for (int i = 0; i < processors; i++) {
                final Callable<Result> callable;
                if (i == 0) {
                    callable = () -> {
                        readInto(measurementFile, dataQueue, 1 << 20);
                        dataQueue.done();
                        return new SegmentProcessor(dataQueue, 16).process();
                    };
                }
                else {
                    callable = () -> new SegmentProcessor(dataQueue, 16).process();
                }
                results.add(executorService.submit(callable));
            }
            executorService.shutdown();

            // Merge the results
            Map<String, Stat> map = null;
            for (int i = 0; i < results.size(); i++) {
                if (i == 0) {
                    map = new TreeMap<>(results.get(0).get().tempStats());
                }
                else {
                    for (Entry<String, Stat> entry : results.get(i).get().tempStats().entrySet()) {
                        map.compute(entry.getKey(), (key, value) -> value == null
                                ? entry.getValue()
                                : Stat.merge(value, entry.getValue()));
                    }
                }
            }

            Result result = new Result(map);
            result.print();
        }

        static void readInto(Path measurementsFile, DataQueue dataQueue, long chunkSize) {
            // The pre-allocated queue size should suffice without blocking
            checkArg(chunkSize * dataQueue.queueSize() >= (1L << 34));

            // should fit at least one temp reading
            checkArg(chunkSize >= 100 * 4 + 1 + 4 + 1);

            try (RandomAccessFile fc = new RandomAccessFile(measurementsFile.toFile(), "r")) {
                long fileSize = fc.length();
                MemorySegment memorySegment = fc.getChannel()
                        .map(MapMode.READ_ONLY, 0, fileSize, Arena.global());
                for (long start = 0; start < fileSize; start += chunkSize) {
                    long curSize = Math.min(fileSize - start, chunkSize);
                    // ?? Prime the memory segment
                    // primeSegment(memorySegment, start, curSize);
                    int delta = start + chunkSize >= fileSize ? 1 : 0;
                    dataQueue.put(new ByteRange(memorySegment, start, start + curSize - delta));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static void checkArg(boolean condition) {
            if (!condition) {
                throw new IllegalArgumentException("");
            }
        }

        private static void primeSegment(MemorySegment segment, long start, long curSize) {
            for (long pos = 0; pos < curSize; pos++) {
                segment.get(JAVA_BYTE, start + pos);
            }
        }

        private record ByteRange(MemorySegment memory, long start, long end) {

        }

        private static class DataQueue {

            private final int queueSize = (1 << 20);
            private final Queue<ByteRange> elem = new ArrayBlockingQueue<>(queueSize, false);
            // Don't use any synchronization because this is running on x86 & hence TSO
            private boolean isDone = false;

            public void done() {
                // System.err.printf("Finished reading..");
                isDone = true;
            }

            public void put(ByteRange range) {
                elem.add(range);
            }

            public int queueSize() {
                return queueSize;
            }

            public ByteRange take() {
                var ret = elem.poll();
                if (ret != null) {
                    return ret;
                }

                // busy spin
                // System.err.printf("Busy spinning...");
                while (true) {
                    if (isDone && elem.isEmpty()) {
                        return null;
                    }
                    ret = elem.poll();
                    if (ret != null) {
                        return ret;
                    }
                }
            }
        }

        static class IndexMemory {

            private final byte[][] cityNames;
            private final int slotsMask;
            private final Stat[] stats;

            IndexMemory(int slotsBits) {
                this.stats = new Stat[1 << slotsBits];
                this.cityNames = new byte[1 << slotsBits][];
                this.slotsMask = (1 << slotsBits) - 1;
            }

            void addDataPoint(byte[] cityBytes, int length, int hash, long temp) {
                int index = linearProbe(cityBytes, length, hash);
                var stat = stats[index];
                if (stat == null) {
                    stats[index] = Stat.firstReading(temp);
                }
                else {
                    stat.mergeReading(temp);
                }
            }

            int linearProbe(byte[] cityBytes, int len, int hash) {
                for (int i = hash;; i = (i + 1) & slotsMask) {
                    var curBytes = cityNames[i];
                    if (curBytes == null) {
                        cityNames[i] = Arrays.copyOf(cityBytes, len);
                        return i;
                    }
                    else {
                        if (Arrays.equals(cityBytes, 0, len, curBytes, 0, curBytes.length)) {
                            return i;
                        }
                    }
                }
            }

            Result result() {
                int N = stats.length;
                TreeMap<String, Stat> map = new TreeMap<>();
                for (int i = 0; i < N; i++) {
                    if (stats[i] != null) {
                        map.put(new String(cityNames[i]), stats[i]);
                    }
                }
                return new Result(map);
            }
        }

    record Result(Map<String, Stat> tempStats) {

      void print() {
        String ret = tempStats().entrySet().stream().sorted(Comparator.comparing(Entry::getKey))
                                .map(entry -> "%s=%s".formatted(entry.getKey(), entry.getValue()))
                                .collect(Collectors.joining(", ", "{", "}"));
        System.out.printf("%s\n", ret);
      }
    }

        static class SegmentProcessor {

            private final DataQueue dataQueue;
            private final IndexMemory indexMemory;
            private final int slotMask;

            SegmentProcessor(DataQueue dataQueue, int slotsBits) {
                this.dataQueue = dataQueue;
                this.slotMask = (1 << slotsBits) - 1;
                this.indexMemory = new IndexMemory(slotsBits);
            }

            Result process() {
                ByteRange range;
                while ((range = dataQueue.take()) != null) {
                    process(range);
                }
                return result();
            }

            Result result() {
                return indexMemory.result();
            }

            private void process(ByteRange range) {
                long rangeStart = range.start();
                long rangeEnd = range.end();
                MemorySegment memory = range.memory();

                // System.err.printf(
                // STR."Starting chunk start=\{rangeStart}, size=\{rangeEnd - rangeStart}\n");

                long nextPos = rangeStart;
                if (rangeStart > 0) {
                    while (memory.get(JAVA_BYTE, nextPos++) != '\n') {
                    }
                }

                byte[] cityBytes = new byte[512];
                boolean negative;
                long temp;
                byte nextChar;
                int cityLen, hash;

                while (true) {
                    if (nextPos > rangeEnd) {
                        break;
                    }

                    cityLen = hash = 0;
                    while ((nextChar = memory.get(JAVA_BYTE, nextPos++)) != ';') {
                        cityBytes[cityLen++] = nextChar;
                        hash = (hash * 31 + nextChar) & slotMask;
                    }

                    negative = memory.get(JAVA_BYTE, nextPos) == '-';
                    if (negative) {
                        nextPos++;
                    }

                    temp = 0;
                    while ((nextChar = memory.get(JAVA_BYTE, nextPos++)) != '\n') {
                        if (nextChar != '.') {
                            temp = temp * 10 + (nextChar - '0');
                        }
                    }

                    indexMemory.addDataPoint(cityBytes, cityLen, hash, negative ? -temp : temp);
                }
            }
        }

        static class Stat {

            public static Stat merge(Stat left, Stat right) {
                return new Stat(
                        Math.min(left.min, right.min),
                        Math.max(left.max, right.max),
                        left.sum + right.sum,
                        left.count + right.count);
            }

            static Stat firstReading(long temp) {
                return new Stat(temp, temp, temp, 1);
            }

            private long count;
            private long min, max, sum;

            Stat(long min, long max, long sum, long count) {
                this.min = min;
                this.max = max;
                this.sum = sum;
                this.count = count;
            }

            public void mergeReading(long curTemp) {
                min = Math.min(min, curTemp);
                max = Math.max(max, curTemp);
                sum += curTemp;
                count++;
            }

            @Override
            public String toString() {
                return "%.1f/%.1f/%.1f".formatted(min / 10.0, sum / 10.0 / count, max / 10.0);
            }
        }
    }
}
