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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_kumarsaurav123 {

    private static final String FILE = "./measurements.txt";
    private static AtomicInteger indexCount = new AtomicInteger(0);
    private static final ReentrantLock lock = new ReentrantLock();
    private static final int MAX_UNIQUE_KEYS = 11000;
    private static Map<StringHolder, Integer> indexMap;

    private static record Store(double[] min, double[] max, double[] sum,
                                int[] count) {


        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public String toString() {
            return new TreeMap<>(indexMap.entrySet()
                    .stream()
                    .map(e -> Map.entry(e.getKey().toString(),
                            round(min[e.getValue()]) + "/" + round((Math.round(sum[e.getValue()] * 10.0) / 10.0) / count[e.getValue()]) + "/" + round(max[e.getValue()])
                    ))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).toString();
        }
    }

    private static record Pair(long start, int size) {
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        System.out.println(run(FILE));
    }

    public static String run(String filePath) throws IOException, InterruptedException, ExecutionException {
        indexCount = new AtomicInteger(0);
        indexMap = new HashMap<>(MAX_UNIQUE_KEYS);
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        CompletionService<Store> completionService = new ExecutorCompletionService<>(executorService);
        Map<Integer, List<byte[]>> leftOutsMap = new ConcurrentSkipListMap<>();
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        long filelength = file.length();
        AtomicInteger kk = new AtomicInteger();
        MemorySegment memorySegment = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, filelength, Arena.ofShared());
        int nChunks = 1000;

        int pChunkSize = Math.min(Integer.MAX_VALUE, (int) (memorySegment.byteSize() / (1000)));
        if (pChunkSize < 100) {
            pChunkSize = (int) memorySegment.byteSize();
            nChunks = 1;
        }
        ArrayList<Pair> chunks = createStartAndEnd(pChunkSize, nChunks, memorySegment);
        chunks.stream()
                .parallel()
                .map(p -> {

                    return createRunnable(memorySegment, p);
                })
                .forEach(completionService::submit);
        executorService.shutdown();
        int i = 0;
        double[] min = new double[MAX_UNIQUE_KEYS];
        double[] max = new double[MAX_UNIQUE_KEYS];
        double[] sum = new double[MAX_UNIQUE_KEYS];
        int[] count = new int[MAX_UNIQUE_KEYS];
        initArray(i, count, min, max, sum);
        i = 0;
        final Store cureentStore = new Store(min, max, sum, count);
        while (i < chunks.size()) {
            Store newStore = completionService.take().get();
            Map<Integer, StringHolder> reverseMap = indexMap.entrySet()
                    .stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            reverseMap.forEach((key, value) -> {
                cureentStore.sum[key] += newStore.sum[key];
                cureentStore.count[key] += newStore.count[key];
                cureentStore.min[key] = Math.min(cureentStore.min[key],
                        newStore.min[key]);
                cureentStore.max[key] = Math.max(cureentStore.max[key],
                        newStore.max[key]);
            });
            i++;
        }

        return cureentStore.toString();
    }

    private static void initArray(int i, int[] count, double[] min, double[] max, double[] sum) {
        for (; i < count.length; i++) {
            min[i] = Double.POSITIVE_INFINITY;
            max[i] = Double.NEGATIVE_INFINITY;
            sum[i] = 0.0d;
            count[i] = 0;
        }
    }

    private static ArrayList<Pair> createStartAndEnd(int chunksize, int nChunks, MemorySegment memorySegment) {
        ArrayList<Pair> startSizePairs = new ArrayList<>();
        byte eol = "\n".getBytes(StandardCharsets.UTF_8)[0];
        long start = 0;
        long end = -1;
        if (nChunks == 1) {
            startSizePairs.add(new Pair(0, chunksize));
            return startSizePairs;
        }
        else {
            while (start < memorySegment.byteSize()) {
                start = end + 1;
                end = Math.min(memorySegment.byteSize() - 1, start + chunksize - 1);
                while (memorySegment.get(ValueLayout.JAVA_BYTE, end) != eol) {
                    end--;

                }
                startSizePairs.add(new Pair(start, (int) (end - start + 1)));
            }
        }
        return startSizePairs;
    }

    public static Callable<Store> createRunnable(MemorySegment memorySegment, Pair p) {
        return new Callable<Store>() {
            @Override
            public Store call() {
                try {
                    double[] min = new double[MAX_UNIQUE_KEYS];
                    double[] max = new double[MAX_UNIQUE_KEYS];
                    double[] sum = new double[MAX_UNIQUE_KEYS];
                    int[] count = new int[MAX_UNIQUE_KEYS];
                    for (int i = 0; i < count.length; i++) {
                        min[i] = Double.POSITIVE_INFINITY;
                        max[i] = Double.NEGATIVE_INFINITY;
                        sum[i] = 0.0d;
                        count[i] = 0;
                    }

                    byte[] allBytes2 = memorySegment.asSlice(p.start, p.size).toArray(ValueLayout.JAVA_BYTE);
                    byte[] eol = "\n".getBytes(StandardCharsets.UTF_8);
                    byte[] sep = ";".getBytes(StandardCharsets.UTF_8);

                    int st = 0;
                    for (int i = 0; i < allBytes2.length; i++) {
                        if (allBytes2[i] == eol[0]) {
                            ;
                            byte[] s2 = new byte[i - st];
                            System.arraycopy(allBytes2, st, s2, 0, s2.length);
                            for (int j = 0; j < s2.length; j++) {
                                if (s2[j] == sep[0]) {
                                    byte[] city = new byte[j];
                                    byte[] value = new byte[s2.length - j - 1];
                                    System.arraycopy(s2, 0, city, 0, city.length);
                                    System.arraycopy(s2, city.length + 1, value, 0, value.length);
                                    double d = getaDouble(value);
                                    StringHolder citys = new StringHolder(city);
                                    Integer index = indexMap.get(citys);
                                    if (Objects.isNull(index)) {
                                        lock.lock();
                                        if (Objects.isNull(indexMap.get(citys))) {
                                            index = indexCount.getAndIncrement();
                                            indexMap.putIfAbsent(citys, index);

                                        }
                                        index = indexMap.get(citys);
                                        lock.unlock();
                                    }

                                    count[index] = count[index] + 1;
                                    max[index] = Math.max(max[index], d);
                                    min[index] = Math.min(min[index], d);
                                    sum[index] = Double.sum(sum[index], d);
                                    break;
                                }
                            }
                            st = i + 1;
                        }
                    }
                    // System.out.println("Task " + kk + "Completed in " + (System.nanoTime() - start));
                    return new Store(min, max, sum, count);
                }
                catch (Exception e) {
                    // throw new RuntimeException(e);
                    throw e;
                }
            }
        };
    }

    private static double getaDouble(byte[] value) {
        double d = 0.0;
        int s = -1;
        for (int k = value.length - 1; k >= 0; k--) {
            if (value[k] == 45) {
                d = d * -1;
            }
            else if (value[k] == 46) {
            }
            else {
                d = d + (((int) value[k]) - 48) * Math.pow(10, s);
                s++;
            }
        }
        return d;
    }

    static class StringHolder implements Comparable<StringHolder> {
        byte[] bytes;

        public StringHolder(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public String toString() {
            return new String(this.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(this.bytes);
        }

        @Override
        public boolean equals(Object obj) {
            return Arrays.equals(this.bytes, ((StringHolder) obj).bytes);
        }

        @Override
        public int compareTo(StringHolder o) {
            return new String(this.bytes).compareTo(new String(o.bytes));
        }
    }
}
