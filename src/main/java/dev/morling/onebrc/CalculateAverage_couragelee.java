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
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_couragelee {
    private static class Temperature {
        private int cnt = 0;

        private double sum = 0;

        private double min;

        private double max;

        public Temperature(String tempStr) {
            double temp = Double.parseDouble(tempStr);
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.cnt++;
        }

        public Temperature(int cnt, double sum, double min, double max) {
            this.cnt = cnt;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }

        public Temperature addRecord(String tempStr) {
            double temp = Double.parseDouble(tempStr);
            Temperature newTemp = new Temperature(this.cnt, this.sum, this.min, this.max);
            newTemp.min = Math.min(temp, newTemp.min);
            newTemp.max = Math.max(temp, newTemp.max);
            newTemp.sum += temp;
            newTemp.cnt++;
            return newTemp;
        }

        public Temperature merge(Temperature newValue) {
            Temperature oldTemp = new Temperature(this.cnt, this.sum, this.min, this.max);
            oldTemp.min = Math.min(newValue.min, oldTemp.min);
            oldTemp.max = Math.max(newValue.max, oldTemp.max);
            oldTemp.sum += newValue.sum;
            oldTemp.cnt += newValue.cnt;
            return oldTemp;
        }

        public void update(String tempStr) {
            double temp = parseDouble(tempStr);
            this.min = Math.min(temp, this.min);
            this.max = Math.max(temp, this.max);
            this.sum += temp;
            this.cnt++;
        }

        @Override
        public String toString() {
            return STR."\{min}/\{Math.round((sum / cnt) * 10.0) / 10.0}/\{max}";
        }
    }

    private static final String FILE_PATH = "./measurements.txt";

    // 并行任务的数量
    public static final int CONCURRENT_NUM = 20;

    private static FileChannel fc;
    private static long fcSize;

    private static int segmentSize;

    private static Map<String, Temperature> temperatureMap;

    // 需要拼接的行信息
    private static Map<String, byte[]> tempBytesMap = new ConcurrentHashMap<>();

    // 缓存double解析数据
    private static Map<String, Double> doubleCache;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // 初始化
        File file = new File(FILE_PATH);
        fc = new RandomAccessFile(file, "r").getChannel();
        fcSize = fc.size();
        segmentSize = (int) Math.ceil((double) fcSize / CONCURRENT_NUM);

        calculate();

        String resStr = temperatureMap.toString();
        System.out.println(resStr);
    }

    private static void calculate() throws IOException, InterruptedException, ExecutionException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(CONCURRENT_NUM, CONCURRENT_NUM, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

        temperatureMap = new ConcurrentSkipListMap<>();
        preHeatDoubleCache();

        List<Future<Map<String, Temperature>>> res = new ArrayList<>();
        long startPos = 0;
        if (fcSize < 1000000) {
            Future<Map<String, Temperature>> partRes = executor.submit(new Task(startPos, fcSize));
            Map<String, Temperature> map = partRes.get();
            temperatureMap.putAll(map);
        }
        else {
            while (true) {
                if (startPos + segmentSize >= fcSize) {
                    Future<Map<String, Temperature>> partRes = executor.submit(new Task(startPos, fcSize - startPos));
                    res.add(partRes);
                    break;
                }
                else {
                    Future<Map<String, Temperature>> partRes = executor.submit(new Task(startPos, segmentSize));
                    res.add(partRes);
                    startPos += segmentSize;
                }
            }
            // 合并结果
            for (Future<Map<String, Temperature>> future : res) {
                Map<String, Temperature> stringTemperatureMap = future.get();
                for (Map.Entry<String, Temperature> entry : stringTemperatureMap.entrySet()) {
                    String station = entry.getKey();
                    Temperature value = entry.getValue();
                    temperatureMap.merge(station, value, (oldValue, newValue) -> oldValue.merge(newValue));
                }
            }
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);

        // 处理拼接的行信息,不超过总并发数,顺序处理
        for (Map.Entry<String, byte[]> entry : tempBytesMap.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("E")) {
                continue;
            }
            byte[] part1 = entry.getValue();
            byte[] part2 = tempBytesMap.getOrDefault("E" + key, new byte[0]);
            byte[] bytes = new byte[part1.length + part2.length];
            System.arraycopy(part1, 0, bytes, 0, part1.length);
            System.arraycopy(part2, 0, bytes, part1.length, part2.length);
            String[] lines = convertToString1(bytes, 0, bytes.length - 1);
            for (String line : lines) {
                try {
                    handleRecordConcurrently(line);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(line);
                }
            }
        }
    }

    private static class Task implements Callable<Map<String, Temperature>> {
        private long startPos;
        private long size;

        public Task(long startPos, long size) throws IOException {
            this.startPos = startPos;
            this.size = size;
        }

        @Override
        public Map<String, Temperature> call() throws Exception {
            Map<String, Temperature> map = new HashMap<>(10000);
            try {
                // 1亿个byte
                boolean firstRowHandled = false;

                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, startPos, size);
                byte[] lastLastRowBytes = null;
                while (buffer.hasRemaining()) {
                    byte[] bytes = new byte[10000];
                    // 先拼上上一次的最后一行
                    int startIndex = 0;
                    if (lastLastRowBytes != null) {
                        for (byte lastLastRowByte : lastLastRowBytes) {
                            bytes[startIndex++] = lastLastRowByte;
                        }
                    }
                    int readLength = Math.min(buffer.remaining(), 10000 - startIndex);
                    lastLastRowBytes = null;
                    buffer.get(bytes, startIndex, readLength);
                    // 处理第一行
                    int firstIndex = 0;
                    if (!firstRowHandled) {
                        firstRowHandled = true;
                        if (startPos == 0) {
                            // 全文第一行,不要特殊处理
                        }
                        else {
                            while (bytes[firstIndex] != 10) {
                                firstIndex++;
                            }
                            byte[] firstRowBytes = Arrays.copyOfRange(bytes, 0, firstIndex + 1);
                            tempBytesMap.put("E" + String.valueOf(startPos - 1), firstRowBytes);
                            firstIndex++;
                        }
                    }
                    // 分段的最后一行(可能不完整)
                    int lastIndex = startIndex + readLength - 1;

                    while (bytes[lastIndex] != 10) {
                        lastIndex--;
                    }
                    if (lastIndex == startIndex + readLength - 1) {
                        // 分段的最后一行是完整的
                    }
                    else {
                        // 暂存一下
                        lastLastRowBytes = Arrays.copyOfRange(bytes, lastIndex + 1, startIndex + readLength);
                    }

                    // [firstIndex, lastIndex] 这之间的数据是完整的多行数据
                    String[] lines = convertToString1(bytes, firstIndex, lastIndex);
                    handleRecord(map, lines);
                }
                // 处理最后一行
                if (lastLastRowBytes != null) {
                    tempBytesMap.put(String.valueOf(startPos + size - 1), Arrays.copyOf(lastLastRowBytes, lastLastRowBytes.length));
                }
                else {
                    tempBytesMap.put(String.valueOf(startPos + size - 1), new byte[0]);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            return map;
        }
    }

    private static void handleRecord(Map<String, Temperature> map, String[] records) {
        if (records == null || records.length == 0) {
            return;
        }
        for (String record : records) {
            if ("".equals(record)) {
                continue;
            }
            int index = record.indexOf(";");
            String station = record.substring(0, index);
            String stationValue = record.substring(index + 1);
            Temperature temperature = map.get(station);
            if (temperature == null) {
                temperature = new Temperature(stationValue);
                map.put(station, temperature);
            }
            else {
                temperature.update(stationValue);
            }
        }
    }

    private static void handleRecordConcurrently(String record) {
        if (record.isEmpty()) {
            return;
        }
        String[] split = record.split(";");
        String station = split[0];
        String stationValue = split[1];
        // temperatureMap中只能新增值,不会删除
        if (temperatureMap.get(station) == null) {
            if (temperatureMap.putIfAbsent(station, new Temperature(stationValue)) != null) {
                // 插入失败
                temperatureMap.computeIfPresent(station, (key, oldValue) -> oldValue.addRecord(stationValue));
            }
        }
        else {
            // 已经有值了
            temperatureMap.computeIfPresent(station, (key, oldValue) -> oldValue.addRecord(stationValue));
        }
    }

    /**
     *
     * @param bytes
     * @param start 起始索引,包含
     * @param end 结束索引,包含
     * @return
     */
    private static String[] convertToString1(byte[] bytes, int start, int end) {
        if (bytes == null || bytes.length == 0) {
            return new String[0];
        }
        String s = new String(bytes, start, (end - start + 1), StandardCharsets.UTF_8);
        String[] split = s.split("\n");
        return split;
    }

    // 预热-99.9到99.9之间的数,且始终包含一位小数
    private static void preHeatDoubleCache() {
        doubleCache = new ConcurrentHashMap<>();
        for (int i = -99; i < 99; i++) {
            for (int j = 0; j < 10; j++) {
                String stand = String.valueOf(i);
                String v = stand + "." + j;
                doubleCache.put(v, Double.parseDouble(v));
            }
        }
        for (int i = 0; i < 10; i++) {
            String stand = "-0";
            String v = stand + "." + i;
            doubleCache.put(v, Double.parseDouble(v));
        }

    }

    private static double parseDouble(String tempStr) {
        return doubleCache.get(tempStr);
    }
}
