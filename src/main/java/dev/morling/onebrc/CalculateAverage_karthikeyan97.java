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

import sun.misc.Unsafe;

import static java.util.stream.Collectors.*;

import java.io.FileInputStream;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_karthikeyan97 {

    private static final String FILE = "./measurements.txt";

    private record Measurement(modifiedbytearray station, double value) {
    }

    private record customPair(String stationName, MeasurementAggregator agg) {
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private long sum;
        private long count;

        public String toString() {
            return new StringBuffer(14)
                    .append(round(min))
                    .append("/")
                    .append(round((1.0 * sum) / count))
                    .append("/")
                    .append(round(max)).toString();
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    public static void main(String[] args) throws Exception {
        // long start = System.nanoTime();
        System.setSecurityManager(null);
        Collector<Map.Entry<modifiedbytearray, MeasurementAggregator>, MeasurementAggregator, MeasurementAggregator> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    MeasurementAggregator agg = m.getValue();
                    if (a.min >= agg.min) {
                        a.min = agg.min;
                    }
                    if (a.max <= agg.max) {
                        a.max = agg.max;
                    }
                    a.max = Math.max(a.max, m.getValue().max);
                    a.sum += m.getValue().sum;
                    a.count += m.getValue().count;
                },
                (agg1, agg2) -> {
                    if (agg1.min <= agg2.min) {
                        agg2.min = agg1.min;
                    }
                    if (agg1.max >= agg2.max) {
                        agg2.max = agg1.max;
                    }
                    agg2.sum = agg1.sum + agg2.sum;
                    agg2.count = agg1.count + agg2.count;

                    return agg2;
                },
                agg -> agg);

        RandomAccessFile raf = new RandomAccessFile(FILE, "rw");
        long length = raf.length();
        int cores = length > 1000 ? Runtime.getRuntime().availableProcessors() : 1;
        long boundary[][] = new long[cores][2];
        long segments = length / (cores);
        long before = -1;
        for (int i = 0; i < cores - 1; i++) {
            boundary[i][0] = before + 1;
            byte[] b = new byte[107];
            if (before + segments - 107 > 0) {
                raf.seek(before + segments - 107);
            }
            else {
                raf.seek(0);
            }
            while (raf.read() != '\n') {
            }
            boundary[i][1] = raf.getChannel().position() - 1;
            before = boundary[i][1];
        }
        boundary[cores - 1][0] = before + 1;
        boundary[cores - 1][1] = length - 1;

        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe) f.get(null);

        int pageSize = unsafe.pageSize() * 10;

        System.out.println(new TreeMap((Arrays.stream(boundary).parallel().map(i -> {
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(FILE);
                FileChannel fileChannel = fileInputStream.getChannel();
                HashMap<modifiedbytearray, MeasurementAggregator> resultmap = new HashMap<>(12000, 100);

                ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);

                fileChannel.position(i[0]);
                int bytesReading = 0;
                double num = 0;
                int sign = 1;
                boolean isNumber = false;
                byte bi;
                modifiedbytearray stationName = null;
                int hascode = 1;
                int ctr = 0;
                byte[] arr = new byte[100];
                int arrptr = 0;
                int seglen = (int) (i[1] - i[0] + 1);
                while (bytesReading < seglen) {
                    buffer.clear();
                    int bytesRead = fileChannel.read(buffer);
                    if ((bytesReading + bytesRead) <= seglen) {
                        if (bytesRead < 0) {
                            bytesRead = 0;
                        }
                    }
                    else {
                        bytesRead = (seglen - bytesReading);
                    }
                    buffer.flip();
                    int bytesptr = 0;
                    byte[] bufferArr = new byte[bytesRead];
                    buffer.get(bufferArr);
                    while (bytesptr < bytesRead) {
                        bytesReading += 1;
                        bi = bufferArr[bytesptr++];
                        if (ctr > 0) {
                            arr[arrptr++] = bi;
                            hascode = 31 * hascode + bi;
                            ctr--;
                        }
                        else {
                            if (bi >= 240) {
                                arr[arrptr++] = bi;
                                hascode = 31 * hascode + bi;
                                ctr = 3;
                            }
                            else if (bi >= 224) {
                                arr[arrptr++] = bi;
                                hascode = 31 * hascode + bi;
                                ctr = 2;
                            }
                            else if (bi >= 192) {
                                arr[arrptr++] = bi;
                                hascode = 31 * hascode + bi;
                                ctr = 1;
                            }
                            else if (bi == 59) {
                                isNumber = true;
                                stationName = new modifiedbytearray(arr, arrptr, hascode);
                                arr = new byte[100];
                                arrptr = 0;
                                hascode = 1;
                            }
                            else if (bi == 10) {
                                hascode = 1;
                                isNumber = false;
                                MeasurementAggregator agg = resultmap.get(stationName);
                                num *= sign;
                                if (agg == null) {
                                    agg = new MeasurementAggregator();
                                    agg.min = num;
                                    agg.max = num;
                                    agg.sum = (long) (num);
                                    agg.count = 1;
                                    resultmap.put(stationName, agg);
                                }
                                else {
                                    if (agg.min >= num) {
                                        agg.min = num;
                                    }
                                    if (agg.max <= num) {
                                        agg.max = num;
                                    }
                                    agg.sum += (long) (num);
                                    agg.count++;
                                }
                                num = 0;
                                sign = 1;
                            }
                            else {
                                hascode = 31 * hascode + bi;
                                if (isNumber) {
                                    switch (bi) {
                                        case 0x2E:
                                            break;
                                        case 0x2D:
                                            sign = -1;
                                            break;
                                        default:
                                            num = num * 10 + (bi - 0x30);
                                    }
                                }
                                else {
                                    arr[arrptr++] = bi;
                                }
                            }
                        }
                    }
                }
                /*
                 * while (bytesReading < (i[1] - i[0] + 1) && buffer.position() < buffer.limit()) {
                 * buffer.clear();
                 * bytesRead = fileChannel.read(buffer);
                 * buffer.flip();
                 * while (bytesReading <= (i[1] - i[0]) && buffer.position() < buffer.limit()) {
                 * bytesReading += 1;
                 * bi = buffer.get();
                 * String s;
                 * if (ctr > 0) {
                 * hascode = 31 * hascode + bi;
                 * ctr--;
                 * }
                 * else {
                 * if (bi >= 240) {
                 * ctr = 3;
                 * }
                 * else if (bi >= 224) {
                 * ctr = 2;
                 * }
                 * else if (bi >= 192) {
                 * ctr = 1;
                 * }
                 * else if (bi == 59) {
                 * isNumber = true;
                 * System.out.println(buffer);
                 * stationName = new modifiedbytearray(bbstart, buffer.position() - 1, hascode, buffer);
                 * hascode = 1;
                 * bbstart = buffer.position();
                 * }
                 * else if (bi == 10) {
                 * hascode = 1;
                 * isNumber = false;
                 * MeasurementAggregator agg = resultmap.get(stationName);
                 * if (agg == null) {
                 * agg = new MeasurementAggregator();
                 * agg.min = num * sign;
                 * agg.max = num * sign;
                 * agg.sum = (long) (num * sign);
                 * agg.count = 1;
                 * resultmap.put(stationName, agg);
                 * }
                 * else {
                 * agg.min = Math.min(agg.min, num * sign);
                 * agg.max = Math.max(agg.max, num * sign);
                 * agg.sum += (long) (num * sign);
                 * agg.count++;
                 * }
                 * num = 1;
                 * bbstart = buffer.position();
                 * }
                 * else {
                 * hascode = 31 * hascode + bi;
                 * if (isNumber) {
                 * switch (bi) {
                 * case 0x2E:
                 * break;
                 * case 0x2D:
                 * num = num * -1;
                 * break;
                 * default:
                 * num = num * 10 + (bi - 0x30);
                 * }
                 * }
                 * }
                 * }
                 * }
                 * }
                 */
                return resultmap;
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }).flatMap(e -> e.entrySet().stream()).collect(groupingBy(e -> e.getKey(), collector)))) {
            @Override
            public Object put(Object key, Object value) {
                return super.put(((modifiedbytearray) key).getStationName(), value);
            }
        });

        /*
         * .map(a -> {
         * return a.stream().parallel().collect(groupingBy(m -> m.station(), collector));
         * }).flatMap(m -> m.entrySet()
         * .stream()
         */
        // Get the FileChannel from the FileInputStream

        // System.out.println("time taken:" + (System.nanoTime() - start) / 1000000);
        // System.out.println(measurements);
    }

}

class modifiedbytearray {
    private int length;
    private byte[] arr;
    public int hashcode;

    modifiedbytearray(byte[] arr, int length, int hashcode) {
        this.arr = arr;
        this.length = length;
        this.hashcode = hashcode;
    }

    public String getStationName() {
        return new String(this.getArr(), 0, length, StandardCharsets.UTF_8);
    }

    public byte[] getArr() {
        return this.arr;
    }

    @Override
    public String toString() {
        return getStationName();
    }

    @Override
    public boolean equals(Object obj) {
        modifiedbytearray b = (modifiedbytearray) obj;
        return Arrays.equals(this.getArr(), 0, length, b.arr, 0, b.length);
    }

    public int getHashcode() {
        return hashcode;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }
}
