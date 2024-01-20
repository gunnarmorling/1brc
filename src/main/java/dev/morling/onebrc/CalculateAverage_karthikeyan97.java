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
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_karthikeyan97 {

    private static final Unsafe UNSAFE = initUnsafe();

    private static final String FILE = "./measurements.txt";

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private record Measurement(modifiedbytearray station, double value) {
    }

    private record customPair(String stationName, MeasurementAggregator agg) {
    }

    private static class MeasurementAggregator {
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum;
        private long count;

        public String toString() {
            return new StringBuffer(14)
                    .append(round((1.0 * min)))
                    .append("/")
                    .append(round((1.0 * sum) / count))
                    .append("/")
                    .append(round((1.0 * max))).toString();
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    public static void main(String[] args) throws Exception {
        // long start = System.nanoTime();
        // System.setSecurityManager(null);
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

        RandomAccessFile raf = new RandomAccessFile(FILE, "r");
        FileChannel fileChannel = raf.getChannel();
        final long mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length(), Arena.global()).address();
        long length = raf.length();
        final long endAddress = mappedAddress + length - 1;
        int cores = length > 1000 ? Runtime.getRuntime().availableProcessors() * 2 : 1;
        long boundary[][] = new long[cores][2];
        long segments = length / (cores);
        long before = -1;
        for (int i = 0; i < cores - 1; i++) {
            boundary[i][0] = before + 1;
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

        int l3Size = (13 * 1024 * 1024);// unsafe.l3Size();

        System.out.println(new TreeMap((Arrays.stream(boundary).parallel().map(i -> {
            FileInputStream fileInputStream = null;
            try {
                int seglen = (int) (i[1] - i[0] + 1);
                HashMap<modifiedbytearray, MeasurementAggregator> resultmap = new HashMap<>(1000);
                long segstart = mappedAddress + i[0];
                int bytesRemaining = seglen;
                long num = 0;
                int sign = 1;
                boolean isNumber = false;
                byte bi;
                modifiedbytearray stationName = null;
                int hascode = 5381;
                while (bytesRemaining > 0) {
                    int bytesptr = 0;
                    // int bytesread = buffer.remaining() > l3Size ? l3Size : buffer.remaining();
                    // byte[] bufferArr = new byte[bytesread];
                    // buffer.get(bufferArr);
                    int bbstart = 0;
                    int readSize = bytesRemaining > l3Size ? l3Size : bytesRemaining;
                    int actualReadSize = (segstart + readSize + 110 > endAddress || readSize + 110 > i[1]) ? readSize : readSize + 110;
                    byte[] readArr = new byte[actualReadSize];

                    UNSAFE.copyMemory(null, segstart, readArr, UNSAFE.ARRAY_BYTE_BASE_OFFSET, actualReadSize);
                    while (bytesptr < actualReadSize) {
                        bi = readArr[bytesptr++];// UNSAFE.getByte(segstart + bytesReading++);
                        if (!isNumber) {
                            if (bi >= 192) {
                                hascode = (hascode << 5) + hascode ^ bi;
                            }
                            else if (bi == 59) {
                                isNumber = true;
                                stationName = new modifiedbytearray(readArr, bbstart, bytesptr - 2, hascode & 0xFFFFFFFF);
                                bbstart = 0;
                                hascode = 5381;
                                if (bytesptr >= readSize) {
                                    break;
                                }
                            }
                            else {
                                hascode = (hascode << 5) + hascode ^ bi;
                            }
                        }
                        else {
                            switch (bi) {
                                case 0x2E:
                                    break;
                                case 0x2D:
                                    sign = -1;
                                    break;
                                case 10:
                                    hascode = 5381;
                                    isNumber = false;
                                    bbstart = bytesptr;
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
                                    break;
                                default:
                                    num = num * 10 + (bi - 0x30);
                            }
                        }
                    }
                    bytesRemaining -= bytesptr;
                    segstart += bytesptr;
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

        // System.out.println("time taken1:" + (System.nanoTime() - start) / 1000000);
        // System.out.println(measurements);
    }

}

class modifiedbytearray {
    private int length;
    private int start;
    private int end;
    private byte[] arr;
    public int hashcode;

    modifiedbytearray(byte[] arr, int start, int end, int hashcode) {
        this.arr = arr;
        this.length = end - start + 1;
        this.end = end;
        this.start = start;
        this.hashcode = hashcode;
    }

    public String getStationName() {
        return new String(this.getArr(), start, length, StandardCharsets.UTF_8);
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
        return Arrays.equals(this.getArr(), start, end, b.arr, b.start, b.end);
    }

    public int getHashcode() {
        return hashcode;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }
}
