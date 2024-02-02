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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalculateAverage_martin2038 {

    // private static final String FILE = "/Users/martin/Garden/blog/1BRC/1brc/./measurements.txt";

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum;
        private int count;

        void update(int temp) {
            update(1, temp, temp, temp);
        }

        void update(int cnt, long sm, int min, int max) {
            sum += sm;
            count += cnt;
            if (this.min > min) {
                this.min = min;
            }
            if (this.max < max) {
                this.max = max;
            }
        }

        void merge(MeasurementAggregator it) {
            update(it.count, it.sum, it.min, it.max);
        }

        public String toString() {
            var mean = this.sum / 10.0 / this.count;
            return (min / 10f) + "/" + Math.round(mean * 10) / 10f + "/" + (max / 10f);
        }
    }

    public static void main(String[] args) throws IOException {

        var file = new RandomAccessFile(FILE, "r");
        final int maxNameLength = 110;
        var fc = file.getChannel();
        split(file).stream().parallel().map(ck -> {
            // StrFastHashKey 比string快500ms
            var map = new HashMap<StrFastHashKey, MeasurementAggregator>(200);
            // var pb = System.currentTimeMillis();
            try {
                var mb = fc.map(MapMode.READ_ONLY, ck.start, ck.length);
                var buff = new byte[maxNameLength];
                while (mb.hasRemaining()) {
                    var name = readNextHashKey(buff, mb);
                    // var name = readNextString(buff, mb);// .intern();
                    var temp = readNextInt10Times(buff, mb);
                    add2map(map, name, temp);
                }
                // long end = ck.start + ck.length;
                // do {
                // var name = readNext(file, ';', 30).intern();
                // var temp = Double.parseDouble(readNext(file, '\n', 6));
                // var agg = map.computeIfAbsent(name,it->new MeasurementAggregator());
                // agg.update(temp);
                // }while (file.getFilePointer()<end);
            }
            catch (IOException | NumberFormatException e) {
                throw new RuntimeException(e);
            }
            // System.out.println("chunk end , cost : " + (System.currentTimeMillis() - pb));
            return map;
        }).reduce(CalculateAverage_martin2038::reduceMap).ifPresent(map -> {

            var sb = new StringBuilder(map.size() * 100);
            sb.append('{');
            map.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .forEachOrdered(kv -> sb.append(kv.getKey()).append('=').append(kv.getValue()).append(", "));
            sb.deleteCharAt(sb.length() - 1);
            sb.setCharAt(sb.length() - 1, '}');
            var resultStr = sb.toString();
            System.out.println(resultStr);
            // System.out.println(resultStr.hashCode());
        });

    }

    static <Key> HashMap<Key, MeasurementAggregator> reduceMap(HashMap<Key, MeasurementAggregator> aMap, HashMap<Key, MeasurementAggregator> bMap) {
        aMap.forEach((k, v) -> {
            var b = bMap.get(k);
            if (null == b) {
                bMap.put(k, v);
            }
            else {
                b.merge(v);
            }
        });
        return bMap;
    }

    static <Key> void add2map(Map<Key, MeasurementAggregator> map, Key name, int temp) {
        // 比computeIfAbsent 节约1秒
        var agg = map.get(name);
        if (null == agg) {
            agg = new MeasurementAggregator();
            map.put(name, agg);
        }
        // var agg = map.computeIfAbsent(name,it->new MeasurementAggregator());
        agg.update(temp);
    }

    record FileChunk(long start, long length) {
    }

    static List<FileChunk> split(RandomAccessFile file) throws IOException {
        long total = file.length();
        var threadNum = Math.max((int) (total / Integer.MAX_VALUE + 1), Runtime.getRuntime().availableProcessors());
        long avgChunkSize = total / threadNum;
        // System.out.println(avgChunkSize +" \t avgChunkSize : INT/MAX \t"+Integer.MAX_VALUE);
        // Exception in thread "main" java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE
        // at java.base/sun.nio.ch.FileChannelImpl.map(FileChannelImpl.java:1183)
        long lastStart = 0;
        var list = new ArrayList<FileChunk>(threadNum);
        for (var i = 0; i < threadNum - 1; i++) {
            var length = avgChunkSize;
            file.seek(lastStart + length);
            while (file.readByte() != '\n') {
                // file.seek(lastStart+ ++length);
                ++length;
            }
            // include the '\n'
            length++;
            list.add(new FileChunk(lastStart, length));
            lastStart += length;
            if (lastStart >= total) {
                return list;
            }
        }
        list.add(new FileChunk(lastStart, total - lastStart));
        return list;
    }

    static StrFastHashKey readNextHashKey(byte[] buf, MappedByteBuffer mb) {
        int i = 1;
        mb.get(buf, 0, i);
        byte b;
        while ((b = mb.get()) != ';') {
            buf[i++] = b;
        }
        return new StrFastHashKey(buf, i);
    }

    static String readNextString(byte[] buf, MappedByteBuffer mb) {
        int i = 1;
        mb.get(buf, 0, i);
        byte b;
        while ((b = mb.get()) != ';') {
            buf[i++] = b;
        }
        return new String(buf, 0, i);
    }

    // copy from CalculateAverage_3j5a
    // 替换 Double.parse
    // 时间 38秒 -> 5418 ms
    static int readNextInt10Times(byte[] buf, MappedByteBuffer mb) {
        final int min_number_len = 3;
        int i = min_number_len;
        mb.get(buf, 0, i);
        byte b;
        while ((b = mb.get()) != '\n') {
            buf[i++] = b;
        }
        // -3.2
        var zeroAscii = '0';
        int temperature = buf[--i] - zeroAscii;
        i--; // skipping dot
        var base = 10;
        while (i > 0) {
            b = buf[--i];
            if (b == '-') {
                temperature = -temperature;
            }
            else {
                temperature = base * (b - zeroAscii) + temperature;
                base *= base;
            }
        }
        return temperature;
    }

    // static String readNext(RandomAccessFile file, char endFlag,int initLength) throws IOException {
    // StringBuilder input = new StringBuilder(initLength);
    // int c = -1;
    // //boolean eol = false;
    //
    // while (true) {
    // c = file.read();
    // if( c == endFlag || c == -1) {
    // break;
    // }
    // input.append((char)c);
    // }
    //
    // //if ((c == -1) && (input.length() == 0)) {
    // // return null;
    // //}
    // return input.toString();
    // }

    static class StrFastHashKey implements Comparable<StrFastHashKey> {
        final byte[] name;
        final int hash;

        String nameStr;

        StrFastHashKey(byte[] buf, int size) {
            name = new byte[size];
            System.arraycopy(buf, 0, name, 0, size);
            // hash = calculateHash(name, 0, size - 1);
            // FNV1a save 100+ms than calculateHash
            hash = hashFNV1a(name, size);
        }

        @Override
        public boolean equals(Object o) {
            // if (this == o) {return true;}
            // if (o == null || getClass() != o.getClass()) {return false;}
            StrFastHashKey that = (StrFastHashKey) o;
            return hash == that.hash && Arrays.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            if (null == nameStr) {
                nameStr = new String(name);
            }
            return nameStr;
        }

        @Override
        public int compareTo(StrFastHashKey o) {
            return toString().compareTo(o.toString());
        }
    }

    private static final VarHandle LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder())
            .withInvokeExactBehavior();
    private static final VarHandle INT_VIEW = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder())
            .withInvokeExactBehavior();

    /**
     * This is a prime number that gives pretty
     * <a href="https://vanilla-java.github.io/2018/08/15/Looking-at-randomness-and-performance-for-hash-codes.html">good hash distributions</a>
     * on the data in this challenge.
     */
    private static final long RANDOM_PRIME = 0x7A646E4D;

    /**
     * The hash calculation is inspired by
     * <a href="https://questdb.io/blog/building-faster-hash-table-high-performance-sql-joins/#fastmap-internals">QuestDB FastMap</a>
     */
    private static int calculateHash(byte[] buffer, int startPosition, int endPosition) {
        long hash = 0;

        int position = startPosition;
        for (; position + Long.BYTES <= endPosition; position += Long.BYTES) {
            long value = (long) LONG_VIEW.get(buffer, position);
            hash = hash * RANDOM_PRIME + value;
        }

        if (position + Integer.BYTES <= endPosition) {
            int value = (int) INT_VIEW.get(buffer, position);
            hash = hash * RANDOM_PRIME + value;
            position += Integer.BYTES;
        }

        for (; position <= endPosition; position++) {
            hash = hash * RANDOM_PRIME + buffer[position];
        }
        hash = hash * RANDOM_PRIME;
        return (int) hash ^ (int) (hash >>> 32);
    }

    private static final int FNV1_32_INIT = 0x811c9dc5;
    private static final int FNV1_PRIME_32 = 16777619;

    /**
     * https://github.com/prasanthj/hasher/blob/master/src/main/java/hasher/FNV1a.java
     *
     * FNV1a 32 bit variant.
     *
     * @param data   - input byte array
     * @param length - length of array
     * @return - hashcode
     */
    public static int hashFNV1a(byte[] data, int length) {
        int hash = FNV1_32_INIT;
        for (int i = 0; i < length; i++) {
            hash ^= (data[i] & 0xff);
            hash *= FNV1_PRIME_32;
        }

        return hash;
    }
}
