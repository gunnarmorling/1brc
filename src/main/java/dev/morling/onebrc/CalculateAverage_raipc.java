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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.RecursiveTask;

public class CalculateAverage_raipc {
    private static final String FILE = "./measurements.txt";
    private static final int BUFFER_SIZE = 16 * 1024;

    private static final class AggregatedMeasurement {
        final ByteArrayWrapper station;
        private String stationStringCached;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int count;
        private long sum;

        private AggregatedMeasurement(ByteArrayWrapper station) {
            this.station = station;
        }

        public String station() {
            String s = this.stationStringCached;
            if (s == null) {
                this.stationStringCached = s = station.toString();
            }
            return s;
        }

        public AggregatedMeasurement merge(AggregatedMeasurement other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        public void add(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public void format(StringBuilder out) {
            out.append(station()).append('=')
                    .append(min / 10.0).append('/')
                    .append(Math.round(1.0 * sum / count) / 10.0).append('/')
                    .append(max / 10.0);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        File inputFile = new File(FILE);
        ParsingTask parsingTask = new ParsingTask(inputFile, 0, inputFile.length());
        AggregatedMeasurement[] results = parsingTask.fork().join().toArray();
        System.out.println(formatResult(results));
    }

    private static String formatResult(AggregatedMeasurement[] result) {
        Arrays.sort(result, Comparator.comparing(AggregatedMeasurement::station));
        StringBuilder out = new StringBuilder().append('{');
        for (AggregatedMeasurement item : result) {
            item.format(out);
            out.append(", ");
        }
        if (out.length() > 2) {
            out.setLength(out.length() - 2);
        }
        return out.append('}').toString();
    }

    private static class ParsingTask extends RecursiveTask<MyHashMap> {
        private static final int SPLIT_FACTOR = 4;
        private static final int MIN_TASK_SIZE = 256 * 1024;
        private final File file;
        private final long startPosition;
        private final long endPosition;

        private ParsingTask(File file, long startPosition, long endPosition) {
            this.file = file;
            this.startPosition = startPosition;
            this.endPosition = endPosition;
        }

        @Override
        protected MyHashMap compute() {
            long size = endPosition - startPosition;
            if (size <= MIN_TASK_SIZE || size < file.length() / Runtime.getRuntime().availableProcessors() / SPLIT_FACTOR) {
                return doCompute();
            }
            var firstHalf = new ParsingTask(file, startPosition, (startPosition + endPosition) / 2).fork();
            var secondHalf = new ParsingTask(file, (startPosition + endPosition) / 2, endPosition).fork();
            var firstHalfResults = firstHalf.join();
            var secondHalfResults = secondHalf.join();
            firstHalfResults.merge(secondHalfResults);
            return firstHalfResults;
        }

        private MyHashMap doCompute() {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                return new ParsingRoutine(startPosition, endPosition).parse(raf);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class ParsingRoutine {
        private final MyHashMap result = new MyHashMap(2048);
        private final byte[] partialContentBuff = new byte[128];
        private final ByteArrayWrapper reusableWrapper = new ByteArrayWrapper(partialContentBuff, 0, 0, 0);
        private int partialSize;
        private final long startPosition;
        private final long endPosition;

        private ParsingRoutine(long startPosition, long endPosition) {
            this.startPosition = startPosition;
            this.endPosition = endPosition;
        }

        private boolean hasPartialContent() {
            return partialSize > 0;
        }

        MyHashMap parse(RandomAccessFile raf) throws IOException {
            byte[] buffer = new byte[BUFFER_SIZE];
            long offset = findStartOffset(raf, buffer);
            boolean readMore = offset <= endPosition;
            while (readMore) {
                raf.seek(offset);
                int length = raf.read(buffer);
                if (length == -1) {
                    break;
                }
                int bufStart = 0;
                if (hasPartialContent()) {
                    int idxOfLf = indexOf(buffer, '\n', 0, length);
                    if (idxOfLf >= 0) {
                        bufStart = idxOfLf + 1;
                        System.arraycopy(buffer, 0, partialContentBuff, partialSize, bufStart);
                        int toProcess = partialSize + bufStart;
                        partialSize = 0;
                        processPart(offset - toProcess, partialContentBuff, 0, toProcess);
                    }
                    else {
                        System.arraycopy(buffer, 0, partialContentBuff, partialSize, bufStart);
                        offset += length;
                        continue;
                    }
                }
                readMore = processPart(offset, buffer, bufStart, length);
                offset += length;
            }
            return result;
        }

        long findStartOffset(RandomAccessFile raf, byte[] buffer) throws IOException {
            if (startPosition == 0) {
                return 0;
            }
            long offset = startPosition - 1;
            int length = 0;
            int idxOfLf = -1;
            while (offset < endPosition) {
                raf.seek(offset);
                length = raf.read(buffer);
                if (length == -1) {
                    throw new IllegalStateException("No content read on position " + offset);
                }
                offset += length;
                idxOfLf = indexOf(buffer, '\n', 0, length);
                if (idxOfLf >= 0) {
                    break;
                }
            }
            if (offset > startPosition) {
                int start = idxOfLf + 1;
                processPart(offset + start - length, buffer, start, length);
            }
            return offset;
        }

        boolean processPart(long position, byte[] buf, int start, int end) {
            ByteArrayWrapper key = reusableWrapper;
            key.content = buf;
            while (position <= endPosition) {
                int idxOfSemicolon = indexOf(buf, ';', start, end);
                if (idxOfSemicolon >= 0) {
                    int idxOfLf = indexOf(buf, '\n', idxOfSemicolon + 2, Math.max(idxOfSemicolon + 2, end));
                    if (idxOfLf >= 0) {
                        key.start = start;
                        key.length = idxOfSemicolon - start;
                        var aggregatedMeasurement = result.getOrCreate(key);
                        aggregatedMeasurement.add(parseInt(buf, idxOfSemicolon + 1, idxOfLf - 1));
                        int prevStart = start;
                        start = idxOfLf + 1;
                        position += start - prevStart;
                        continue;
                    }
                }
                partialSize = end - start;
                if (partialSize > 0) {
                    System.arraycopy(buf, start, partialContentBuff, 0, partialSize);
                }
                break;
            }
            return hasPartialContent() || position < endPosition;
        }

        private int parseInt(byte[] buf, int from, int toIncl) {
            int mul = 1;
            if (buf[from] == '-') {
                mul = -1;
                ++from;
            }
            int res = buf[toIncl] - '0';
            int dec = 10;
            for (int i = toIncl - 2; i >= from; --i) {
                res += (buf[i] - '0') * dec;
                dec *= 10;
            }
            return mul * res;
        }
    }

    private static final MethodHandle indexOfMH;
    private static final MethodHandle vectorizedHashCodeMH;
    private static final MethodHandle mismatchMH;

    static {
        try {
            Class<?> stringLatin1 = Class.forName("java.lang.StringLatin1");
            var lookup = MethodHandles.privateLookupIn(stringLatin1, MethodHandles.lookup());
            // int indexOf(byte[] value, int ch, int fromIndex, int toIndex)
            indexOfMH = lookup.findStatic(stringLatin1, "indexOf",
                    MethodType.methodType(int.class, byte[].class, int.class, int.class, int.class));

            Class<?> arraysSupport = Class.forName("jdk.internal.util.ArraysSupport");
            lookup = MethodHandles.privateLookupIn(arraysSupport, MethodHandles.lookup());
            // int vectorizedHashCode(Object array, int fromIndex, int length, int initialValue, int basicType)
            vectorizedHashCodeMH = lookup.findStatic(arraysSupport, "vectorizedHashCode",
                    MethodType.methodType(int.class, Object.class, int.class, int.class, int.class, int.class));
            lookup = MethodHandles.privateLookupIn(arraysSupport, MethodHandles.lookup());
            // int mismatch(byte[] a, int aFromIndex, byte[] b, int bFromIndex, int length)
            mismatchMH = lookup.findStatic(arraysSupport, "mismatch",
                    MethodType.methodType(int.class, byte[].class, int.class, byte[].class, int.class, int.class));
        }
        catch (Exception e) {
            throw new Error(e);
        }
    }

    static int indexOf(byte[] src, int ch, int fromIndex, int toIndex) {
        try {
            return (int) indexOfMH.invoke(src, ch, fromIndex, toIndex);
        }
        catch (Throwable e) {
            throw new Error(e);
        }
    }

    static int vectorizedHashCode(byte[] array, int fromIndex, int length) {
        try {
            return (int) vectorizedHashCodeMH.invoke((Object) array, fromIndex, length, 0, 8);
        }
        catch (Throwable e) {
            throw new Error(e);
        }
    }

    static boolean arraysEqual(byte[] a, int aFromIndex, byte[] b, int bFromIndex, int length) {
        try {
            return ((int) mismatchMH.invoke(a, aFromIndex, b, bFromIndex, length)) < 0;
        }
        catch (Throwable e) {
            throw new Error(e);
        }
    }

    private static class ByteArrayWrapper {
        private byte[] content;
        private int start;
        private int length;
        int hashCodeCached;

        ByteArrayWrapper(byte[] content, int start, int length, int hashCodeCached) {
            this.content = content;
            this.start = start;
            this.length = length;
            this.hashCodeCached = hashCodeCached;
        }

        int calculateHashCode() {
            return this.hashCodeCached = vectorizedHashCode(content, start, length);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ByteArrayWrapper bw && isEqualTo(bw);
        }

        boolean isEqualTo(ByteArrayWrapper other) {
            return length == other.length && arraysEqual(content, start, other.content, other.start, length);
        }

        @Override
        public int hashCode() {
            return hashCodeCached;
        }

        @Override
        public String toString() {
            return new String(content, start, length, StandardCharsets.UTF_8);
        }

        ByteArrayWrapper copy() {
            byte[] copy = Arrays.copyOfRange(content, start, start + length);
            return new ByteArrayWrapper(copy, 0, copy.length, hashCodeCached);
        }

    }

    private static class MyHashMap {
        private static final int INVERSE_LOAD_FACTOR = 2;
        private AggregatedMeasurement[] data;
        private int size;

        MyHashMap(int initialCapacity) {
            this.data = new AggregatedMeasurement[initialCapacity];
        }

        private void grow() {
            AggregatedMeasurement[] oldData = data;
            AggregatedMeasurement[] newData = new AggregatedMeasurement[oldData.length * 2];
            for (AggregatedMeasurement measurement : oldData) {
                if (measurement != null) {
                    put(measurement, newData);
                }
            }
            this.data = newData;
        }

        private void put(AggregatedMeasurement value, AggregatedMeasurement[] data) {
            int length = data.length;
            int hashCode = value.hashCode();
            int idx = length & (hashCode ^ (hashCode >> 16));
            for (int i = idx; i < length; ++i) {
                if (data[i] == null) {
                    data[i] = value;
                    return;
                }
            }
            for (int i = 0; i < idx; ++i) {
                if (data[i] == null) {
                    data[i] = value;
                    return;
                }
            }
        }

        AggregatedMeasurement getOrCreate(ByteArrayWrapper key) {
            AggregatedMeasurement[] data = this.data;
            int length = data.length;
            int hashCode = key.calculateHashCode();
            int idx = (length - 1) & (hashCode ^ (hashCode >> 16));
            AggregatedMeasurement result = doGetOrCreate(key, idx, length, data);
            return result != null ? result : doGetOrCreate(key, 0, idx, data);
        }

        private AggregatedMeasurement doGetOrCreate(ByteArrayWrapper key, int from, int to, AggregatedMeasurement[] data) {
            for (int i = from; i < to; ++i) {
                AggregatedMeasurement item = data[i];
                if (item != null) {
                    if (item.station.isEqualTo(key)) {
                        return item;
                    }
                }
                else {
                    AggregatedMeasurement result = new AggregatedMeasurement(key.copy());
                    ++size;
                    if (size * INVERSE_LOAD_FACTOR >= data.length) {
                        grow();
                        put(result, this.data);
                    }
                    else {
                        data[i] = result;
                    }
                    return result;
                }
            }
            return null;
        }

        void merge(MyHashMap other) {
            for (AggregatedMeasurement measurement : other.data) {
                if (measurement != null) {
                    merge(measurement);
                }
            }
        }

        private boolean merge(AggregatedMeasurement value) {
            AggregatedMeasurement[] data = this.data;
            int length = data.length;
            ByteArrayWrapper key = value.station;
            int hashCode = key.hashCode();
            int idx = (length - 1) & (hashCode ^ (hashCode >> 16));
            return doMerge(key, value, idx, length, data) || doMerge(key, value, 0, idx, data);
        }

        private boolean doMerge(ByteArrayWrapper key, AggregatedMeasurement value, int from, int to, AggregatedMeasurement[] data) {
            for (int i = from; i < to; ++i) {
                AggregatedMeasurement item = data[i];
                if (item != null) {
                    if (item.station.isEqualTo(key)) {
                        item.merge(value);
                        return true;
                    }
                }
                else {
                    ++size;
                    if (size * INVERSE_LOAD_FACTOR >= data.length) {
                        grow();
                        put(value, this.data);
                    }
                    else {
                        data[i] = value;
                    }
                    return true;
                }
            }
            return false;
        }

        AggregatedMeasurement[] toArray() {
            AggregatedMeasurement[] result = new AggregatedMeasurement[size];
            int i = 0;
            for (AggregatedMeasurement measurement : data) {
                if (measurement != null) {
                    result[i++] = measurement;
                }
            }
            return result;
        }
    }

}
