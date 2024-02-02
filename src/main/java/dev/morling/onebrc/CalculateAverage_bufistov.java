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

import static java.lang.Math.toIntExact;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Future;

class ByteArrayWrapper {
    private final byte[] data;

    public ByteArrayWrapper(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object other) {
        return Arrays.equals(data, ((ByteArrayWrapper) other).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

public class CalculateAverage_bufistov {

    static class ResultRow {
        byte[] station;

        String stationString;
        long min, max, count, suma;

        ResultRow() {
        }

        ResultRow(byte[] station, long value) {
            this.station = new byte[station.length];
            System.arraycopy(station, 0, this.station, 0, station.length);
            this.min = value;
            this.max = value;
            this.count = 1;
            this.suma = value;
        }

        ResultRow(long value) {
            this.min = value;
            this.max = value;
            this.count = 1;
            this.suma = value;
        }

        void setStation(long startPosition, long endPosition) {
            this.station = new byte[(int) (endPosition - startPosition)];
            for (int i = 0; i < this.station.length; ++i) {
                this.station[i] = UNSAFE.getByte(startPosition + i);
            }
        }

        public String toString() {
            stationString = new String(station, StandardCharsets.UTF_8);
            return stationString + "=" + round(min / 10.0) + "/" + round(suma / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        void update(long newValue) {
            this.count += 1;
            this.suma += newValue;
            if (newValue < this.min) {
                this.min = newValue;
            }
            else if (newValue > this.max) {
                this.max = newValue;
            }
        }

        ResultRow merge(ResultRow another) {
            this.count += another.count;
            this.suma += another.suma;
            this.min = Math.min(this.min, another.min);
            this.max = Math.max(this.max, another.max);
            return this;
        }
    }

    static class OpenHash {
        ResultRow[] data;
        int dataSizeMask;

        // ResultRow metrics = new ResultRow();

        public OpenHash(int capacityPow2) {
            assert capacityPow2 <= 20;
            int dataSize = 1 << capacityPow2;
            dataSizeMask = dataSize - 1;
            data = new ResultRow[dataSize];
        }

        int hashByteArray(byte[] array) {
            int result = 0;
            long mask = 0;
            for (int i = 0; i < array.length; ++i, mask = ((mask + 1) & 3)) {
                result += array[i] << mask;
            }
            return result & dataSizeMask;
        }

        void merge(byte[] station, long value, int hashValue) {
            while (data[hashValue] != null && !Arrays.equals(station, data[hashValue].station)) {
                hashValue += 1;
                hashValue &= dataSizeMask;
            }
            if (data[hashValue] == null) {
                data[hashValue] = new ResultRow(station, value);
            }
            else {
                data[hashValue].update(value);
            }
            // metrics.update(delta);
        }

        void merge(byte[] station, long value) {
            merge(station, value, hashByteArray(station));
        }

        void merge(final long startPosition, long endPosition, int hashValue, long value) {
            while (data[hashValue] != null && !equalsToStation(startPosition, endPosition, data[hashValue].station)) {
                hashValue += 1;
                hashValue &= dataSizeMask;
            }
            if (data[hashValue] == null) {
                data[hashValue] = new ResultRow(value);
                data[hashValue].setStation(startPosition, endPosition);
            }
            else {
                data[hashValue].update(value);
            }
        }

        boolean equalsToStation(long startPosition, long endPosition, byte[] station) {
            if (endPosition - startPosition != station.length) {
                return false;
            }
            for (int i = 0; i < station.length; ++i, ++startPosition) {
                if (UNSAFE.getByte(startPosition) != station[i])
                    return false;
            }
            return true;
        }

        HashMap<ByteArrayWrapper, ResultRow> toJavaHashMap() {
            HashMap<ByteArrayWrapper, ResultRow> result = new HashMap<>(20000);
            for (int i = 0; i < data.length; ++i) {
                if (data[i] != null) {
                    var key = new ByteArrayWrapper(data[i].station);
                    result.put(key, data[i]);
                }
            }
            return result;
        }
    }

    static final Unsafe UNSAFE;

    static {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            UNSAFE = (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    static final long LINE_SEPARATOR = '\n';

    public static class FileRead implements Callable<HashMap<ByteArrayWrapper, ResultRow>> {

        private final FileChannel fileChannel;

        private long currentLocation;
        private long bytesToRead;

        private static final int hashCapacityPow2 = 18;

        static final int hashCapacityMask = (1 << hashCapacityPow2) - 1;

        public FileRead(FileChannel fileChannel, long startLocation, long bytesToRead, boolean firstSegment) {
            this.fileChannel = fileChannel;
            this.currentLocation = startLocation;
            this.bytesToRead = bytesToRead;
        }

        @Override
        public HashMap<ByteArrayWrapper, ResultRow> call() throws IOException {
            try {
                OpenHash openHash = new OpenHash(hashCapacityPow2);
                log("Reading the channel: " + currentLocation + ":" + bytesToRead);
                if (currentLocation > 0) {
                    toLineBeginPrefix();
                }
                toLineBeginSuffix();
                var memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, currentLocation, bytesToRead, Arena.global());
                currentLocation = memorySegment.address();
                processChunk(openHash);
                log("Done Reading the channel: " + currentLocation + ":" + bytesToRead);
                return openHash.toJavaHashMap();
            }
            catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        byte getByte(long position) throws IOException {
            MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, 1);
            return byteBuffer.get();
        }

        void toLineBeginPrefix() throws IOException {
            while (getByte(currentLocation - 1) != LINE_SEPARATOR) {
                ++currentLocation;
                --bytesToRead;
            }
        }

        void toLineBeginSuffix() throws IOException {
            while (getByte(currentLocation + bytesToRead - 1) != LINE_SEPARATOR) {
                ++bytesToRead;
            }
        }

        void processChunk(OpenHash result) {
            long nameBegin = currentLocation;
            long nameEnd = -1;
            long numberBegin = -1;
            int currentHash = 0;
            int currentMask = 0;
            int nameHash = 0;
            long end = currentLocation + bytesToRead;
            byte nextByte;
            for (; currentLocation < end; ++currentLocation) {
                nextByte = UNSAFE.getByte(currentLocation);
                if (nextByte == ';') {
                    nameEnd = currentLocation;
                    numberBegin = currentLocation + 1;
                    nameHash = currentHash & hashCapacityMask;
                }
                else if (nextByte == LINE_SEPARATOR) {
                    long value = getValue(numberBegin, currentLocation);
                    // log("Station name: '" + getStationName(nameBegin, nameEnd) + "' value: " + value + " hash: " + nameHash);
                    result.merge(nameBegin, nameEnd, nameHash, value);
                    nameBegin = currentLocation + 1;
                    currentHash = 0;
                    currentMask = 0;
                }
                else {
                    currentHash += (nextByte << currentMask);
                    currentMask = (currentMask + 1) & 3;
                }
            }
        }

        long getValue(long startLocation, long endLocation) {
            byte nextByte = UNSAFE.getByte(startLocation);
            boolean negate = nextByte == '-';
            long result = negate ? 0 : nextByte - '0';
            for (long i = startLocation + 1; i < endLocation; ++i) {
                nextByte = UNSAFE.getByte(i);
                if (nextByte != '.') {
                    result *= 10;
                    result += nextByte - '0';
                }
            }
            return negate ? -result : result;
        }

        String getStationName(long from, long to) {
            byte[] bytes = new byte[(int) (to - from)];
            for (int i = 0; i < bytes.length; ++i) {
                bytes[i] = UNSAFE.getByte(from + i);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    public static void main(String[] args) throws Exception {
        String fileName = "measurements.txt";
        if (args.length > 0 && args[0].length() > 0) {
            fileName = args[0];
        }
        log("InputFile: " + fileName);
        FileInputStream fileInputStream = new FileInputStream(fileName);
        int numThreads = 2 * Runtime.getRuntime().availableProcessors();
        if (args.length > 1) {
            numThreads = Integer.parseInt(args[1]);
        }
        log("NumThreads: " + numThreads);
        FileChannel channel = fileInputStream.getChannel();
        final long fileSize = channel.size();
        long remaining_size = fileSize;
        long chunk_size = Math.min((fileSize + numThreads - 1) / numThreads, Integer.MAX_VALUE - 5);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long startLocation = 0;
        ArrayList<Future<HashMap<ByteArrayWrapper, ResultRow>>> results = new ArrayList<>(numThreads);
        var fileChannel = FileChannel.open(Paths.get(fileName));
        boolean firstSegment = true;
        while (remaining_size > 0) {
            long actualSize = Math.min(chunk_size, remaining_size);
            results.add(executor.submit(new FileRead(fileChannel, startLocation, toIntExact(actualSize), firstSegment)));
            firstSegment = false;
            remaining_size -= actualSize;
            startLocation += actualSize;
        }
        executor.shutdown();

        // Wait for all threads to finish
        while (!executor.isTerminated()) {
            Thread.yield();
        }
        log("Finished all threads");
        fileInputStream.close();
        HashMap<ByteArrayWrapper, ResultRow> result = new HashMap<>(20000);
        for (var future : results) {
            for (var entry : future.get().entrySet()) {
                result.merge(entry.getKey(), entry.getValue(), ResultRow::merge);
            }
        }
        ResultRow[] finalResult = result.values().toArray(new ResultRow[0]);
        for (var row : finalResult) {
            row.toString();
        }
        Arrays.sort(finalResult, Comparator.comparing(a -> a.stationString));
        System.out.println("{" + String.join(", ", Arrays.stream(finalResult).map(ResultRow::toString).toList()) + "}");
        log("All done!");
    }

    static void log(String message) {
        // System.err.println(Instant.now() + "[" + Thread.currentThread().getName() + "]: " + message);
    }
}
