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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_artpar {
    public static final int N_THREADS = 8;
    private static final String FILE = "./measurements.txt";
    private static final int INT_MAP_SIZE = 8192; // from calculateIntegerByteMapTest()
    final static int[] byteHashMapToInt = calculateIntegerByteMap();
    private static final Unsafe UNSAFE = initUnsafe();
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    // final int VECTOR_SIZE = 512;
    // final int VECTOR_SIZE_1 = VECTOR_SIZE - 1;
    final int AVERAGE_CHUNK_SIZE = 1024 * 64;
    final int AVERAGE_CHUNK_SIZE_1 = AVERAGE_CHUNK_SIZE - 1;

    public CalculateAverage_artpar() throws IOException {
        long start = Instant.now().toEpochMilli();
        Path measurementFile = Paths.get(FILE);
        long fileSize = Files.size(measurementFile);

        // System.out.println("File size - " + fileSize);
        int expectedChunkSize = Math.toIntExact(Math.min(fileSize / N_THREADS, Integer.MAX_VALUE / 2));

        ExecutorService threadPool = Executors.newFixedThreadPool(N_THREADS);

        long chunkStartPosition = 0;
        RandomAccessFile fis = new RandomAccessFile(measurementFile.toFile(), "r");
        List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
        long bytesReadCurrent = 0;

        FileChannel fileChannel = FileChannel.open(measurementFile, StandardOpenOption.READ);
        for (int i = 0; chunkStartPosition < fileSize; i++) {

            int chunkSize = expectedChunkSize;
            chunkSize = fis.skipBytes(chunkSize);

            bytesReadCurrent += chunkSize;
            while (((char) fis.read()) != '\n' && bytesReadCurrent < fileSize) {
                chunkSize++;
                bytesReadCurrent++;
            }

            // System.out.println("[" + chunkStartPosition + "] - [" + (chunkStartPosition + chunkSize) + " bytes");
            if (chunkStartPosition + chunkSize >= fileSize) {
                chunkSize = (int) Math.min(fileSize - chunkStartPosition, Integer.MAX_VALUE);
            }
            if (chunkSize < 1) {
                break;
            }
            if (chunkSize >= Integer.MAX_VALUE) {
                throw new RuntimeException();
            }

            // MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStartPosition,
            // chunkSize);

            ReaderRunnable readerRunnable = new ReaderRunnable(chunkStartPosition, chunkSize, fileChannel);
            Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
            // System.out.println("Added future [" + chunkStartPosition + "][" + chunkSize + "]");
            futures.add(future);
            chunkStartPosition = chunkStartPosition + chunkSize + 1;
        }

        fis.close();

        Map<String, MeasurementAggregator> globalMap = futures.parallelStream().flatMap(future -> {
            try {
                return future.get().entrySet().stream();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).parallel().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, MeasurementAggregator::combine));
        fileChannel.close();

        Map<String, ResultRow> results = globalMap.entrySet().stream().parallel()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().finish()));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        PrintStream printStream = new PrintStream(new BufferedOutputStream(System.out));
        // PrintStream printStream = System.out;
        printStream.print("{");

        boolean isFirst = true;
        for (Map.Entry<String, ResultRow> stringResultRowEntry : measurements.entrySet()) {
            if (!isFirst) {
                printStream.print(", ");
                printStream.flush();
            }
            printStream.flush();
            printStream.print(stringResultRowEntry.getKey());
            printStream.flush();
            printStream.print("=");
            printStream.flush();
            stringResultRowEntry.getValue().printTo(printStream);
            printStream.flush();
            isFirst = false;
        }

        System.out.print("}\n");

        // long end = Instant.now().toEpochMilli();
        // System.out.println((end - start) / 1000);

    }

    public static int[] calculateIntegerByteMapTest() {
        int[] intToIntMap = null;
        for (int j = 0; j < 10000; j++) {
            int length = 2000 + j;
            intToIntMap = new int[length];
            boolean hasHashClash = false;
            Map<Integer, Integer> byteHashToInt = new HashMap<>();
            for (int i = -999; i < 1000; i++) {
                int hashCode = hashInteger(i);

                // String s = new String(value);
                int position = hashCode & (length - 1);
                // System.out.printf("%.1f => %s length [%d] hash [%d] => %d\n", number, s, s.length(), hashCode, position);
                if (byteHashToInt.containsKey(hashCode) || intToIntMap[position] != 0) {
                    // System.err.println("HashClash [" + hashCode + "] -> " +
                    // byteHashToInt.get(
                    // hashCode) + " vs " + number + " == [" + position + "] =>" + intToIntMap[position]);
                    hasHashClash = true;
                    break;
                }
                else {
                    byteHashToInt.put(hashCode, i);
                    intToIntMap[position] = i;
                }
            }
            if (!hasHashClash) {
                // 8192
                System.out.println("NoHash clash at [" + length + "]");
                // throw new RuntimeException("clash");
                return intToIntMap;
            }

        }
        System.out.println("Fail");
        return null;
    }

    private static int hashInteger(int i) {
        float number = i / 10f;
        String numberString = String.format("%.1f", number);
        byte[] value = numberString.getBytes();

        int hashCode = 1;
        for (int k = 0; k < value.length; k++) {
            hashCode = hashCode * 31 + value[k];
        }
        return hashCode;
    }

    public static int[] calculateIntegerByteMap() {
        long start = System.currentTimeMillis();
        int[] intToIntMap = new int[INT_MAP_SIZE];
        for (int i = -999; i < 1000; i++) {
            float number = i / 10f;
            byte[] value = String.format("%.1f", number).getBytes();

            int hashCode = 1;
            for (byte b : value) {
                hashCode = hashCode * 31 + b;
            }
            int position = hashCode & (INT_MAP_SIZE - 1);
            intToIntMap[position] = i;
        }
        long end = System.currentTimeMillis();
        // System.out.println("calculateIntegerByteMap " + (end - start) + " ms");
        return intToIntMap;
    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_artpar();
    }

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

    static boolean unsafeEquals(long aStart, long aLength, long bStart, long bLength) {
        if (aLength != bLength) {
            return false;
        }
        for (int i = 0; i < aLength; ++i) {
            if (UNSAFE.getByte(aStart + i) != UNSAFE.getByte(bStart + i)) {
                return false;
            }
        }
        return true;
    }

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min / 10) + "/" + round(mean / 10) + "/" + round(max / 10);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public void printTo(PrintStream out) {
            out.printf("%.1f/%.1f/%.1f", min / 10, mean / 10, max / 10);
        }
    }

    private static class MeasurementAggregator {
        private int min = 999;
        private int max = -999;
        private double sum;
        private long count;

        MeasurementAggregator combine(MeasurementAggregator other) {
            min = other.min + ((min - other.min) & ((min - other.min) >> (32 * 8 - 1)));
            max = max - ((max - other.max) & ((max - other.max) >> (32 * 8 - 1)));
            sum += other.sum;
            count += other.count;
            return this;
        }

        void combine(int value) {
            sum += value;
            count++;

            min = value + ((min - value) & ((min - value) >> (32 * 8 - 1))); // min(x, y)
            max = max - ((max - value) & ((max - value) >> (32 * 8 - 1))); // max(x, y)
        }

        ResultRow finish() {
            double mean = (count > 0) ? sum / count : 0;
            return new ResultRow(min, mean, max);
        }
    }

    static class StationName {
        public final int hash;
        private final ByteBuffer nameBytes;
        private final MeasurementAggregator measurementAggregator = new MeasurementAggregator();
        public int count = 0;

        public StationName(ByteBuffer nameBytes, int hash) {
            this.nameBytes = nameBytes;
            this.hash = hash;
        }

    }

    private class ReaderRunnable {
        private final long startPosition;
        private final FileChannel fileChannel;
        private final int chunkSize;
        StationNameMap stationNameMap = new StationNameMap();

        private ReaderRunnable(long startPosition, int chunkSize, FileChannel fileChannel) throws IOException {
            this.chunkSize = chunkSize;
            this.startPosition = startPosition;
            this.fileChannel = fileChannel;
            // mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition, chunkSize);
        }

        public Map<String, MeasurementAggregator> run() throws IOException {
            MemorySegment mappedSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    startPosition, chunkSize, Arena.global());

            long rawBufferAddress = UNSAFE.allocateMemory(100);
            int rawBufferReadIndex = 0;
            long position = mappedSegment.address();
            long endPosition = position + chunkSize;
            byte b;
            int hash;
            int nameHash;

            hash = 1;

            while (position < endPosition) {

                while ((position < endPosition) &&
                        (b = UNSAFE.getByte(position++)) != ';') {
                    UNSAFE.putByte(rawBufferAddress + rawBufferReadIndex++, b);
                    hash = hash * 31 + b;
                }

                nameHash = hash;
                hash = 1;

                while ((position < endPosition) &&
                        (b = UNSAFE.getByte(position++)) != '\n') {
                    hash = hash * 31 + b;
                }
                stationNameMap.getOrCreate(rawBufferAddress, rawBufferReadIndex,
                        byteHashMapToInt[hash & (INT_MAP_SIZE - 1)], nameHash);
                rawBufferReadIndex = 0;
                hash = 1;

            }
            return Arrays.stream(stationNameMap.names).parallel().filter(Objects::nonNull).collect(
                    Collectors.toMap(e -> StandardCharsets.UTF_8.decode(e.nameBytes).toString(),
                            e -> e.measurementAggregator, MeasurementAggregator::combine));
        }
    }

    class StationNameMap {
        int[] indexes = new int[AVERAGE_CHUNK_SIZE];
        StationName[] names = new StationName[AVERAGE_CHUNK_SIZE];
        int currentIndex = 0;
        ByteBuffer bytesForName = ByteBuffer.allocateDirect(1000 * 100);
        int nameBufferIndex = 0;

        public void getOrCreate(long stationNameBytesAddress, int length, int doubleValue, int hash) {
            int position = hash & AVERAGE_CHUNK_SIZE_1;
            while (indexes[position] != 0 && (names[indexes[position]].hash != hash)) {
                position = ++position & AVERAGE_CHUNK_SIZE_1;
            }
            if (indexes[position] != 0) {
                StationName stationName = names[indexes[position]];
                stationName.measurementAggregator.combine(doubleValue);
            }
            else {
                ByteBuffer nameSlice = bytesForName.slice(nameBufferIndex, length);
                nameBufferIndex += length;
                for (int i = 0; i < length; i++) {
                    nameSlice.put(UNSAFE.getByte(stationNameBytesAddress + i));
                }
                nameSlice.flip();
                StationName stationName = new StationName(nameSlice, hash);
                indexes[position] = ++currentIndex;
                names[indexes[position]] = stationName;
                stationName.measurementAggregator.combine(doubleValue);
            }
        }
    }

}