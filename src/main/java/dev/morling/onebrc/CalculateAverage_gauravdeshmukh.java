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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_gauravdeshmukh {

    private static final String FILE = "./measurements.txt";
    private static final byte NEGATIVE_SIGN_BYTE = 0x2D;
    private static final byte DOT_BYTE = 0x2E;
    private static final int SEARCH_SPACE_BUFFER_SIZE = 140;

    private static final long SEMI_COLON_MASK = 0x3B3B3B3B3B3B3B3BL;
    private static final long EOL_MASK = 0x0A0A0A0A0A0A0A0AL;

    private static class ByteString {
        final private String string;
        final private int staticHashCode;

        public ByteString(byte[] bytes) {
            this.string = new String(bytes, StandardCharsets.UTF_8);
            this.staticHashCode = this.string.hashCode();
        }

        public byte[] getBytes() {
            return string.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public boolean equals(Object bs) {
            return this.string.equals(bs.toString());
        }

        @Override
        public int hashCode() {
            return staticHashCode;
        }

        @Override
        public String toString() {
            return this.string;
        }
    }

    private static class Measurement {
        public ByteString station;
        public int value;

        public Measurement(ByteString station, int value) {
            this.station = station;
            this.value = value;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(station.toString());
            sb.append(";");
            sb.append(value);
            return sb.toString();
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private int sum;
        private long count;

        public String toString() {
            return round(min / 10.0) + "/" + round(sum * 1.0 / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws Exception {
        // long st = System.currentTimeMillis();
        int cores = 1;

        File file = new File(FILE);
        long fileSize = file.length();
        if (fileSize > 1048576) {
            cores = Runtime.getRuntime().availableProcessors();
        }
        long chunkSize = fileSize / cores;

        ExecutorService executorService = Executors.newFixedThreadPool(cores);
        List<ParallelFileReaderTask> callableTasks = new ArrayList<>(cores);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        long end = chunkSize, start = 0;
        for (int i = 0; i < cores; i++) {
            if (i < cores - 1) {
                MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, end, Math.min(SEARCH_SPACE_BUFFER_SIZE, fileSize - end));
                int eolIndex = -1;
                int extraBytes = 0;
                while (true) {
                    long word;
                    try {
                        word = mbb.getLong();
                    }
                    catch (java.nio.BufferUnderflowException ex) {
                        byte[] remainingBytes = ByteBuffer.allocate(8).putLong(0).array();
                        mbb.get(mbb.position(), remainingBytes, 0, mbb.remaining());
                        word = ByteBuffer.wrap(remainingBytes).getLong();
                    }
                    eolIndex = findEolInLong(word);
                    if (eolIndex > -1) {
                        extraBytes = extraBytes + eolIndex + 1;
                        break;
                    }
                    extraBytes += 8;
                }
                end = end + extraBytes;
            }

            callableTasks.add(new ParallelFileReaderTask(start, (end - start),
                    raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, (end - start))));
            start = end;
            end = Math.min(end + chunkSize, fileSize - 1);
        }
        List<Future<Map<ByteString, MeasurementAggregator>>> futures = executorService.invokeAll(callableTasks);
        List<Map<ByteString, MeasurementAggregator>> resultList = new ArrayList<>(futures.size());
        for (Future<Map<ByteString, MeasurementAggregator>> future : futures) {
            resultList.add(future.get());
        }

        Map<String, MeasurementAggregator> resultMap = new TreeMap<>();
        for (Map<ByteString, MeasurementAggregator> map : resultList) {
            for (Map.Entry<ByteString, MeasurementAggregator> entry : map.entrySet()) {
                MeasurementAggregator agg = resultMap.get(entry.getKey().toString());
                if (agg == null) {
                    agg = new MeasurementAggregator();
                    resultMap.put(entry.getKey().toString(), agg);
                }
                agg.min = Math.min(agg.min, entry.getValue().min);
                agg.max = Math.max(agg.max, entry.getValue().max);
                agg.sum = agg.sum + entry.getValue().sum;
                agg.count = agg.count + entry.getValue().count;
            }
        }
        System.out.println(resultMap);
        executorService.shutdown();
        // System.out.println("Time taken: " + (System.currentTimeMillis() - st));
    }

    private static int findEolInLong(long word) {
        return findPositionInLong(word, EOL_MASK);
    }

    private static int findSemiColonInLong(long word) {
        return findPositionInLong(word, SEMI_COLON_MASK);
    }

    private static int findPositionInLong(long word, long searchMask) {
        long maskedWord = word ^ searchMask;
        long tmp = (maskedWord - 0x0101010101010101L) & ~maskedWord & 0x8080808080808080L;
        return tmp == 0 ? -1 : (Long.numberOfLeadingZeros(tmp) >>> 3);
    }

    private static class ParallelFileReaderTask implements Callable<Map<ByteString, MeasurementAggregator>> {
        private long start;
        private int size;
        private MappedByteBuffer mbf;
        byte[] bytes;
        private static final int BATCH_READ_SIZE = 64;
        Map<ByteString, MeasurementAggregator> map;

        public ParallelFileReaderTask(long start, long size, MappedByteBuffer mbf) {
            this.start = start;
            this.size = (int) size;
            this.mbf = mbf;
            this.bytes = new byte[BATCH_READ_SIZE];
            this.map = new HashMap<>(10000);
        }

        @Override
        public Map<ByteString, MeasurementAggregator> call() throws Exception {
            int bytesReadTillNow = 0;
            int startOfStation = 0, startOfNumber = -1, endOfStation = -1, endOfNumber = -1;
            boolean isLastRead = false;
            try {
                while (bytesReadTillNow < this.size) {
                    int semiColonIndex = -1;
                    while (semiColonIndex == -1 && bytesReadTillNow < this.size) {
                        long currentWord;
                        try {
                            currentWord = mbf.getLong();
                        }
                        catch (java.nio.BufferUnderflowException ex) {
                            int remainingBytesCount = this.size - bytesReadTillNow;
                            byte[] remainingBytes = ByteBuffer.allocate(8).putLong(0).array();
                            mbf.get(bytesReadTillNow, remainingBytes, 0, remainingBytesCount);
                            currentWord = ByteBuffer.wrap(remainingBytes).getLong();
                        }
                        semiColonIndex = findSemiColonInLong(currentWord);
                        if (semiColonIndex > -1) {
                            endOfStation = bytesReadTillNow + semiColonIndex;
                            startOfNumber = bytesReadTillNow + semiColonIndex + 1;
                            mbf.position(startOfNumber);
                            bytesReadTillNow += semiColonIndex + 1;
                        }
                        else {
                            bytesReadTillNow += 8;
                        }
                    }

                    int stationLength = endOfStation - startOfStation;
                    byte[] stationBytes = new byte[stationLength];
                    mbf.get(startOfStation, stationBytes, 0, stationLength);

                    int eolIndex = -1;
                    while (eolIndex == -1 && bytesReadTillNow < this.size) {
                        long currentWord;
                        try {
                            currentWord = mbf.getLong();
                        }
                        catch (java.nio.BufferUnderflowException ex) {
                            int remainingBytesCount = this.size - bytesReadTillNow;
                            byte[] remainingBytes = ByteBuffer.allocate(8).putLong(0).array();
                            mbf.get(bytesReadTillNow, remainingBytes, 0, remainingBytesCount);
                            currentWord = ByteBuffer.wrap(remainingBytes).getLong();
                            isLastRead = true;
                        }
                        eolIndex = findEolInLong(currentWord);
                        if (eolIndex > -1) {
                            endOfNumber = bytesReadTillNow + eolIndex;
                            startOfStation = bytesReadTillNow + eolIndex + 1;
                            mbf.position(startOfStation);
                            bytesReadTillNow += eolIndex + 1;
                        }
                        else {
                            bytesReadTillNow += 8;
                        }
                        if (isLastRead) {
                            bytesReadTillNow = this.size;
                            if (eolIndex == -1) {
                                endOfNumber = this.size;
                            }
                        }
                    }

                    int numberLength = endOfNumber - startOfNumber;
                    byte[] numberBytes = new byte[numberLength];
                    mbf.get(startOfNumber, numberBytes, 0, numberLength);

                    Measurement measurement = new Measurement(new ByteString(stationBytes),
                            getIntegerFromTemperatureBytes(numberBytes));
                    MeasurementAggregator aggregator = this.map.get(measurement.station);
                    if (aggregator == null) {
                        aggregator = new MeasurementAggregator();
                        this.map.put(measurement.station, aggregator);
                    }
                    aggregator.min = Math.min(aggregator.min, measurement.value);
                    aggregator.max = Math.max(aggregator.max, measurement.value);
                    aggregator.sum += measurement.value;
                    aggregator.count++;
                }
            }
            catch (Exception ex) {
                throw ex;
            }

            return this.map;
        }

        private int getIntegerFromTemperatureBytes(byte[] numberBytes) {
            int firstDigitIndex = (numberBytes[0] ^ NEGATIVE_SIGN_BYTE) == 0 ? 1 : 0;
            int ret = 0;
            for (int i = firstDigitIndex; i < numberBytes.length; i++) {
                if ((numberBytes[i] ^ DOT_BYTE) != 0) {
                    ret = (ret << 3) + (ret << 1) + ((int) numberBytes[i] - 48);
                }
            }
            return (firstDigitIndex > 0) ? -ret : ret;
        }
    }
}
