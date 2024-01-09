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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_maeda6uiui {
    record RecordCollectorResult(
                                 Map<String, Double> mins,
                                 Map<String, Double> maxes,
                                 Map<String, Double> sums,
                                 Map<String, Integer> counts) {

    }

    static class RecordCollector implements Callable<RecordCollectorResult> {
        private String inputFilepath;
        private long startByteIndex;
        private long numBytesToRead;
        private char delimiter;
        private int byteBufferSize;
        private int bisBufferSize;

        private Map<String, Double> mins;
        private Map<String, Double> maxes;
        private Map<String, Double> sums;
        private Map<String, Integer> counts;

        public RecordCollector(
                               String inputFilepath,
                               long startByteIndex,
                               long numBytesToRead,
                               char delimiter,
                               int byteBufferSize,
                               int bisBufferSize) {
            this.inputFilepath = inputFilepath;
            this.startByteIndex = startByteIndex;
            this.numBytesToRead = numBytesToRead;
            this.delimiter = delimiter;
            this.byteBufferSize = byteBufferSize;
            this.bisBufferSize = bisBufferSize;

            mins = new HashMap<>();
            maxes = new HashMap<>();
            sums = new HashMap<>();
            counts = new HashMap<>();
        }

        private int byteToInt(byte b) {
            return switch (b) {
                case '0' -> 0;
                case '1' -> 1;
                case '2' -> 2;
                case '3' -> 3;
                case '4' -> 4;
                case '5' -> 5;
                case '6' -> 6;
                case '7' -> 7;
                case '8' -> 8;
                case '9' -> 9;
                default -> -1;
            };
        }

        private double parseDouble(byte[] bs) {
            // Get the sign
            int valSign;
            if (bs[0] == '-') {
                valSign = -1;
            }
            else {
                valSign = 1;
            }

            // Get the dot position
            int dotPos = -1;
            for (int i = 0; i < bs.length; i++) {
                if (bs[i] == '.') {
                    dotPos = i;
                    break;
                }
            }
            if (dotPos == -1) {
                return Double.NaN;
            }

            // Get the integer part
            int valIntPart;
            int intPartStartIndex = (valSign == -1) ? 1 : 0;
            int intPartLength = dotPos - intPartStartIndex;

            // One-digit value
            if (intPartLength == 1) {
                valIntPart = this.byteToInt(bs[dotPos - 1]);
            }
            // Two-digit value
            else if (intPartLength == 2) {
                int valTens = this.byteToInt(bs[dotPos - 2]);
                int valOnes = this.byteToInt(bs[dotPos - 1]);
                valIntPart = valTens * 10 + valOnes;
            }
            else {
                return Double.NaN;
            }

            // Get the decimal part
            double valDecPart = this.byteToInt(bs[dotPos + 1]) * 0.1;

            return valSign * (valIntPart + valDecPart);
        }

        @Override
        public RecordCollectorResult call() {
            // Start and end indices are most likely pointing to the middle of a line
            // Therefore, actual start and end indices should be determined
            // before proceeding to actual reading of the file
            long actualStartByteIndex = -1;
            long actualEndByteIndex = -1;

            try (var bis = new BufferedInputStream(new FileInputStream(inputFilepath))) {
                int b;
                int readCount = 0;
                long firstLFPos;

                // If start index specified is 0, actual start index is also 0
                if (startByteIndex == 0) {
                    actualStartByteIndex = 0;
                }
                else {
                    // Skip until the preceding byte of the start index specified
                    bis.skipNBytes(startByteIndex - 1);

                    // Get the preceding byte
                    b = bis.read();

                    // If the preceding byte is LF,
                    // actual start index is the start index specified
                    // because it is the start of a new line
                    if (b == '\n') {
                        actualStartByteIndex = startByteIndex;
                    }
                }

                if (actualStartByteIndex != -1) {
                    // Skip until the end byte specified
                    bis.skipNBytes(numBytesToRead);
                }
                // Start index specified is pointing to the middle of a line
                // In that case, actual start index is the one following the LF of that line
                // (Start index of the next line)
                else {
                    firstLFPos = startByteIndex;
                    while ((b = bis.read()) != -1) {
                        readCount++;
                        if (b == '\n') {
                            break;
                        }

                        firstLFPos++;
                    }
                    actualStartByteIndex = firstLFPos + 1;

                    // Skip until the end byte specified
                    bis.skipNBytes(numBytesToRead - readCount);
                }

                // Actual end index is the first LF encountered
                readCount = 0;
                firstLFPos = startByteIndex + numBytesToRead;
                while ((b = bis.read()) != -1) {
                    readCount++;
                    if (b == '\n') {
                        break;
                    }

                    firstLFPos++;
                }
                actualEndByteIndex = firstLFPos;
            }
            catch (IOException e) {
                System.err.println(e);
                return null;
            }

            // Get actual number of bytes to read
            long actualNumBytesToRead = actualEndByteIndex - actualStartByteIndex + 1;

            // Read bytes from the range obtained above
            try (var bis = new BufferedInputStream(new FileInputStream(inputFilepath), bisBufferSize)) {
                // Skip until the start byte
                bis.skipNBytes(actualStartByteIndex);

                final int EXTENSION_SIZE = 64;
                var buffer = new byte[byteBufferSize];
                var extendedBuffer = new byte[byteBufferSize + EXTENSION_SIZE];

                // Read bytes in chunk
                long numTotalBytesRead = 0;
                while (true) {
                    int chunkSize;
                    if (actualNumBytesToRead - numTotalBytesRead < byteBufferSize) {
                        chunkSize = (int) (actualNumBytesToRead - numTotalBytesRead);
                    }
                    else {
                        chunkSize = byteBufferSize;
                    }

                    if (chunkSize <= 0) {
                        break;
                    }

                    Arrays.fill(buffer, (byte) 0);
                    bis.read(buffer, 0, chunkSize);
                    numTotalBytesRead += chunkSize;

                    // Copy read content to another buffer
                    Arrays.fill(extendedBuffer, (byte) 0);
                    System.arraycopy(buffer, 0, extendedBuffer, 0, chunkSize);

                    // Read until next LF is found
                    // if end of buffer read above does not correspond to end of line
                    for (int i = 0; i < EXTENSION_SIZE; i++) {
                        int b = bis.read();
                        if (b == -1) {
                            break;
                        }
                        else if (b == '\n') {
                            extendedBuffer[chunkSize + i] = '\n';
                            numTotalBytesRead++;
                            break;
                        }

                        extendedBuffer[chunkSize + i] = (byte) b;
                        numTotalBytesRead++;
                    }

                    int currentDelimPos = -1;
                    int currentLFPos = -1;
                    int nextLineStartPos = 0;

                    for (int i = 0; i < extendedBuffer.length; i++) {
                        if (extendedBuffer[i] == 0) {
                            break;
                        }

                        if (extendedBuffer[i] == delimiter) {
                            currentDelimPos = i;
                        }
                        else if (extendedBuffer[i] == '\n') {
                            currentLFPos = i;
                        }

                        if (currentLFPos != -1) {
                            // Error
                            if (currentDelimPos == -1) {
                                System.err.printf(
                                        "Error near byte index %d\n",
                                        actualStartByteIndex + numTotalBytesRead);
                            }
                            else {
                                String stationName = new String(
                                        Arrays.copyOfRange(extendedBuffer, nextLineStartPos, currentDelimPos));

                                // Parse string to double by myself
                                // because Double.parseDouble() is slow...
                                double temperature = this.parseDouble(
                                        Arrays.copyOfRange(extendedBuffer, currentDelimPos + 1, currentLFPos));

                                // Populate the maps
                                if (!mins.containsKey(stationName)) {
                                    mins.put(stationName, temperature);
                                    maxes.put(stationName, temperature);
                                    sums.put(stationName, temperature);
                                    counts.put(stationName, 1);
                                }
                                else {
                                    double currentMin = mins.get(stationName);
                                    double currentMax = maxes.get(stationName);
                                    double currentSum = sums.get(stationName);
                                    int currentCount = counts.get(stationName);

                                    if (temperature < currentMin) {
                                        mins.put(stationName, temperature);
                                    }
                                    else if (temperature > currentMax) {
                                        maxes.put(stationName, temperature);
                                    }

                                    sums.put(stationName, currentSum + temperature);
                                    counts.put(stationName, currentCount + 1);
                                }
                            }

                            nextLineStartPos = currentLFPos + 1;
                            currentDelimPos = -1;
                            currentLFPos = -1;
                        }
                    }
                }
            }
            catch (IOException e) {
                System.err.println(e);
                return null;
            }

            return new RecordCollectorResult(mins, maxes, sums, counts);
        }
    }

    private static double round(double d) {
        return Math.round(d * 10.0) / 10.0;
    }

    public static void main(String[] args) {
        final String INPUT_FILEPATH = "./measurements.txt";
        final int DESIRED_NUM_THREADS = 20;
        final char DELIMITER = ';';
        final int BIS_BUFFER_SIZE = 1024 * 1024;
        final int BYTE_BUFFER_SIZE = 1024;
        final int MULTI_THREAD_NUM_LINES_THRESHOLD = DESIRED_NUM_THREADS * 10;

        // First get the number of total bytes in the input file
        long numTotalBytes;
        try {
            numTotalBytes = Files.size(Paths.get(INPUT_FILEPATH));
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Make sure the input file has enough lines
        // for this multithreading approach to work efficiently
        int actualNumThreads = 1;
        try (var br = new BufferedReader(new FileReader(INPUT_FILEPATH))) {
            int lineCount = 0;
            while (br.readLine() != null) {
                lineCount++;
                if (lineCount >= MULTI_THREAD_NUM_LINES_THRESHOLD) {
                    actualNumThreads = DESIRED_NUM_THREADS;
                    break;
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Calculate the number of bytes each thread has to process
        long numBytesToProcessPerThread = numTotalBytes / actualNumThreads;
        long remainingNumBytesToProcess = numTotalBytes % actualNumThreads;

        var exec = Executors.newFixedThreadPool(actualNumThreads);

        var futures = new ArrayList<Future<RecordCollectorResult>>();
        for (int i = 0; i < actualNumThreads; i++) {
            RecordCollector recordCollector;
            if (i == actualNumThreads - 1) {
                recordCollector = new RecordCollector(
                        INPUT_FILEPATH,
                        i * numBytesToProcessPerThread,
                        numBytesToProcessPerThread + remainingNumBytesToProcess,
                        DELIMITER,
                        BYTE_BUFFER_SIZE,
                        BIS_BUFFER_SIZE);
            }
            else {
                recordCollector = new RecordCollector(
                        INPUT_FILEPATH,
                        i * numBytesToProcessPerThread,
                        numBytesToProcessPerThread,
                        DELIMITER,
                        BYTE_BUFFER_SIZE,
                        BIS_BUFFER_SIZE);
            }

            Future<RecordCollectorResult> future = exec.submit(recordCollector);
            futures.add(future);
        }

        // Consolidate results of each thread
        var mins = new HashMap<String, Double>();
        var maxes = new HashMap<String, Double>();
        var sums = new HashMap<String, Double>();
        var counts = new HashMap<String, Integer>();
        try {
            for (var future : futures) {
                RecordCollectorResult result = future.get();

                result.mins.forEach((k, v) -> {
                    if (!mins.containsKey(k)) {
                        mins.put(k, v);
                    }
                    else {
                        mins.put(k, Double.min(v, mins.get(k)));
                    }
                });
                result.maxes.forEach((k, v) -> {
                    if (!maxes.containsKey(k)) {
                        maxes.put(k, v);
                    }
                    else {
                        maxes.put(k, Double.max(v, maxes.get(k)));
                    }
                });
                result.sums.forEach((k, v) -> {
                    if (!sums.containsKey(k)) {
                        sums.put(k, v);
                    }
                    else {
                        sums.put(k, Double.sum(v, sums.get(k)));
                    }
                });
                result.counts.forEach((k, v) -> {
                    if (!counts.containsKey(k)) {
                        counts.put(k, v);
                    }
                    else {
                        counts.put(k, Integer.sum(v, counts.get(k)));
                    }
                });
            }
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return;
        }
        finally {
            exec.shutdown();
        }

        // Calculate means
        var means = new HashMap<String, Double>();
        sums.forEach((k, v) -> means.put(k, v / counts.get(k)));

        // Sort station names
        List<String> sortedStationNames = means
                .keySet()
                .stream()
                .sorted()
                .toList();

        // Create output string
        var sb = new StringBuilder();

        sb.append("{");
        sortedStationNames.forEach(stationName -> {
            sb
                    .append(stationName)
                    .append("=")
                    .append(round(mins.get(stationName)))
                    .append("/")
                    .append(round(means.get(stationName)))
                    .append("/")
                    .append(round(maxes.get(stationName)))
                    .append(", ");
        });
        sb.delete(sb.length() - 2, sb.length());
        sb.append("}");

        // Print result string
        System.out.println(sb);
    }
}