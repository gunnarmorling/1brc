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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_hchiorean {

    private static Map<String, double[]> parseLines(Integer key, CharBuffer chars, ConcurrentMap<Integer, String> leftoversMap) {
        Map<String, double[]> data = new HashMap<>();

        int startIdx = 0;
        int endIdx = chars.length() - 1;
        while (chars.charAt(startIdx) != '\n') {
            ++startIdx;
        }
        while (chars.charAt(endIdx) != '\n') {
            --endIdx;
        }
        if (startIdx < endIdx) {
            parseSanitizedCharBuffer(chars, data, startIdx, endIdx);
        }
        String firstPartBeforeDelim = chars.subSequence(0, startIdx + 1).toString();
        String lastPartBeforeDelim = chars.subSequence(endIdx + 1, chars.length()).toString();
        leftoversMap.put(key, firstPartBeforeDelim + lastPartBeforeDelim);
        chars = null;
        return data;
    }

    private static void parseSanitizedCharBuffer(CharSequence sequence, Map<String, double[]> data, int startIdx, int endIdx) {
        StringBuilder parseBuffer = new StringBuilder();
        String name = null;
        for (int i = startIdx; i < endIdx; ++i) {
            char c = sequence.charAt(i);
            if (c == '\r') {
                continue;
            }
            if (c == '\n') {
                if (parseBuffer.isEmpty()) {
                    continue;
                }
                addParsedDataToMap(data, parseBuffer, name);
                parseBuffer.setLength(0);
                continue;
            }
            if (c == ';') {
                name = parseBuffer.toString();
                parseBuffer.setLength(0);
                continue;
            }
            parseBuffer.append(c);
        }
        if (!parseBuffer.isEmpty()) {
            assert name != null;
            addParsedDataToMap(data, parseBuffer, name);
        }
    }

    private static void addParsedDataToMap(Map<String, double[]> data, StringBuilder parseBuffer, String name) {
        String value = parseBuffer.toString();
        double valueNum = Double.parseDouble(value);
        double[] existingMeasurements = data.putIfAbsent(name, new double[]{ valueNum, valueNum, 1, valueNum });
        if (existingMeasurements != null) {
            existingMeasurements[0] = Math.min(existingMeasurements[0], valueNum);
            existingMeasurements[1] = Math.max(existingMeasurements[1], valueNum);
            ++existingMeasurements[2];
            existingMeasurements[3] += valueNum;
        }
    }

    static Map<String, double[]> readFile(File file) throws Exception {

        Map<String, double[]> aggregate = new TreeMap<>(Comparator.naturalOrder());
        List<Future<Map<String, double[]>>> futures = new ArrayList<>();
        ConcurrentMap<Integer, String> leftoversMap = new ConcurrentSkipListMap<>();

        Charset defaultCharset = Charset.defaultCharset();
        CharsetDecoder decoder = defaultCharset.newDecoder();

        int bufferCapacity = 10 * 1024 * 1024;

        long len = file.length();
        int idCounter = 0;
        try (
                ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
                FileChannel chan = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {

            ByteBuffer mainBuffer = ByteBuffer.allocate(bufferCapacity);
            int totalRead = 0;
            while (totalRead < len) {
                mainBuffer.clear();
                long read = chan.read(mainBuffer);
                if (read == -1) {
                    break;
                }
                totalRead += read;
                mainBuffer.flip();
                CharBuffer chars = null;
                for (;;) {
                    try {
                        chars = decoder.decode(mainBuffer);
                        break;
                    }
                    catch (CharacterCodingException e) {
                        // keep reading byte by byte until a valid sequence is decoded
                        mainBuffer.rewind();
                        ByteBuffer nextByte = ByteBuffer.allocate(1);
                        chan.read(nextByte);
                        nextByte.flip();
                        mainBuffer = ByteBuffer.allocate(mainBuffer.capacity() + 1).put(mainBuffer).put(nextByte);
                        mainBuffer.flip();
                    }
                }
                int nextId = idCounter++;
                CharBuffer finalChars = chars;
                futures.add(
                        executor.submit(() -> parseLines(Integer.valueOf(nextId), finalChars, leftoversMap)));
            }
        }
        for (Future<Map<String, double[]>> future : futures) {
            Map<String, double[]> chunk = future.get();
            aggregate(chunk, aggregate);
        }
        String leftovers = String.join("", leftoversMap.values());
        parseSanitizedCharBuffer(leftovers, aggregate, 0, leftovers.length());

        return aggregate;
    }

    private static void aggregate(Map<String, double[]> chunks, Map<String, double[]> aggregate) {
        for (Map.Entry<String, double[]> chunk : chunks.entrySet()) {
            String name = chunk.getKey();
            double[] chunkData = chunk.getValue();
            double[] aggregateData = aggregate.putIfAbsent(name, chunkData);
            if (aggregateData != null) {
                aggregateData[0] = Math.min(aggregateData[0], chunkData[0]);
                aggregateData[1] = Math.max(aggregateData[1], chunkData[1]);
                aggregateData[2] += chunkData[2];
                aggregateData[3] += chunkData[3];
            }
        }
    }

    static void print(Map<String, double[]> dataMap) {
        System.out.print("{");
        for (Iterator<Map.Entry<String, double[]>> dataEntryIt = dataMap.entrySet().iterator(); dataEntryIt.hasNext();) {
            String entryOutput = format(dataEntryIt.next());
            System.out.print(entryOutput);
            if (dataEntryIt.hasNext()) {
                System.out.print(", ");
            }
        }
        System.out.println("}");
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static String format(Map.Entry<String, double[]> entry) {
        double[] dataPoints = entry.getValue();
        double min = round(dataPoints[0]);
        double max = round(dataPoints[1]);
        double mean = round(dataPoints[3] / dataPoints[2]);
        return entry.getKey() + "=" + min + "/" + mean + "/" + max;
    }

    public static void main(String[] args) throws Exception {
        File file = new File("./measurements.txt");
        Map<String, double[]> data = readFile(file);
        print(data);
    }
}
