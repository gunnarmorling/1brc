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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CalculateAverage_davecom {

    /*
     * Original Header Could Not Be Changed to Match Checks so...
     * Copyright 2024 David Kopec
     * Licensed under the Apache License, Version 2.0 (the "License");
     * Created by David Kopec with some inspiration from seijikun's solution
     * and assistance from GitHub Copilot.
     */

    private static final String FILE = "./measurements.txt";

    private static final ConcurrentHashMap<ByteBuffer, Integer> mins = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<ByteBuffer, Integer> maxs = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<ByteBuffer, Integer> sums = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<ByteBuffer, Integer> counts = new ConcurrentHashMap<>();

    public static void processChunk(MappedByteBuffer chunk, long chunkSize) {
        // setup
        chunk.load();
        HashMap<ByteBuffer, IntSummaryStatistics> values = new HashMap<>();

        // do the actual processing
        long end = chunk.position() + chunkSize;
        // byte[] name = new byte[128];
        int value = 0;
        byte b = 0;
        boolean negate = false;
        long nameStart = 0;
        long nameEnd = 0;
        int nameLength = 0;
        while (chunk.position() < end) {
            // read name up to semicolon
            nameStart = chunk.position();
            b = chunk.get();
            while (b != ';') {
                b = chunk.get();
            }
            nameEnd = chunk.position() - 1;
            nameLength = (int) (nameEnd - nameStart);
            // generate byte array for name
            ByteBuffer nameBuffer = ByteBuffer.allocate(nameLength);
            chunk.get(chunk.position() - nameLength - 1, nameBuffer.array(), 0, nameLength);
            // convert name to string
            // read value
            value = 0;
            b = chunk.get();
            negate = false;
            while (b != '\n') {
                if (b == '.') {
                    b = chunk.get();
                    continue;
                }
                else if (b == '-') {
                    negate = true;
                    b = chunk.get();
                    continue;
                }
                value = value * 10 + (b - '0');
                b = chunk.get();
            }
            if (negate) {
                value = -value;
            }

            if (values.containsKey(nameBuffer)) {
                values.get(nameBuffer).accept(value);
            }
            else {
                IntSummaryStatistics stats = new IntSummaryStatistics();
                stats.accept(value);
                values.put(nameBuffer, stats);
            }
        }

        for (ByteBuffer nameBfr : values.keySet()) {
            IntSummaryStatistics stats = values.get(nameBfr);
            mins.compute(nameBfr, (k, v) -> v == null ? stats.getMin() : Math.min(v, stats.getMin()));
            maxs.compute(nameBfr, (k, v) -> v == null ? stats.getMax() : Math.max(v, stats.getMax()));
            sums.compute(nameBfr, (k, v) -> v == null ? (int) stats.getSum() : (v + (int) stats.getSum()));
            counts.compute(nameBfr, (k, v) -> v == null ? (int) stats.getCount() : (v + (int) stats.getCount()));
        }
    }

    public static void outputResults() {
        // output results sorted by name with format {name}={min}/{mean}/{max} in one giant string
        // fast string concatenation starting with { and ending with } with a comma between each (no newlines)
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        // var sortedNames = mins.keySet().stream().sorted().toArray(String[]::new);

        DecimalFormat df = new DecimalFormat("0.0");
        df.setRoundingMode(RoundingMode.HALF_UP);
        List<String> sortedNames = mins.keySet().stream()
                .map(b -> new String(b.array(), 0, b.limit()))
                .sorted()
                .collect(Collectors.toList());
        for (String nameStr : sortedNames) {
            ByteBuffer name = ByteBuffer.wrap(nameStr.getBytes());
            double min = ((double) mins.get(name)) / 10;
            double max = ((double) maxs.get(name)) / 10;
            double average = ((double) sums.get(name)) / ((double) counts.get(name)) / 10;
            sb.append(nameStr);
            sb.append('=');
            sb.append(df.format(min));
            sb.append('/');
            sb.append(df.format(average));
            sb.append('/');
            sb.append(df.format(max));
            sb.append(',');
            sb.append(' ');
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 2);
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append('}');
        System.out.println(sb.toString());
    }

    public static void main(String[] args) throws IOException {
        // create thread pool
        ExecutorService es = Executors.newVirtualThreadPerTaskExecutor();

        // load file
        FileChannel fc = FileChannel.open(Path.of(FILE));

        // configuration information
        long fileSize = fc.size();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        int numChunks = numProcessors * 2000;
        // System.out.println("numProcessors: " + numProcessors);
        // System.out.println("numChunks: " + numChunks);

        // create check buffer
        ByteBuffer bb = ByteBuffer.allocateDirect(128);

        // find appropriate chunks
        // System.out.println("fileSize: " + fileSize);
        long chunkLimit = fileSize / numChunks;
        long chunkStart = 0;
        long chunkEnd = chunkLimit;
        // int chunkNum = 0;
        while (chunkEnd < fileSize) {
            // System.out.println("initiated chunkNum: " + chunkNum);
            // find the next newline
            fc.position(chunkEnd);
            bb.clear();
            fc.read(bb);
            bb.flip();
            while (bb.get() != '\n' && bb.position() < bb.limit()) {
            }
            chunkEnd = chunkEnd + bb.position();
            if (chunkEnd > fileSize) {
                chunkEnd = fileSize - 1;
            }
            // process chunk
            long chunkSize = chunkEnd - chunkStart;
            if (chunkSize < 1) {
                break;
            }
            // System.out.println("chunkStart: " + chunkStart);
            // System.out.println("chunkEnd: " + chunkEnd);
            // System.out.println("chunkSize: " + chunkSize);
            MappedByteBuffer chunk = fc.map(FileChannel.MapMode.READ_ONLY, chunkStart, chunkSize);
            // final int chunkNumFinal = chunkNum;
            es.submit(() -> {
                // System.out.println("started chunkNum: " + chunkNumFinal);
                processChunk(chunk, chunkSize);
                // System.out.println("finished chunkNum: " + chunkNumFinal);
            });
            chunkStart = chunkEnd;
            chunkEnd = chunkEnd + chunkLimit;
            if (chunkEnd > fileSize) {
                chunkEnd = fileSize - 1;
            }
            // chunkNum++;
        }
        es.close();

        outputResults();
    }
}
