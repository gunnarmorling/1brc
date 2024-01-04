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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.radughiorma.Arguments;

public class CalculateAverage_padreati {

    private static final VectorSpecies<Byte> species = ByteVector.SPECIES_PREFERRED;
    private static String FILE;
    private static final int CHUNK_SIZE = 1024 * 1024;

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private record MeasurementAggregator(double min, double max, double sum, long count) {

        public MeasurementAggregator(double seed) {
            this(seed, seed, seed, 1);
        }

        public MeasurementAggregator merge(MeasurementAggregator b) {
            return new MeasurementAggregator(
                    Math.min(min, b.min),
                    Math.max(max, b.max),
                    sum + b.sum,
                    count + b.count
            );
        }

        public ResultRow toResultRow() {
            return new ResultRow(min, sum / count, max);
        }
    }

    public static void main(String[] args) throws IOException {
        FILE = Arguments.measurmentsFilename(args);
        new CalculateAverage_padreati().run();
    }

    private void run() throws IOException {
        File file = new File(FILE);
        var splits = findFileSplits();
        List<StructuredTaskScope.Subtask<Map<String, MeasurementAggregator>>> subtasks = new ArrayList<>();
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            for (int i = 0; i < splits.size(); i++) {
                long splitStart = splits.get(i);
                long splitEnd = i < splits.size() - 1 ? splits.get(i + 1) : file.length() + 1;
                subtasks.add(scope.fork(() -> chunkProcessor(file, splitStart, splitEnd)));
            }
            scope.join();
            scope.throwIfFailed();

            var resultList = subtasks.stream().map(StructuredTaskScope.Subtask::get).toList();
            TreeMap<String, ResultRow> measurements = collapseResults(resultList);
            System.out.println(measurements);

        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Long> findFileSplits() throws IOException {
        var splits = new ArrayList<Long>();
        splits.add(0L);

        File file = new File(FILE);
        long next = CHUNK_SIZE;
        while (true) {
            if (next >= file.length()) {
                break;
            }
            try (FileInputStream fis = new FileInputStream(file)) {
                long skip = fis.skip(next);
                if (skip != next) {
                    throw new RuntimeException();
                }
                // find first new line
                while (true) {
                    int ch = fis.read();
                    if (ch != '\n') {
                        next++;
                        continue;
                    }
                    break;
                }
                // skip eventual \\r
                if (fis.read() == '\r') {
                    next++;
                }
                splits.add(next + 1);
                next += CHUNK_SIZE;
            }
        }
        return splits;
    }

    public Map<String, MeasurementAggregator> chunkProcessor(File source, long start, long end) throws IOException {
        var map = new HashMap<String, MeasurementAggregator>();
        byte[] buffer = new byte[(int) (end - start)];
        int len;
        try (FileInputStream bis = new FileInputStream(source)) {
            bis.skip(start);
            len = bis.read(buffer, 0, buffer.length);
        }

        List<Integer> nlIndexes = new ArrayList<>();
        List<Integer> commaIndexes = new ArrayList<>();

        int loopBound = species.loopBound(len);
        int i = 0;

        for (; i < loopBound; i += species.length()) {
            ByteVector v = ByteVector.fromArray(species, buffer, i);
            var mask = v.compare(VectorOperators.EQ, '\n');
            for (int j = 0; j < species.length(); j++) {
                if (mask.laneIsSet(j)) {
                    nlIndexes.add(i + j);
                }
            }
            mask = v.compare(VectorOperators.EQ, ';');
            for (int j = 0; j < species.length(); j++) {
                if (mask.laneIsSet(j)) {
                    commaIndexes.add(i + j);
                }
            }
        }
        for (; i < len; i++) {
            if (buffer[i] == '\n') {
                nlIndexes.add(i);
            }
            if (buffer[i] == ';') {
                commaIndexes.add(i);
            }
        }

        int startLine = 0;
        for (int j = 0; j < nlIndexes.size(); j++) {
            int endLine = nlIndexes.get(j);
            int commaIndex = commaIndexes.get(j);
            String key = new String(buffer, startLine, commaIndex - startLine);
            double value = Double.parseDouble(new String(buffer, commaIndex + 1, endLine - commaIndex - 1));
            map.merge(key, new MeasurementAggregator(value), MeasurementAggregator::merge);
            startLine = endLine + 1;
        }
        return map;
    }

    private TreeMap<String, ResultRow> collapseResults(List<Map<String, MeasurementAggregator>> resultList) {
        HashMap<String, MeasurementAggregator> aggregate = new HashMap<>();
        for (var map : resultList) {
            for (var entry : map.entrySet()) {
                aggregate.merge(entry.getKey(), entry.getValue(), MeasurementAggregator::merge);
            }
        }
        TreeMap<String, ResultRow> measurements = new TreeMap<>();
        for (var entry : aggregate.entrySet()) {
            measurements.put(entry.getKey(), entry.getValue().toResultRow());
        }
        return measurements;
    }

}
