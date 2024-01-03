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
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class CalculateAverage_padreati {

    private static final VectorSpecies<Byte> species = ByteVector.SPECIES_PREFERRED;
    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_SIZE = 1024 * 1024;

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {

        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator(double seed) {
            this.min = seed;
            this.max = seed;
            this.sum = seed;
            this.count = 1L;
        }

        public MeasurementAggregator merge(double v) {
            this.min = Math.min(min, v);
            this.max = Math.max(max, v);
            this.sum += v;
            this.count++;
            return this;
        }

        public MeasurementAggregator merge(MeasurementAggregator b) {
            this.min = Math.min(min, b.min);
            this.max = Math.max(max, b.max);
            this.sum += b.sum;
            this.count += b.count;
            return this;
        }

        public ResultRow toResultRow() {
            return new ResultRow(min, sum / count, max);
        }
    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_padreati().run();
    }

    private void run() throws IOException {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var subtasks = runSplits(scope);
            scope.join();
            scope.throwIfFailed();

            TreeMap<String, ResultRow> measurements = collapseResults(subtasks);
            System.out.println(measurements);

        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<StructuredTaskScope.Subtask<Map<String, MeasurementAggregator>>> runSplits(StructuredTaskScope.ShutdownOnFailure scope)
            throws IOException {

        List<StructuredTaskScope.Subtask<Map<String, MeasurementAggregator>>> subtasks = new ArrayList<>();
        long last = 0L;

        File file = new File(FILE);
        long next = CHUNK_SIZE;
        while (next < file.length()) {
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
                long nnext = next + 1;
                long llast = last;
                subtasks.add(scope.fork(() -> chunkProcessor(file, llast, nnext)));
                last = nnext;
                next += CHUNK_SIZE;
            }
        }
        return subtasks;
    }

    private Map<String, MeasurementAggregator> chunkProcessor(File source, long start, long end) throws IOException {
        var map = new HashMap<String, MeasurementAggregator>();
        byte[] buffer = new byte[(int) (end - start)];
        int len;
        try (FileInputStream bis = new FileInputStream(source)) {
            bis.skip(start);
            len = bis.read(buffer, 0, buffer.length);
        }

        List<Byte> indexes = new ArrayList<>();

        int loopBound = species.loopBound(len);
        int i = 0;
        int last = 0;

        for (; i < loopBound; i += species.length()) {
            ByteVector v = ByteVector.fromArray(species, buffer, i);
            var mask1 = v.compare(VectorOperators.EQ, '\n');
            var mask2 = v.compare(VectorOperators.EQ, ';');
            last = pushIndexes(mask1.or(mask2), last, indexes);
        }
        for (; i < len; i++) {
            if (buffer[i] == '\n') {
                indexes.add((byte) (i - loopBound + last + 1));
                last = -i + loopBound - 1;
                continue;
            }
            if (buffer[i] == ';') {
                indexes.add((byte) (i - loopBound + last + 1));
                last = -i + loopBound - 1;
            }
        }

        int startLine = 0;
        for (int j = 0; j < indexes.size(); j += 2) {
            int commaLen = indexes.get(j);
            int nlLen = indexes.get(j + 1);
            String key = new String(buffer, startLine, commaLen - 1);
            startLine += commaLen;
            double value = parseDouble(buffer, startLine, nlLen - 1);
            if (!map.containsKey(key)) {
                map.put(key, new MeasurementAggregator(value));
            }
            else {
                map.get(key).merge(value);
            }
            startLine += nlLen;
        }
        return map;
    }

    private double parseDouble(byte[] buffer, int start, int len) {
        int sign = 1;
        if (buffer[start] == '-') {
            sign = -1;
        }
        int value = 0;
        for (int k = start + ((sign < 0) ? 1 : 0); k < start + len; k++) {
            if (buffer[k] != '.') {
                value = value * 10 + (buffer[k] - '0');
            }
        }
        return sign * value / 10.;
    }

    private int pushIndexes(VectorMask<Byte> mask, int prev, List<Byte> indexes) {
        for (int i = 0; i < species.length(); i++) {
            if (mask.laneIsSet(i)) {
                indexes.add((byte) (i + prev + 1));
                prev = -i - 1;
            }
        }
        return species.length() + prev;
    }

    private TreeMap<String, ResultRow> collapseResults(List<StructuredTaskScope.Subtask<Map<String, MeasurementAggregator>>> resultList) {
        Map<String, MeasurementAggregator> aggregate = resultList.getFirst().get();
        for (var subtask : resultList.subList(1, resultList.size())) {
            var map = subtask.get();
            for (var entry : map.entrySet()) {
                String key = entry.getKey();
                if (!aggregate.containsKey(key)) {
                    aggregate.put(key, entry.getValue());
                }
                else {
                    aggregate.get(key).merge(entry.getValue());
                }
            }
        }
        TreeMap<String, ResultRow> measurements = new TreeMap<>();
        for (var entry : aggregate.entrySet()) {
            measurements.put(entry.getKey(), entry.getValue().toResultRow());
        }
        return measurements;
    }

}
