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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Phaser;

public class CalculateAverage_dpsoft {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_ROWS = 1 << 15;
    private static final int ROWS_MASK = MAX_ROWS - 1;

    public static void main(String[] args) throws IOException {
        final var cpus = Runtime.getRuntime().availableProcessors();
        final var segments = getMemorySegments(cpus);
        final var tasks = new MeasurementExtractor[segments.size()];
        final var phaser = new Phaser(segments.size());

        for (int i = 0; i < segments.size(); i++) {
            tasks[i] = new MeasurementExtractor(segments.get(i), phaser);
        }

        phaser.awaitAdvance(phaser.getPhase());

        final var allMeasurements = Arrays.stream(tasks)
                .parallel()
                .map(MeasurementExtractor::getMeasurements)
                .reduce(MeasurementMap::merge)
                .orElseThrow();

        System.out.println(sortSequentially(allMeasurements));

        System.exit(0);
    }

    private static Map<String, Measurement> sortSequentially(MeasurementMap allMeasurements) {
        final Map<String, Measurement> sorted = new TreeMap<>();
        for (Measurement m : allMeasurements.measurements) {
            if (m != null) {
                sorted.put(new String(m.name, StandardCharsets.UTF_8), m);
            }
        }
        return sorted;
    }

    // Inspired by @spullara
    private static List<FileSegment> getMemorySegments(int numberOfSegments) throws IOException {
        var file = new File(FILE);
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfSegments;
        List<FileSegment> segments = new ArrayList<>(numberOfSegments);

        if (fileSize < 1_000_000) {
            segments.add(new FileSegment(0, fileSize));
            return segments;
        }

        while (segmentSize >= Integer.MAX_VALUE) {
            numberOfSegments += 1;
            segmentSize = fileSize / numberOfSegments;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < numberOfSegments; i++) {
                long segStart = i * segmentSize;
                long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
                segStart = findSegment(i, 0, randomAccessFile, segStart, segEnd);
                segEnd = findSegment(i, numberOfSegments - 1, randomAccessFile, segEnd, fileSize);

                segments.add(new FileSegment(segStart, segEnd));
            }
        }
        return segments;
    }

    private static long findSegment(int i, int skipSegment, RandomAccessFile raf, long location, long fileSize) throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    break;
            }
        }
        return location;
    }

    record FileSegment(long start, long end) {
    }

    static final class MeasurementExtractor implements Runnable {
        private final FileSegment segment;
        private final Phaser phaser;
        private final MeasurementMap measurements = new MeasurementMap();

        MeasurementExtractor(FileSegment memorySegment, Phaser phaser) {
            this.segment = memorySegment;
            this.phaser = phaser;
            (new Thread(this)).start();
        }

        @Override
        public void run() {
            long segmentEnd = segment.end();
            try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
                var mbb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());
                mbb.order(ByteOrder.nativeOrder());

                if (segment.start() > 0) {
                    skipToFirstLine(mbb);
                }

                while (mbb.remaining() > 0 && mbb.position() <= segmentEnd) {
                    int pos = mbb.position();
                    int nameHash = hashAndRewind(mbb);
                    var m = measurements.getOrCompute(nameHash, mbb, pos);
                    int temp = readTemperatureFromBuffer(mbb);

                    m.sample(temp);
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Error reading file", e);
            }
            finally {
                phaser.arriveAndAwaitAdvance();
            }
        }

        // inspired by @lawrey
        private static int hashAndRewind(MappedByteBuffer mbb) {
            int hash = 0;
            int idx = mbb.position();
            outer: while (true) {
                int name = mbb.getInt();
                for (int c = 0; c < 4; c++) {
                    int b = (name >> (c << 3)) & 0xFF;
                    if (b == ';') {
                        idx += c + 1;
                        break outer;
                    }
                    hash ^= b * 82805;
                }
                idx += 4;
            }

            var rewind = mbb.position() - idx;
            mbb.position(mbb.position() - rewind);
            return hash;
        }

        private static int readTemperatureFromBuffer(MappedByteBuffer mbb) {
            int temp = 0;
            boolean negative = false;

            outer: while (mbb.remaining() > 0) {
                int b = mbb.get();
                switch (b) {
                    case '-':
                        negative = true;
                        break;
                    default:
                        temp = 10 * temp + (b - '0');
                        break;
                    case '.':
                        b = mbb.get();
                        temp = 10 * temp + (b - '0');
                    case '\r':
                        mbb.get();
                    case '\n':
                        break outer;
                }
            }
            if (negative)
                temp = -temp;
            return temp;
        }

        public MeasurementMap getMeasurements() {
            return measurements;
        }

        // Skips to the first line in the buffer, used for chunk processing.
        private static void skipToFirstLine(MappedByteBuffer mbb) {
            while ((mbb.get() & 0xFF) >= ' ') {
                // Skip bytes until reaching the start of a line.
            }
        }
    }

    // credits to @shipilev
    static class MeasurementMap {
        private final Measurement[] measurements = new Measurement[MAX_ROWS];

        public Measurement getOrCompute(int hash, MappedByteBuffer mbb, int position) {
            int index = hash & ROWS_MASK;
            var measurement = measurements[index];
            if (measurement != null && hash == measurement.nameHash && Measurement.equalsTo(measurement.name, mbb, position)) {
                return measurement;
            }
            else {
                return compute(hash, mbb, position);
            }
        }

        private Measurement compute(int hash, MappedByteBuffer mbb, int position) {
            var index = hash & ROWS_MASK;
            Measurement m;

            while (true) {
                m = measurements[index];
                if (m == null || (hash == m.nameHash && Measurement.equalsTo(m.name, mbb, position))) {
                    break;
                }
                index = (index + 1) & ROWS_MASK;
            }

            if (m == null) {
                int len = mbb.position() - position - 1;
                byte[] bytes = new byte[len];
                mbb.position(position);
                mbb.get(bytes, 0, len);
                mbb.get();
                measurements[index] = m = new Measurement(bytes, hash);
            }

            return m;
        }

        public MeasurementMap merge(MeasurementMap otherMap) {
            for (Measurement other : otherMap.measurements) {
                if (other == null)
                    continue;
                int index = other.nameHash & ROWS_MASK;
                while (true) {
                    Measurement m = measurements[index];
                    if (m == null) {
                        measurements[index] = other;
                        break;
                    }
                    else if (Arrays.equals(m.name, other.name)) {
                        m.merge(other);
                        break;
                    }
                    else {
                        index = (index + 1) & ROWS_MASK;
                    }
                }
            }
            return this;
        }
    }

    static final class Measurement {
        public final int nameHash;
        public final byte[] name;

        public long sum;
        public int count = 0;
        public int min = Integer.MAX_VALUE;
        public int max = Integer.MIN_VALUE;

        public Measurement(byte[] name, int nameHash) {
            this.name = name;
            this.nameHash = nameHash;
        }

        public static boolean equalsTo(byte[] name, MappedByteBuffer mbb, int position) {
            int len = mbb.position() - position - 1;
            if (len != name.length)
                return false;
            for (int i = 0; i < len; i++) {
                if (name[i] != mbb.get(position + i))
                    return false;
            }
            return true;
        }

        public void sample(int temp) {
            min = Math.min(min, temp);
            max = Math.max(max, temp);
            sum += temp;
            count++;
        }

        public Measurement merge(Measurement m2) {
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
            count += m2.count;
            return this;
        }

        public String toString() {
            return round(((double) min) / 10.0) + "/" + round((((double) sum) / 10.0) / count) + "/" + round(((double) max) / 10.0);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}