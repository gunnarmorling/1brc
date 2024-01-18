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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Phaser;

/*
 * Credits to:
 *
 *  - @lawrey (Peter Lawrey), used his idea for generating a unique hash for each station.
 *  - @spullara, used his idea for splitting the file into segments.
 *
 */
public class CalculateAverage_dpsoft {
    private static final String FILE = "./measurements.txt";
    private static final int MAX_ROWS = 1 << 18;
    private static final int ROWS_MASK = MAX_ROWS - 1;
    private static final String[] stations = new String[MAX_ROWS];

    public static void main(String[] args) throws IOException {
        final var fileMemorySegment = getFileMemorySegment();
        final var segments = getMemorySegments(fileMemorySegment);
        final var tasks = new MeasurementExtractor[segments.size()];
        final var phaser = new Phaser(segments.size());

        for (int i = 0; i < segments.size(); i++) {
            final var task = new MeasurementExtractor(segments.get(i), phaser);
            tasks[i] = task;
        }

        phaser.awaitAdvance(phaser.getPhase());

        final var allMeasurementsMap = Arrays.stream(tasks)
                .parallel()
                .map(MeasurementExtractor::getMeasurements)
                .reduce(Measurement::mergeMeasurementMaps)
                .orElseThrow();

        final Map<String, Measurement> sortedMeasurementsMap = new TreeMap<>();

        for (int i = 0; i < stations.length; i++) {
            String station = stations[i];
            if (station != null)
                sortedMeasurementsMap.put(station, allMeasurementsMap[i]);
        }

        System.out.print('{');
        String sep = "";
        for (Map.Entry<String, Measurement> entry : sortedMeasurementsMap.entrySet()) {
            System.out.print(sep);
            System.out.print(entry);
            sep = ", ";
        }
        System.out.println("}");

        System.exit(0);
    }

    private static MemorySegment getFileMemorySegment() throws IOException {
        try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            // Map the entire file to memory using MemorySegment
            return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());
        }
    }

    // Credits to @spullara for the idea of splitting the file into segments
    private static List<FileSegment> getMemorySegments(MemorySegment memorySegment) {
        int numberOfSegments = Runtime.getRuntime().availableProcessors();
        long fileSize = memorySegment.byteSize();
        long segmentSize = fileSize / numberOfSegments;

        List<FileSegment> segments = new ArrayList<>(numberOfSegments);

        if (segmentSize < 1_000_000) {
            segments.add(new FileSegment(0, fileSize));
            return segments;
        }

        for (int i = 0; i < numberOfSegments; i++) {
            long segStart = i * segmentSize;
            long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
            segStart = findSegment(i, 0, memorySegment, segStart, segEnd);
            segEnd = findSegment(i, numberOfSegments - 1, memorySegment, segEnd, fileSize);
            segments.add(new FileSegment(segStart, segEnd));
        }

        return segments;
    }

    record FileSegment(long start, long end) {
    }

    private static long findSegment(int i, int skipSegment, MemorySegment memSeg, long location, long fileSize) {
        if (i != skipSegment) {
            long remaining = fileSize - location;
            int bufferSize = remaining < 64 ? (int) remaining : 64;
            MemorySegment slice = memSeg.asSlice(location, bufferSize);
            for (int offset = 0; offset < slice.byteSize(); offset++) {
                if (slice.get(ValueLayout.OfChar.JAVA_BYTE, offset) == '\n') {
                    return location + offset + 1;
                }
            }
        }
        return location;
    }

    static final class MeasurementExtractor implements Runnable {
        final static int SEMI_PATTERN = compilePattern((byte) ';');
        private final Measurement[] measurements = new Measurement[MAX_ROWS];
        private final FileSegment segment;
        private final Phaser phaser;

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
                    int key = readKey(mbb);
                    Measurement m = measurements[key];
                    if (m == null) {
                        m = measurements[key] = new Measurement();
                        if (stations[key] == null) {
                            int len = mbb.position() - pos - 1;
                            byte[] bytes = new byte[len];
                            mbb.position(pos);
                            mbb.get(bytes, 0, len);
                            mbb.get();
                            stations[key] = new String(bytes, StandardCharsets.UTF_8);
                        }
                    }
                    int temp = readTemperatureFromBuffer(mbb);
                    m.sample(temp / 10.0);
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Error reading file", e);
            }
            finally {
                phaser.arriveAndAwaitAdvance();
            }
        }

        public Measurement[] getMeasurements() {
            return measurements;
        }

        // Credits to @lawrey for the initial idea.
        private static int readKey(MappedByteBuffer mbb) {
            // Get the first integer from the buffer and treat it as the initial hash
            long hash = mbb.getInt();
            int rewind = -1;

            // If the hash does not contain a semicolon
            if (hasNotSemi(hash)) {
                do {
                    // Get the next integer from the buffer
                    int word = mbb.getInt();
                    // Find the position of the first occurrence of the semicolon in the word
                    var position = firstInstance(word, SEMI_PATTERN);
                    // Depending on the position of the semicolon, adjust the word and the rewind value
                    if (position == 3) {
                        rewind = 3;
                        word = ';';
                    }
                    else if (position == 2) {
                        rewind = 2;
                        word &= 0xFFFF;
                    }
                    else if (position == 1) {
                        rewind = 1;
                        word &= 0xFFFFFF;
                    }
                    else if (position == 0) {
                        rewind = 0;
                    }
                    // Update the hash value
                    hash = hash * 21503 + word;
                } while (rewind == -1);
                // Finalize the hash value
                hash += hash >>> 1;
                // Adjust the position in the buffer
                mbb.position(mbb.position() - rewind);
                // Return the hash value, ensuring it is positive and within the range of the array
                return (int) Math.abs(hash % ROWS_MASK);
            }
            // If the key is 4 bytes long and starts with a semicolon
            // Finalize the hash value
            hash += hash >>> 1;
            // Adjust the position in the buffer
            mbb.position(mbb.position() - rewind - 1);
            // Return the hash value, ensuring it is positive and within the range of the array
            return (int) Math.abs(hash % ROWS_MASK);
        }

        // Reads a temperature value from the buffer.
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

        // This method checks if a given hash does not contain a semicolon (';').
        // The hash is a long integer, and the method treats it as a sequence of bytes.
        private static boolean hasNotSemi(long hash) {
            return (hash & 0xFF000000) != (';' << 24);

        }

        // Skips to the first line in the buffer, used for chunk processing.
        private static void skipToFirstLine(MappedByteBuffer mbb) {
            while ((mbb.get() & 0xFF) >= ' ') {
                // Skip bytes until reaching the start of a line.
            }
        }

        // It takes a byte as an argument and creates a pattern that consists of four copies of the byte.
        // This pattern is stored in an integer and can be used to find the first occurrence of the byte in another integer.
        private static int compilePattern(byte byteToFind) {
            int pattern = byteToFind & 0xFF;
            return pattern | (pattern << 8) | (pattern << 16) | (pattern << 24);
        }

        // This method uses the SWAR (SIMD Within A Register) technique to find the first occurrence of a semicolon in an integer.
        // The integer is treated as four bytes, and the method checks each byte to see if it is a semicolon.
        private static int firstInstance(int bytes, int pattern) {
            int masked = bytes ^ pattern;
            int underflow = masked - 0x01010101;
            int clearHighBits = underflow & ~masked;
            int highBitOfSeparator = clearHighBits & 0x80808080;
            return Integer.numberOfLeadingZeros(highBitOfSeparator) / 8;
        }
    }

    static final class Measurement {
        double sum = 0.0;
        long count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        // Default constructor for Measurement.
        public Measurement() {
        }

        // Adds a new temperature sample and updates min, max, and average.
        public void sample(double temp) {
            min = Math.min(min, temp);
            max = Math.max(max, temp);
            sum += temp;
            count++;
        }

        // Returns a formatted string representing min, average, and max.
        public String toString() {
            return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
        }

        // Helper method to round a double value to one decimal place.
        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // Merges this Measurement with another Measurement.
        public Measurement merge(Measurement m2) {
            if (m2 == null) {
                return this;
            }
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
            count += m2.count;
            return this;
        }

        public static Measurement[] mergeMeasurementMaps(Measurement[] a, Measurement[] b) {
            for (int i = 0; i < a.length; i++) {
                if (a[i] != null)
                    a[i].merge(b[i]);
                else
                    a[i] = b[i];
            }
            return a;
        }
    }
}