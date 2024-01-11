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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/*
 * # Main Speed Drivers
 *
 * Changes were made in this order, each header includes the runtime before and after the change,
 * and whose implementation (if any) was used as a reference.
 *
 * ## Parallel Process Chunks [160.5 -> 18] [twobiers]
 *
 * Rather than reading data top to bottom and attempting to parallelize processing with batches
 * of the parsed data, we read chunks of data (about 1 MB) and parrallelize processing per chunk.
 *
 * Several implementations do this kind of processing using a FileChannel to map chunks to buffers,
 * the reference above gave the idea to use an iterator.
 *
 * ## Share Byte Array when Deserializing [18 -> 6.5] [Various]
 *
 * When deserializing names after going through the effort of processing one byte at a time
 * when processing a chunk of data we can re-use a single byte array to store the characters
 * that make up the name. This removes the need to allocate and de-allocate memory for the buffer.
 *
 * We can then use the new String(byte[], 0, length) constructor to create the String without
 * worrying about clearing the underlying byte array as we provide a length.
 *
 * For this one I did not use any particular implementation as a reference but have seen it in many.
 *
 * ## Store ints Compute Doubles at End [6.5 -> 6.2] [None]
 *
 * Since input has a single decimal only we can effectively ignore it, do all of our math with the
 * numbers as integers, then only when printing out divide by 10.0 to get the correct values.
 *
 * The impact of this is small, maybe even nothing in this implementation, but keeping it in place.
 *
 * ## Use graal [6.2 -> 5.3] [None]
 *
 * Change from 21.0.1-tem to 21.0.1-graal.
 *
 * ## Process ByteBuffer for Name then Value [5.3 -> 4.7] [None]
 *
 * This started as a refactor and turned out to have noticeable runtime impact, which is nice.
 *
 * Rather than processing the ByteBuffer in a single while (current != '\n') with a condition
 * to switch from getting the name to calculating the integer value on (current == ';') the
 * logic was split into 2 separate loops.
 *
 * The first, while (current != ';') and a second, while (current != '\n').
 *
 * # For my Own Reference
 *
 * ## Constraints
 *
 * - Station name: non null UTF-8 string of length [1, 100] bytes
 * - Temperature value: non null double [-99.9, 99.9] with one fractional digit
 * - Station names: maximum of 10,000 unique names
 *
 * ## Run Commands
 *
 * ./mvnw clean verify && ./test.sh MeanderingProgrammer
 *
 * ./mvnw clean verify && ./calculate_average_MeanderingProgrammer.sh
 *
 * ## Runtimes
 *
 * Baseline: 2:40.597
 * Current:  0:04.668
 */
public class CalculateAverage_MeanderingProgrammer {

    private static final String FILE = "./measurements.txt";

    private static class ChunkReader implements Iterator<ByteBuffer> {

        private static final long CHUNK_SIZE = 1_024 * 1_024;

        private final FileChannel channel;
        private final long size;
        private long read;

        public ChunkReader(Path path) throws Exception {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.size = this.channel.size();
            this.read = 0;
        }

        public long estimateIterations() {
            return this.size / CHUNK_SIZE;
        }

        @Override
        public boolean hasNext() {
            return this.nextChunkSize() > 0;
        }

        @Override
        public ByteBuffer next() {
            ByteBuffer buffer = null;
            try {
                buffer = this.channel.map(FileChannel.MapMode.READ_ONLY, this.read, this.nextChunkSize());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            // Logic to clamp buffer to last complete line
            int bufferSize = buffer.limit();
            while (buffer.get(bufferSize - 1) != '\n') {
                bufferSize--;
            }
            buffer.limit(bufferSize);
            this.read += bufferSize;
            return buffer;
        }

        private long nextChunkSize() {
            return Math.min(CHUNK_SIZE, this.size - this.read);
        }
    }

    private static record Row(String name, int value) {
    }

    private static class RowReader implements Iterator<Row> {

        private final ByteBuffer buffer;
        private final byte[] nameBuffer;

        public RowReader(ByteBuffer buffer) {
            this.buffer = buffer;
            this.nameBuffer = new byte[100];
        }

        @Override
        public boolean hasNext() {
            return this.buffer.hasRemaining();
        }

        @Override
        public Row next() {
            var index = 0;
            var current = buffer.get();
            while (current != ';') {
                this.nameBuffer[index] = current;
                index++;
                current = buffer.get();
            }
            var name = new String(this.nameBuffer, 0, index, StandardCharsets.UTF_8);

            var negative = false;
            var value = 0;
            current = buffer.get();
            while (current != '\n') {
                if (current == '-') {
                    negative = true;
                }
                else if (current != '.') {
                    value = (value * 10) + (current - '0');
                }
                current = buffer.get();
            }
            if (negative) {
                value *= -1;
            }

            return new Row(name, value);
        }
    }

    private static class Measurement {

        private int min;
        private int max;
        private long sum;
        private int count;

        public Measurement(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        public Measurement merge(Measurement other) {
            if (other.min < this.min) {
                this.min = other.min;
            }
            if (other.max > this.max) {
                this.max = other.max;
            }
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        @Override
        public String toString() {
            return String.format(
                    "%.1f/%.1f/%.1f",
                    this.min / 10.0,
                    (this.sum / 10.0) / this.count,
                    this.max / 10.0);
        }
    }

    public static void main(String[] args) throws Exception {
        run();
    }

    private static void run() throws Exception {
        var reader = new ChunkReader(Paths.get(FILE));
        var iterator = Spliterators.spliterator(reader, reader.estimateIterations(), Spliterator.IMMUTABLE);
        var measurements = StreamSupport.stream(iterator, true)
                .flatMap(buffer -> toMeasurements(buffer).entrySet().stream())
                .collect(Collectors.toConcurrentMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue(),
                        Measurement::merge));
        System.out.println(new TreeMap<>(measurements));
    }

    private static Map<String, Measurement> toMeasurements(ByteBuffer buffer) {
        var iterator = Spliterators.spliteratorUnknownSize(new RowReader(buffer), Spliterator.IMMUTABLE);
        return StreamSupport.stream(iterator, false)
                .collect(Collectors.toMap(
                        row -> row.name(),
                        row -> new Measurement(row.value()),
                        Measurement::merge));
    }
}
