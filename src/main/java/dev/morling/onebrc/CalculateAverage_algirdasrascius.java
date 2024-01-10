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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_algirdasrascius {

    private static final String FILE = "./measurements.txt";

    private static final int MAX_NAMES = 10000;
    private static final int MAP_ENTRY_COUNT = 20011; // Prime exceeding double MAX_NAMES count
    private static final int MAX_NAME_LENGTH_IN_CHARS = 100;
    private static final int MAX_NAME_LENGTH_IN_BYTES = MAX_NAME_LENGTH_IN_CHARS * 4;
    private static final int NAME_BUFFER_LENGTH = MAX_NAMES * MAX_NAME_LENGTH_IN_BYTES;

    private static class AggregatorMap {
        private final AggregatorMapEntry[] entries = new AggregatorMapEntry[MAP_ENTRY_COUNT];
        private final byte[] nameBuffer = new byte[NAME_BUFFER_LENGTH];
        private int nameBufferEnd = 0;

        void add(byte[] buffer, int nameStart, int nameEnd, int nameHash, short value) {
            getEntry(buffer, nameStart, nameEnd, nameHash).accumulate(value);
        }

        void combineWith(AggregatorMap other) {
            for (int i = 0; i < MAP_ENTRY_COUNT; i++) {
                AggregatorMapEntry entry = other.entries[i];
                while (entry != null) {
                    getEntry(other.nameBuffer, entry.nameStart, entry.nameEnd, entry.nameHash).combineWith(entry);
                    entry = entry.nextEntry;
                }
            }
        }

        void printResult() {
            Map<String, ResultRow> sortedMeasurements = new TreeMap<>();
            for (int i = 0; i < MAP_ENTRY_COUNT; i++) {
                AggregatorMapEntry entry = entries[i];
                while (entry != null) {
                    String name = new String(nameBuffer, entry.nameStart, entry.nameEnd - entry.nameStart, StandardCharsets.UTF_8);
                    sortedMeasurements.put(name, entry.result());
                    entry = entry.nextEntry;
                }
            }
            System.out.println(sortedMeasurements);
        }

        private AggregatorMapEntry getEntry(byte[] buffer, int nameStart, int nameEnd, int nameHash) {
            int index = (nameHash & 0x7FFFFFFF) % MAP_ENTRY_COUNT;
            AggregatorMapEntry firstEntry = entries[index];
            AggregatorMapEntry entry = firstEntry;
            while (entry != null && (entry.nameHash != nameHash || !Arrays.equals(buffer, nameStart, nameEnd, nameBuffer, entry.nameStart, entry.nameEnd))) {
                entry = entry.nextEntry;
            }
            if (entry == null) {
                int entryNameStart = nameBufferEnd;
                int nameLength = nameEnd - nameStart;
                System.arraycopy(buffer, nameStart, nameBuffer, entryNameStart, nameLength);
                nameBufferEnd += nameLength;
                entry = new AggregatorMapEntry(entryNameStart, nameBufferEnd, nameHash, firstEntry);
                entries[index] = entry;
            }
            return entry;
        }

    }

    private static class AggregatorMapEntry {
        private final int nameStart;
        private final int nameEnd;
        private final int nameHash;
        private final AggregatorMapEntry nextEntry;
        private short min = Short.MAX_VALUE;
        private short max = Short.MIN_VALUE;
        private long sum;
        private int count;

        public AggregatorMapEntry(int nameStart, int nameEnd, int nameHash, AggregatorMapEntry nextEntry) {
            this.nameStart = nameStart;
            this.nameEnd = nameEnd;
            this.nameHash = nameHash;
            this.nextEntry = nextEntry;
        }

        void accumulate(short value) {
            if (min > value) {
                min = value;
            }
            if (max < value) {
                max = value;
            }
            sum += value;
            count++;
        }

        void combineWith(AggregatorMapEntry other) {
            if (min > other.min) {
                min = other.min;
            }
            if (max < other.max) {
                max = other.max;
            }
            sum += other.sum;
            count += other.count;
        }

        ResultRow result() {
            // return new ResultRow(min, (short) ((sum + count / 2) / count), max);
            return new ResultRow(min, (short) Math.round(((double) sum) / count), max);
        }
    }

    private record ResultRow(short min, short mean, short max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private String round(short value) {
            return value >= 0
                    ? ((value / 10) + "." + (value % 10))
                    : ("-" + (-value / 10) + "." + (-value % 10));
        }
    }

    private static final int READ_CHUNK_SIZE = 1024 * 1024;
    private static final int MAX_LINE_SIZE = MAX_NAME_LENGTH_IN_BYTES + 10;
    private static final int READ_BUFFER_SIZE = READ_CHUNK_SIZE + MAX_LINE_SIZE;

    private static class ReaderTask implements Callable<AggregatorMap> {
        private final AggregatorMap aggregatorMap = new AggregatorMap();
        private final byte[] buffer = new byte[READ_BUFFER_SIZE];
        private final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        private final FileChannel channel;
        private final AtomicLong nextChunkPosition;

        public ReaderTask(FileChannel channel, AtomicLong nextChunkPosition) {
            this.channel = channel;
            this.nextChunkPosition = nextChunkPosition;
        }

        @Override
        public AggregatorMap call() throws Exception {
            while (processChunk()) {
            }
            return aggregatorMap;
        }

        private boolean processChunk() throws IOException {
            long channelPosition = nextChunkPosition.getAndAdd(READ_CHUNK_SIZE);
            int endIndex = 0;

            byteBuffer.rewind();
            do {
                int bytesRead = channel.read(byteBuffer, channelPosition + endIndex);
                if (bytesRead < 0) {
                    break;
                }
                endIndex += bytesRead;
            } while (endIndex < READ_CHUNK_SIZE);

            int index = 0;

            // If this is not the first chunk skip till first line end
            if (channelPosition != 0) {
                byte v;
                while (index < endIndex && buffer[index] != '\n') {
                    index++;
                }
                index++;
            }

            if (endIndex > READ_CHUNK_SIZE + 1) {
                endIndex = READ_CHUNK_SIZE + 1;
            }

            while (index < endIndex) {
                index = processLine(index);
            }

            return endIndex > READ_CHUNK_SIZE;
        }

        private int processLine(int index) {
            // Read station name
            byte v;
            int nameStart = index;
            int nameHash = 0;
            while ((v = buffer[index]) != ';') {
                index++;
                nameHash = 31 * nameHash + v;
            }
            int nameEnd = index;

            // Skip ;
            index++;

            // Read temperature value
            v = buffer[index++];
            boolean negative = false;
            if (v == '-') {
                negative = true;
                v = buffer[index++];
            }
            int value = v - '0';
            v = buffer[index++];
            if (v != '.') {
                value = value * 10 + (v - '0');
                index++;
            }
            v = buffer[index++];
            value = value * 10 + (v - '0');
            if (negative) {
                value = -value;
            }

            // Skip line feed
            index++;

            // System.out.println(new String(buffer, nameStart, nameEnd - nameStart) + " = " + value);
            aggregatorMap.add(buffer, nameStart, nameEnd, nameHash, (short) value);
            return index;
        }

    }

    public static void main(String[] args) throws Exception {
        try (FileInputStream stream = new FileInputStream(FILE)) {
            FileChannel channel = stream.getChannel();
            AtomicLong nextChunkPosition = new AtomicLong(0L);

            int threadCount = Runtime.getRuntime().availableProcessors() * 2;
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            ExecutorCompletionService<AggregatorMap> completionService = new ExecutorCompletionService<>(executorService);
            for (int i = 0; i < threadCount; i++) {
                completionService.submit(new ReaderTask(channel, nextChunkPosition));
            }

            AggregatorMap aggregatorMap = completionService.take().get();
            for (int i = 1; i < threadCount; i++) {
                aggregatorMap.combineWith(completionService.take().get());
            }
            aggregatorMap.printResult();
            executorService.close();
        }
    }
}
