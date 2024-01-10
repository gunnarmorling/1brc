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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An attempt at using the "new" Vector API for determining where newline and semicolons are.
 */
public class CalculateAverage_gabrielreid {

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int BYTE_SPECIES_LEN = BYTE_SPECIES.length();
    private static final byte SEMICOLON_BYTE = (byte) ';';
    private static final byte NEWLINE_BYTE = (byte) '\n';
    private static final byte NEG_BYTE = (byte) '-';

    private static final int BLOCK_READ_SIZE = 1024 * 1024 * 16;
    private static final int SUMMARY_TABLE_SIZE = 2048;
    private static final int MAP_INITIAL_SIZE = 450;

    /**
     * State with the full summary table, as well as leftover bytes between processed blocks that need to
     * be handled afterward.
     */
    record State(Map<String, CitySummary> map, byte[] remainderBytes) {
    }

    public static void main(String[] args) throws IOException {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numReadBuffers = numCores + 2;

        var blockBuilderQueue = new LinkedBlockingDeque<BlockBuilder>(numReadBuffers);
        for (int i = 0; i < numReadBuffers; i++) {
            blockBuilderQueue.add(new BlockBuilder(BLOCK_READ_SIZE));
        }
        try (var fjp = new ForkJoinPool(numCores)) {

            CompletableFuture<State> stateFuture = CompletableFuture.completedFuture(new State(new HashMap<>(MAP_INITIAL_SIZE), new byte[0]));

            try (var fis = new FileInputStream("./measurements.txt")) {
                var blockBuilder = Objects.requireNonNull(blockBuilderQueue.poll());
                boolean skipToNewline = false;
                int cnt;
                while ((cnt = fis.read(blockBuilder.readBuffer)) != -1) {

                    var localBlockBuilder = blockBuilder;
                    var localCnt = cnt;
                    var localSkipToNewline = skipToNewline;
                    skipToNewline = true;
                    stateFuture = stateFuture.thenCombine(
                            CompletableFuture.supplyAsync(() -> {
                                var summaryMap = localBlockBuilder.buildSummaryTable(localCnt, localSkipToNewline);

                                int unprocessedRemainderSize = localBlockBuilder.firstLineStart + (localCnt - localBlockBuilder.lastLineEnd);
                                var unprocessedBytes = new byte[unprocessedRemainderSize];
                                System.arraycopy(localBlockBuilder.readBuffer, 0, unprocessedBytes, 0, localBlockBuilder.firstLineStart);
                                System.arraycopy(localBlockBuilder.readBuffer, localBlockBuilder.lastLineEnd, unprocessedBytes, localBlockBuilder.firstLineStart,
                                        (localCnt - localBlockBuilder.lastLineEnd));

                                localBlockBuilder.reset();
                                blockBuilderQueue.add(localBlockBuilder);
                                return new State(summaryMap, unprocessedBytes);
                            }, fjp), (state, newState) -> {
                                newState.map.forEach(
                                        (k, v) -> state.map.merge(k, v, CitySummary::add));

                                var newRemainderBytes = new byte[state.remainderBytes.length + newState.remainderBytes.length];
                                System.arraycopy(state.remainderBytes, 0, newRemainderBytes, 0, state.remainderBytes.length);
                                System.arraycopy(newState.remainderBytes, 0, newRemainderBytes, state.remainderBytes.length, newState.remainderBytes.length);
                                return new State(state.map, newRemainderBytes);
                            });

                    try {
                        blockBuilder = blockBuilderQueue.poll(1, TimeUnit.HOURS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

            }

            stateFuture = stateFuture.thenApply(state -> {
                BlockBuilder blockBuilder;
                try {
                    blockBuilder = blockBuilderQueue.poll(1, TimeUnit.HOURS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                System.arraycopy(state.remainderBytes, 0, blockBuilder.readBuffer, 0, state.remainderBytes.length);

                var m = blockBuilder.buildSummaryTable(state.remainderBytes.length, false);
                m.forEach(
                        (k, v) -> state.map.merge(k, v, CitySummary::add));
                return new State(state.map, new byte[0]);
            });

            var state = stateFuture.join();
            System.out.println(STR."{\{state.map.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .map(e -> String.format(Locale.US, "%s=%.1f/%.1f/%.1f", e.getKey(), e.getValue().min / 10f,
                            (e.getValue().sum / (float) e.getValue().count) / 10f, e.getValue().max / 10f))
                    .collect(Collectors.joining(", "))}}");

    }}

    /**
     * Parses number values as integers from the byte array.
     * <p>
     * The multiplier is 1 if positive and -1 if negative.
     */
    static short parseNumFromLine(byte[] buf, int offset, int len) {
        return switch (len) {
            case 3 -> (short) ((((buf[offset] - '0') * 10) + buf[offset + 2] - '0'));
            case 4 -> (short) ((((buf[offset] - '0') * 100) + (buf[offset + 1] - '0') * 10 + buf[offset + 3] - '0'));
            default -> throw new IllegalStateException("Unexpected number length %d".formatted(len));
        };
    }

    /**
     * Aggregator of temperature values. All values are stored as integers internally (i.e. multiplied by 10).
     */
    static class CitySummary {
        int max;
        int min;
        long sum;
        int count;

        public CitySummary(int value) {
            this.max = value;
            this.min = value;
            this.sum = value;
            this.count = 1;
        }

        void add(int value) {
            this.max = Math.max(value, this.max);
            this.min = Math.min(value, this.min);
            this.sum += value;
            this.count++;
        }

        CitySummary add(CitySummary other) {
            this.max = Math.max(other.max, this.max);
            this.min = Math.min(other.min, this.min);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

    }

    /**
     * A wrapper around a city name.
     * <p>
     * Provides a view of a large read buffer, but can also be cloned and detached from the read buffer.
     */
    static final class ByteSlice {

        private final byte[] buf;
        private final int offset;
        private final int len;

        public ByteSlice(byte[] buf, int offset, int len) {
            this.buf = buf;
            this.offset = offset;
            this.len = len;
        }

        public String valueAsString() {
            return new String(this.buf, this.offset, this.len, StandardCharsets.UTF_8);
        }

        public int hashCode() {
            return hashCode(this.buf, this.offset, this.len);
        }

        public static int hashCode(byte[] buf, int offset, int len) {
            int result = 1;
            int i = 0;
            for (; i + 3 < len; i += 4) {
                result = 31 * 31 * 31 * 31 * result
                        + 31 * 31 * 31 * buf[offset + i]
                        + 31 * 31 * buf[offset + i + 1]
                        + 31 * buf[offset + i + 2]
                        + buf[offset + i + 3];
            }
            for (; i < len; i++) {
                result = 31 * result + buf[offset + i];
            }
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ByteSlice otherByteSlice) {
                return ByteSlice.equal(this, otherByteSlice);
            }
            return false;
        }

        public static boolean equal(ByteSlice a, ByteSlice b) {
            return Arrays.equals(a.buf, a.offset, a.offset + a.len, b.buf, b.offset, b.offset + b.len);
        }

        public static boolean equal(ByteSlice a, byte[] buf, int offset, int len) {
            return Arrays.equals(a.buf, a.offset, a.offset + a.len, buf, offset, offset + len);
        }

    }

    record ValueNode(ByteSlice byteSlice, CitySummary citySummary) {

    }

    static final class SummaryTable {

        private static final int LOAD_FACTOR = 4;

        private int size;
        private ValueNode[] values;
        private int valueCount;
        private int resizeThreshold;

        private byte[] localBufferBytes = new byte[MAP_INITIAL_SIZE * 100];
        private int localBufferPtr = 0;

        SummaryTable(int size) {
            this.size = size;
            this.values = new ValueNode[size];
            this.resizeThreshold = size / LOAD_FACTOR;
        }

        void reset() {
            for (int i = 0; i < size; i++) {
                this.values[i] = null;
            }
            localBufferPtr = 0;
        }

        public void addAll(SummaryTable other) {
            for (int i = 0; i < other.size; i++) {
                var otherSlice = other.values[i];
                if (otherSlice != null) {
                    putValueNode(otherSlice);
                }
            }
        }

        private void putValueNode(ValueNode valueNode) {
            int hashCode = valueNode.byteSlice.hashCode();
            int index = (hashCode & 0x7FFFFFFF) % size;
            while (values[index] != null) {
                if (ByteSlice.equal(values[index].byteSlice, valueNode.byteSlice)) {
                    values[index].citySummary.add(valueNode.citySummary);
                    return;
                }
                index = (index + (hashCode & 0xFF) + 1) % size;
            }
            values[index] = valueNode;
            valueCount++;
            resizeIfNecessary();
        }

        public void putTemperatureValue(byte[] buf, int offset, int len, int value) {

            int hashCode = ByteSlice.hashCode(buf, offset, len);
            int index = (hashCode & 0x7FFFFFFF) % size;
            while (values[index] != null) {
                if (ByteSlice.equal(values[index].byteSlice, buf, offset, len)) {
                    values[index].citySummary.add(value);
                    return;
                }
                index = (index + (hashCode & 0xFF) + 1) % size;
            }

            System.arraycopy(buf, offset, this.localBufferBytes, this.localBufferPtr, len);
            var byteSlice = new ByteSlice(this.localBufferBytes, this.localBufferPtr, len);
            var valueNode = new ValueNode(byteSlice, new CitySummary(value));
            localBufferPtr += len;

            values[index] = valueNode;
            valueCount++;
            resizeIfNecessary();
        }

        private void resizeIfNecessary() {
            if (valueCount >= resizeThreshold) {
                int newSize = size * 2;
                var resized = new SummaryTable(newSize);
                for (int i = 0; i < this.size; i++) {
                    if (this.values[i] != null) {
                        resized.putValueNode(this.values[i]);
                    }
                }
                resized.addAll(this);
                byte[] localBufferBytes = new byte[this.localBufferBytes.length * 2];
                System.arraycopy(this.localBufferBytes, 0, localBufferBytes, 0, this.localBufferPtr);
                this.values = resized.values;
                this.size = newSize;
                this.valueCount = resized.valueCount;
                this.resizeThreshold = newSize / LOAD_FACTOR;
                this.localBufferBytes = localBufferBytes;
            }
        }

        public Map<String, CitySummary> toMap() {
            HashMap<String, CitySummary> m = HashMap.newHashMap(valueCount);
            for (int i = 0; i < size; i++) {
                var valueNode = this.values[i];
                if (valueNode != null) {
                    m.put(valueNode.byteSlice.valueAsString(), valueNode.citySummary);
                }
            }
            return m;
        }
    }

    /**
     * Performs actual building of a SummaryTable from a read buffer.
     */
    static class BlockBuilder {
        final byte[] readBuffer;
        private final SummaryTable summaryTable;

        private int firstLineStart;
        private int lastLineEnd;

        public BlockBuilder(int readBufferSize) {
            this.readBuffer = new byte[readBufferSize];
            this.summaryTable = new SummaryTable(SUMMARY_TABLE_SIZE);
        }

        void reset() {
            firstLineStart = -1;
            lastLineEnd = -1;
            this.summaryTable.reset();

        }

        public Map<String, CitySummary> buildSummaryTable(int readByteCount, boolean skipToNewline) {
            parseLineSegments(readByteCount, skipToNewline);
            return summaryTable.toMap();
        }

        private void parseLineSegments(int byteCount, boolean skipToNewline) {
            var upperBound = BYTE_SPECIES.loopBound(byteCount) - BYTE_SPECIES_LEN;
            int idx = 0;

            if (skipToNewline) {
                while (this.readBuffer[idx] != NEWLINE_BYTE) {
                    idx++;
                }
                idx++;
            }

            this.firstLineStart = idx;
            int lineStart = idx;

            while (idx < upperBound) {
                var byteVector = ByteVector.fromArray(BYTE_SPECIES, readBuffer, idx);
                var newlineIdx = byteVector.eq(NEWLINE_BYTE).firstTrue();
                if (newlineIdx < BYTE_SPECIES_LEN) {
                    var semicolonByteMask = byteVector.eq(SEMICOLON_BYTE);
                    var semicolonIdx = semicolonByteMask.firstTrue();

                    int semicolonOffset = idx + semicolonIdx;
                    int lineEnd = idx + newlineIdx;
                    short negative = (short) ((readBuffer[idx + semicolonIdx + 1] == NEG_BYTE) ? 1 : 0);
                    int numLength = (byte) ((lineEnd - (semicolonOffset + 1)));
                    var num = parseNumFromLine(this.readBuffer, semicolonOffset + 1 + negative, numLength - negative);
                    num = negative == 1 ? (short) -num : num;
                    int nameLen = semicolonOffset - lineStart;
                    summaryTable.putTemperatureValue(this.readBuffer, lineStart, nameLen, num);
                    idx = lineEnd + 1;
                    lastLineEnd = idx;
                    lineStart = idx;
                }
                else {
                    // TODO This is mostly just out of laziness so that we process a full line on every step
                    idx += BYTE_SPECIES_LEN - 7; // Max length of semicolon, negative temp, and newline is 7
                }
            }

            int semicolonIdx = -1;
            while (idx < byteCount) {
                if (readBuffer[idx] == SEMICOLON_BYTE) {
                    semicolonIdx = idx;
                }
                if (readBuffer[idx] == NEWLINE_BYTE) {
                    int lineEnd = idx;

                    short negative = (short) ((readBuffer[semicolonIdx + 1] == NEG_BYTE) ? 1 : 0);
                    int numLength = (byte) ((lineEnd - (semicolonIdx + 1)));
                    var num = parseNumFromLine(this.readBuffer, semicolonIdx + 1 + negative, numLength - negative);
                    num = negative == 1 ? (short) -num : num;
                    int nameLen = semicolonIdx - lineStart;
                    summaryTable.putTemperatureValue(this.readBuffer, lineStart, nameLen, num);
                    idx = lineEnd + 1;
                    lastLineEnd = idx;
                    lineStart = idx;
                }
                idx++;
            }
        }
    }
}
