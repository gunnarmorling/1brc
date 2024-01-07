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
import java.util.Locale;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
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

    /**
     * State with the full summary table, as well as leftover bytes between processed blocks that need to
     * be handled afterward.
     */
    record State(SummaryTable summaryTable, byte[] remainderBytes) {
    }

    public static void main(String[] args) throws IOException {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numReadBuffers = numCores + 2;

        var blockBuilderQueue = new LinkedBlockingDeque<BlockBuilder>(numReadBuffers);
        for (int i = 0; i < numReadBuffers; i++) {
            blockBuilderQueue.add(new BlockBuilder(BLOCK_READ_SIZE));
        }
        try (var fjp = new ForkJoinPool(numCores)) {

            CompletableFuture<State> stateFuture = CompletableFuture.completedFuture(new State(new SummaryTable(SUMMARY_TABLE_SIZE), new byte[0]));

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
                                var summaryTable = localBlockBuilder.buildSummaryTable(localCnt, localSkipToNewline);

                                int unprocessedRemainderSize = localBlockBuilder.firstLineStart + (localCnt - localBlockBuilder.lastLineEnd);
                                var unprocessedBytes = new byte[unprocessedRemainderSize];
                                System.arraycopy(localBlockBuilder.readBuffer, 0, unprocessedBytes, 0, localBlockBuilder.firstLineStart);
                                System.arraycopy(localBlockBuilder.readBuffer, localBlockBuilder.lastLineEnd, unprocessedBytes, localBlockBuilder.firstLineStart,
                                        (localCnt - localBlockBuilder.lastLineEnd));

                                localBlockBuilder.reset();
                                blockBuilderQueue.add(localBlockBuilder);
                                return new State(summaryTable, unprocessedBytes);
                            }, fjp), (state, newState) -> {
                                state.summaryTable.addAll(newState.summaryTable);

                                var newRemainderBytes = new byte[state.remainderBytes.length + newState.remainderBytes.length];
                                System.arraycopy(state.remainderBytes, 0, newRemainderBytes, 0, state.remainderBytes.length);
                                System.arraycopy(newState.remainderBytes, 0, newRemainderBytes, state.remainderBytes.length, newState.remainderBytes.length);
                                return new State(state.summaryTable, newRemainderBytes);
                            });

                    try {
                        blockBuilder = blockBuilderQueue.poll(1, TimeUnit.HOURS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

            }

            stateFuture = stateFuture.thenApply(state -> {
                BlockBuilder blockBuilder = null;
                try {
                    blockBuilder = blockBuilderQueue.poll(1, TimeUnit.HOURS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                System.arraycopy(state.remainderBytes, 0, blockBuilder.readBuffer, 0, state.remainderBytes.length);
                state.summaryTable.addAll(blockBuilder.buildSummaryTable(state.remainderBytes.length, false));
                return new State(state.summaryTable, new byte[0]);
            });

            var state = stateFuture.join();
            System.out.println(state.summaryTable.toOutputString());
        }
    }

    /**
     * Parses number values as integers from the byte array.
     * <p>
     * The multiplier is 1 if positive and -1 if negative.
     */
    static short parseNumFromLine(byte[] buf, int offset, int len, int multiplier) {
        return switch (len) {
            case 3 -> (short) ((((buf[offset] - '0') * 10) + buf[offset + 2] - '0') * multiplier);
            case 4 -> (short) ((((buf[offset] - '0') * 100) + (buf[offset + 1] - '0') * 10 + buf[offset + 3] - '0') * multiplier);
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

        void add(CitySummary other) {
            this.max = Math.max(other.max, this.max);
            this.min = Math.min(other.min, this.min);
            this.sum += other.sum;
            this.count += other.count;
        }

    }

    /**
     * A wrapper around a city name.
     * <p>
     * Provides a view of a large read buffer, but can also be cloned and detached from the read buffer.
     */
    static final class ByteSlice {

        private final byte[] buf;
        private int offset;
        private int len;
        private int hash;

        public ByteSlice(byte[] buf) {
            this.buf = buf;
        }

        public static ByteSlice clone(ByteSlice src) {
            var bytes = new byte[src.len];
            System.arraycopy(src.buf, src.offset, bytes, 0, src.len);
            var copy = new ByteSlice(bytes);
            copy.offset = 0;
            copy.len = src.len;
            copy.hash = src.hash;
            return copy;
        }

        @Override
        public String toString() {
            return "ByteSlice[%s]".formatted(new String(this.buf, this.offset, this.len, StandardCharsets.UTF_8));
        }

        public String valueAsString() {
            return new String(this.buf, this.offset, this.len, StandardCharsets.UTF_8);
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        public int calculateHashCode() {
            int result = 1;
            int end = offset + len;
            for (int i = offset; i < end; i++) {
                result = 31 * result + buf[i];
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
            return a.hash == b.hash && Arrays.equals(a.buf, a.offset, a.offset + a.len, b.buf, b.offset, b.offset + b.len);
        }

    }

    static final class SummaryTable {

        private int size;
        private ByteSlice[] keys;
        private CitySummary[] values;
        private int valueCount;

        SummaryTable(int size) {
            this.size = size;
            this.keys = new ByteSlice[size];
            this.values = new CitySummary[size];
        }

        public void addAll(SummaryTable other) {
            for (int i = 0; i < other.size; i++) {
                var otherSlice = other.keys[i];
                if (otherSlice != null) {
                    putCitySummary(otherSlice, other.values[i]);
                }
            }
        }

        private void putCitySummary(ByteSlice key, CitySummary value) {
            resizeIfNecessary();
            int index = (key.hash & 0x7FFFFFFF) % size;
            while (keys[index] != null) {
                if (ByteSlice.equal(keys[index], key)) {
                    values[index].add(value);
                    return;
                }
                // TODO Consider secondary hash for the stride here
                index = (index + 1) % size;
            }
            keys[index] = key;
            values[index] = value;
            valueCount++;
        }

        public void putTemperatureValue(ByteSlice key, int value) {
            resizeIfNecessary();
            int index = (key.hash & 0x7FFFFFFF) % size;
            while (keys[index] != null) {
                if (ByteSlice.equal(keys[index], key)) {
                    values[index].add(value);
                    return;
                }
                // TODO Consider secondary hash for the stride here
                // System.out.println("Collision!");
                index = (index + 1) % size;
            }
            keys[index] = ByteSlice.clone(key);
            values[index] = new CitySummary(value);
            valueCount++;
        }

        private void resizeIfNecessary() {
            if (valueCount == size) {
                var resized = new SummaryTable(size * 2);
                resized.addAll(this);
                this.keys = resized.keys;
                this.values = resized.values;
                this.size = resized.size;
                this.valueCount = resized.valueCount;
            }
        }

        public String toOutputString() {
            SortedMap<String, CitySummary> m = new TreeMap<>();
            for (int i = 0; i < size; i++) {
                ByteSlice slice = keys[i];
                if (slice != null) {
                    m.put(slice.valueAsString(), values[i]);
                }
            }
            return "{" + m.entrySet().stream().map(e -> String.format(Locale.US, "%s=%.1f/%.1f/%.1f", e.getKey(), e.getValue().min / 10f,
                    (e.getValue().sum / (float) e.getValue().count) / 10f, e.getValue().max / 10f)).collect(Collectors.joining(", ")) + "}";
        }
    }

    /**
     * Performs actual building of a SummaryTable from a read buffer.
     */
    static class BlockBuilder {
        final byte[] readBuffer;
        private final int numSegmentLengths;
        private final byte[] segmentLengths;
        private final int[] hashCodes;
        private final short[] temperatureValues;
        private final ByteSlice byteSlice;

        private int firstLineStart;
        private int lastLineEnd;
        private int lineCount;

        public BlockBuilder(int readBufferSize) {
            this.readBuffer = new byte[readBufferSize];
            // TODO This sizing is almost certainly non-optimal, but it seems to work
            this.numSegmentLengths = readBufferSize / 2;
            this.segmentLengths = new byte[numSegmentLengths];
            this.hashCodes = new int[numSegmentLengths];
            this.temperatureValues = new short[numSegmentLengths];
            this.byteSlice = new ByteSlice(this.readBuffer);
        }

        void reset() {
            firstLineStart = -1;
            lastLineEnd = -1;
            lineCount = 0;
        }

        public SummaryTable buildSummaryTable(int readByteCount, boolean skipToNewline) {

            parseLineSegments(readByteCount, skipToNewline);
            calculateHashesAndTemperatures();

            SummaryTable summaryTable = new SummaryTable(SUMMARY_TABLE_SIZE);

            this.byteSlice.offset = this.firstLineStart;

            int segmentCount = lineCount * 2;
            int lineCounter = 0;
            for (int segmentIdx = 0; segmentIdx < segmentCount; segmentIdx += 2) {
                // TODO It would likely be better if this ByteSlice was just a view on the arrays
                this.byteSlice.len = this.segmentLengths[segmentIdx];
                this.byteSlice.hash = this.hashCodes[lineCounter];
                summaryTable.putTemperatureValue(this.byteSlice, this.temperatureValues[lineCounter]);
                this.byteSlice.offset += (this.segmentLengths[segmentIdx] + (this.segmentLengths[segmentIdx + 1] & 0b01111111) + 2);
                lineCounter++;
            }

            return summaryTable;

        }

        private void calculateHashesAndTemperatures() {
            this.byteSlice.offset = this.firstLineStart;

            int segmentCount = lineCount * 2;
            int lineCounter = 0;
            for (int segmentIdx = 0; segmentIdx < segmentCount; segmentIdx += 2) {
                // TODO It would likely be better if this ByteSlice was just a view on the arrays
                this.byteSlice.len = this.segmentLengths[segmentIdx];
                this.hashCodes[lineCounter++] = this.byteSlice.calculateHashCode();
                this.byteSlice.offset += (this.segmentLengths[segmentIdx] + (this.segmentLengths[segmentIdx + 1] & 0b01111111) + 2);
            }

            // TODO It might be better/faster to do this in the previous loop instead of second loop
            lineCounter = 0;
            int offset = this.firstLineStart + this.segmentLengths[0] + 1;
            for (int segmentIdx = 1; segmentIdx < segmentCount; segmentIdx += 2) {
                byte segmentLength = this.segmentLengths[segmentIdx];
                var isNeg = (byte) (segmentLength >> 7);
                int numLength = (segmentLength & 0b01111111) + isNeg;
                short temperatureValue = parseNumFromLine(this.readBuffer, offset - isNeg, numLength, (isNeg * 2) + 1);
                this.temperatureValues[lineCounter++] = temperatureValue;
                offset += -isNeg + numLength + this.segmentLengths[segmentIdx + 1] + 2;
            }
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
            this.lineCount = 0;
            int segmentCounter = 0;
            int lineStart = idx;

            while (idx < upperBound && lineCount < numSegmentLengths) {
                var byteVector = ByteVector.fromArray(BYTE_SPECIES, readBuffer, idx);
                var newlineByteMask = byteVector.eq(NEWLINE_BYTE);
                if (newlineByteMask.anyTrue()) {
                    var semicolonByteMask = byteVector.eq(SEMICOLON_BYTE);
                    var semicolonIdx = semicolonByteMask.firstTrue();
                    int semicolonOffset = idx + semicolonIdx;
                    int lineEnd = idx + newlineByteMask.firstTrue();
                    byte negMult = (byte) ((readBuffer[idx + semicolonIdx + 1] == NEG_BYTE) ? 0b10000000 : 0);
                    this.segmentLengths[segmentCounter++] = (byte) (semicolonOffset - lineStart);
                    this.segmentLengths[segmentCounter++] = (byte) ((lineEnd - (semicolonOffset + 1)) | negMult);
                    this.lineCount++;
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
                    byte negMult = (byte) ((readBuffer[semicolonIdx + 1] == NEG_BYTE) ? 0b10000000 : 0);
                    this.segmentLengths[segmentCounter++] = (byte) (semicolonIdx - lineStart);
                    this.segmentLengths[segmentCounter++] = (byte) ((lineEnd - (semicolonIdx + 1)) | negMult);
                    this.lineCount++;
                    idx = lineEnd + 1;
                    lastLineEnd = idx;
                    lineStart = idx;
                }
                idx++;
            }

        }

    }

}
