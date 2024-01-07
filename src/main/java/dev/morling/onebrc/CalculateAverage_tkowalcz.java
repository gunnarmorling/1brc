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

import jdk.incubator.vector.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * This solution has two (conscious) assumptions about the input data:
 * <ol>
 * <li>The measurements can be numbers in one of four forms: -XX.X, XX.X, -X.X, X.X</li>
 * <li>The city name fits into vector register (has at most 32 bytes). This is a soft requirement: I check for this
 * condition and will execute fallback if that is not the case, but I did not code the fallback.
 * </li>
 * </ol>
 * <p>
 * For top speed we <b>hope</b> that the "hash" function has no collisions. If that is not the case we hit a very slow
 * fallback path that ensures correctness.
 * <p>
 *  I would prefer to split this class but don't want to pollute source tree and vectorisation breaks when split into methods.
 */
public class CalculateAverage_tkowalcz {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');

    // Used to identify positions where vector containing temperature measurement has '-', '.' and '\n' characters.
    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
    // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
    // There are four combinations of possible mask results from comparing (less than) vector containing temperature
    // measurement with ASCII_ZERO. Hence, only four entries are populated.
    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = { 0, 0, 0, 0, 5, 5, 0, 0, 0, 6, 4 };

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = 0x400000 - 1;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        try (Arena arena = Arena.ofShared()) {
            int availableProcessors = Runtime.getRuntime().availableProcessors();

            MemorySegment inputData = mmapDataFile(FILE, arena);
            List<MemorySegment> memorySegments = divideAlongNewlines(inputData, availableProcessors);

            CompletionService<List<StatisticsAggregate>> completionService = new ExecutorCompletionService<>(
                    Executors.newFixedThreadPool(
                            availableProcessors,
                            new DaemonThreadFactory()));
            memorySegments.forEach(slice -> completionService.submit(() -> execute(slice)));

            TreeMap<String, StatisticsAggregate> results = new TreeMap<>();
            for (int i = 0; i < memorySegments.size(); i++) {
                List<StatisticsAggregate> result = completionService.take().get();
                for (StatisticsAggregate statisticsAggregate : result) {
                    StatisticsAggregate node = statisticsAggregate;
                    do {
                        results.merge(node.cityAsString(), node, StatisticsAggregate::merge);
                        node = node.getNext();
                    } while (node != null);
                }
            }

            System.out.println(results);
        }
    }

    static List<MemorySegment> divideAlongNewlines(MemorySegment inputData, int numberOfParts) {
        List<MemorySegment> result = new ArrayList<>();

        long startingOffset = 0;
        long sliceSize = inputData.byteSize() / numberOfParts;
        do {
            long endingOffset = findPastNewline(inputData, startingOffset + sliceSize);
            result.add(inputData.asSlice(startingOffset, endingOffset - startingOffset));

            startingOffset = endingOffset;
        } while (startingOffset < inputData.byteSize() - sliceSize);

        result.add(inputData.asSlice(startingOffset, inputData.byteSize() - startingOffset));
        return result;
    }

    public static List<StatisticsAggregate> execute(MemorySegment inputData) {
        StatisticsAggregate[] dataTable = new StatisticsAggregate[TABLE_SIZE];
        long dataSize = inputData.byteSize();

        long offset1 = 0;
        long offset2 = findPastNewline(inputData, dataSize / 2);

        long end1 = offset2;
        long end2 = dataSize;

        DoubleCursor doubleCursor = executeDoublePumped(inputData, dataTable, offset1, offset2, end1, end2);
        offset1 = executeSinglePumped(inputData, dataTable, doubleCursor.offset1(), end1);
        offset2 = executeSinglePumped(inputData, dataTable, doubleCursor.offset2(), end2);
        Map<String, StatisticsAggregate> tailResults1 = executeScalar(inputData, offset1, end1);
        Map<String, StatisticsAggregate> tailResults2 = executeScalar(inputData, offset2, end2);

        List<StatisticsAggregate> result = filterEmptyEntries(dataTable);
        result.addAll(tailResults1.values());
        result.addAll(tailResults2.values());

        return result;
    }

    // I'm really tired at this point
    static Map<String, StatisticsAggregate> executeScalar(MemorySegment inputData, long offset, long end) {
        // Why getting byte data from a memory segment is so hard?
        byte[] inputDataArray = new byte[(int) (end - offset)];
        for (int i = 0; i < inputDataArray.length; i++) {
            inputDataArray[i] = inputData.get(ValueLayout.JAVA_BYTE, offset + i);
        }

        String inputString = new String(inputDataArray, StandardCharsets.UTF_8);

        String[] lines = inputString.split("\n");
        return stream(lines)
                .map(line -> {
                    String[] cityAndTemperature = line.split(";");

                    byte[] cityBytes = cityAndTemperature[0].getBytes(StandardCharsets.UTF_8);
                    StatisticsAggregate aggregate = new StatisticsAggregate(cityBytes, cityBytes.length);

                    long temperature = (long) Float.parseFloat(cityAndTemperature[1]) * 10;
                    return aggregate.accept(temperature);
                })
                .collect(Collectors.toMap(StatisticsAggregate::cityAsString, Function.identity(), StatisticsAggregate::merge));
    }

    public static DoubleCursor executeDoublePumped(MemorySegment inputData, StatisticsAggregate[] dataTable, long offset1, long end1, long offset2, long end2) {
        end1 -= SPECIES.length();
        end2 -= SPECIES.length();

        while (offset1 < end1 && offset2 < end2) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset1 += firstDelimiter1 + 1;

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            int firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset2 += firstDelimiter2 + 1;

            if (firstDelimiter1 == 32) {
                // Slow path - need to reimplement all logic in a scalar way, but it does not have to exact -
                // this string will always hit this path
                System.out.println(STR."Unsupported city name exceeding \{SPECIES.length()} bytes. Starts with \{toString(byteVector1)}");
            }

            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);

            int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;

            StatisticsAggregate statisticsAggregate_1 = dataTable[index1];
            if (statisticsAggregate_1 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput1.reinterpretAsBytes().intoArray(city, 0, hashMask1);

                statisticsAggregate_1 = new StatisticsAggregate(city, hashMask1.trueCount());
                dataTable[index1] = statisticsAggregate_1;
            } else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_1.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                    // Very slow path: linked list of collisions
                    statisticsAggregate_1 = findCityInChain(statisticsAggregate_1, hashInput1, hashMask1);
                }
            }

            if (firstDelimiter2 == 32) {
                // Slow path - need to reimplement all logic in a scalar way, but it does not have to exact -
                // this string will always hit this path
                System.out.println(STR."Unsupported city name exceeding \{SPECIES.length()} bytes. Starts with \{toString(byteVector2)}");
            }

            VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[firstDelimiter2];
            Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);

            int perfectHash32_2 = hashInput2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index2 = perfectHash32_2 & TABLE_SIZE_MASK;

            StatisticsAggregate statisticsAggregate_2 = dataTable[index2];
            if (statisticsAggregate_2 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput2.reinterpretAsBytes().intoArray(city, 0, hashMask2);

                statisticsAggregate_2 = new StatisticsAggregate(city, hashMask2.trueCount());
                dataTable[index2] = statisticsAggregate_2;
            } else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_2.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput2).allTrue()) {
                    // Very slow path: linked list of collisions
                    statisticsAggregate_2 = findCityInChain(statisticsAggregate_2, hashInput2, hashMask2);
                }
            }

            byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) (mask1.toLong() & 0x0F);

            long value = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_1.accept(value);
            offset1 += STOI_SIZE_LOOKUP[lookupIndex1];

            byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex2 = (int) (mask2.toLong() & 0x0F);

            value = byteVector2
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex2])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_2.accept(value);
            offset2 += STOI_SIZE_LOOKUP[lookupIndex2];
        }

        return new DoubleCursor(offset1, offset2);
    }

    public record DoubleCursor(long offset1, long offset2) {
    }

    public static long executeSinglePumped(MemorySegment inputData, StatisticsAggregate[] dataTable, long offset1, long end1) {
        end1 -= SPECIES.length();
        while (offset1 < end1) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            offset1 += firstDelimiter1 + 1;

            if (firstDelimiter1 == 32) {
                // Slow path - need to reimplement all logic in a scalar way, but it does not have to exact -
                // this string will always hit this path
                System.out.println(STR."Unsupported city name exceeding \{SPECIES.length()} bytes. Starts with \{toString(byteVector1)}");
            }

            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);

            int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;

            StatisticsAggregate statisticsAggregate_1 = dataTable[index1];
            if (statisticsAggregate_1 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput1.reinterpretAsBytes().intoArray(city, 0, hashMask1);

                statisticsAggregate_1 = new StatisticsAggregate(city, hashMask1.trueCount());
                dataTable[index1] = statisticsAggregate_1;
            } else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_1.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                    // Very slow path: linked list of collisions
                    statisticsAggregate_1 = findCityInChain(statisticsAggregate_1, hashInput1, hashMask1);
                }
            }

            byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) (mask1.toLong() & 0x0F);

            long value = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

            statisticsAggregate_1.accept(value);
            offset1 += STOI_SIZE_LOOKUP[lookupIndex1];
        }

        return offset1;
    }

    private static List<StatisticsAggregate> filterEmptyEntries(StatisticsAggregate[] dataTable) {
        List<StatisticsAggregate> result = new ArrayList<>();
        for (StatisticsAggregate statisticsAggregate : dataTable) {
            if (statisticsAggregate != null) {
                result.add(statisticsAggregate);
            }
        }

        return result;
    }

    static StatisticsAggregate findCityInChain(StatisticsAggregate startingNode, Vector<Byte> hashInput, VectorMask<Byte> hashMask) {
        StatisticsAggregate node = startingNode.getNext();
        while (node != null) {
            ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, node.getCity(), 0);
            if (!cityVector.compare(VectorOperators.EQ, hashInput).allTrue()) {
                node = node.getNext();
            }
            else {
                return node;
            }
        }

        byte[] city = new byte[SPECIES.length()];
        hashInput.reinterpretAsBytes().intoArray(city, 0, hashMask);
        return startingNode.attachLast(new StatisticsAggregate(city, hashMask.trueCount()));
    }

    private static long findPastNewline(MemorySegment inputData, long position) {
        while (inputData.get(ValueLayout.JAVA_BYTE, position) != '\n') {
            position++;

            if (position == inputData.byteSize()) {
                return position;
            }
        }

        return position + 1;
    }

    private static String toString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        return new String(array, StandardCharsets.UTF_8);
    }

    private static MemorySegment mmapDataFile(String fileName, Arena arena) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[33];
        result[0] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 33; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    static class DaemonThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread result = new Thread(r);
            result.setDaemon(true);
            return result;
        }
    }

    public static class StatisticsAggregate {

        private final byte[] city;
        private final int cityLength;

        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;

        private long sum;
        private int count;

        private StatisticsAggregate next;

        public StatisticsAggregate(byte[] city, int cityLength) {
            this.city = city;
            this.cityLength = cityLength;
        }

        public byte[] getCity() {
            return city;
        }

        public String cityAsString() {
            return new String(city, 0, cityLength, StandardCharsets.UTF_8);
        }

        public StatisticsAggregate accept(long value) {
            min = Math.min(min, value);
            max = Math.max(max, value);

            sum += value;
            count++;

            return this;
        }

        public StatisticsAggregate attachLast(StatisticsAggregate statisticsAggregate) {
            if (next != null) {
                next.attachLast(statisticsAggregate);
            }
            else {
                next = statisticsAggregate;
            }

            return statisticsAggregate;
        }

        public StatisticsAggregate getNext() {
            return next;
        }

        public static StatisticsAggregate merge(StatisticsAggregate one, StatisticsAggregate other) {
            StatisticsAggregate result = new StatisticsAggregate(one.city, one.cityLength);
            result.min = Math.min(one.min, other.min);
            result.max = Math.max(one.max, other.max);

            result.sum = one.sum + other.sum;
            result.count = one.count + other.count;

            return result;
        }

        @Override
        public String toString() {
            float average = (sum / 10.0f) / count;
            float actualMin = min / 10.0f;
            float actualMax = max / 10.0f;

            return String.format("%.1f/%.1f/%.1f", actualMin, average, actualMax);
        }
    }
}
