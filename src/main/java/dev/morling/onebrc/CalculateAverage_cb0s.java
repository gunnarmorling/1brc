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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_cb0s {

    private static final String FILE = "./measurements.txt";
    private static final int INPUT_BUFFER_SIZE = 1 << 16; // yields the best performance on my system...

    public static void main(String[] args) throws IOException, InterruptedException {
        run();
        // benchmark();
    }

    private static void benchmark() throws IOException {
        var startTime = System.currentTimeMillis();
        for (int count = 0; count < 3; ++count) {
            run();
        }
        var stopTime = System.currentTimeMillis();

        System.out.println(STR."Running 3 times took: \{stopTime - startTime}ms (1 run: \{(stopTime - startTime) / 3}ms)");
    }

    private static void run() throws IOException {
        var fileSize = getFileSize();

        // for consistency for smaller files (actually a mess, could be solved more elegantly in the parsing step)
        var processors = Runtime.getRuntime().availableProcessors();
        processors = Math.max(1, Math.min(processors, (int) fileSize / 106));
        while (fileSize / processors < INPUT_BUFFER_SIZE && processors > 1)
            --processors;

        var chunkSize = fileSize / processors;

        System.out.write('{');

        // for getting a bit more out of this solution, we don't check for null
        var mergedResults = IntStream.range(0, processors)
                .parallel()
                .mapToObj(i -> processChunk(i, chunkSize))
                .reduce(TempResultStorage::merge).get();

        var endResult = mergedResults.aggregatedResultsPreOrdered.stream()
                .map(Station::toString)
                .collect(Collectors.joining(", "));

        System.out.write(endResult.getBytes());

        System.out.write(new byte[]{ '}', '\n' });
    }

    private static class MeasurementAggregator {
        public MeasurementAggregator(int initialValue) {
            min = initialValue;
            max = initialValue;
            count = 1;
            sum = initialValue;
        }

        public int min, max, count;
        // we need to long if the possible absolute sum is greater than 2^31
        public long sum;
    }

    private record Station(
            MeasurementAggregator results,
            RawName rawName
    ) implements Comparable<Station> {

    @Override
    public boolean equals(Object otherObject) {
        if (otherObject instanceof Station otherStation) {
            return otherStation.rawName.equals(rawName);
        }
        return false;
    }

    @Override
    public int compareTo(Station otherStation) {
        return rawName.compareTo(otherStation.rawName);
    }

    @Override
    public String toString() {
        return STR."\{rawName}=\{results.min/10.0}/\{Math.round(results.sum / (float) results.count) / 10.0}/\{results.max/10.0}";
    }

    @Override
    public int hashCode() {
        return rawName.hashCode();
    }

    }

    private record RawName(
            byte[] rawName
    ) implements Comparable<RawName> {

    @Override
    public boolean equals(Object otherObject) {
        RawName otherRawName = (RawName) otherObject;
        return Arrays.equals(otherRawName.rawName, this.rawName);

        /*
         * Although being safer, comparing actually is a small bottleneck
         * if (otherObject instanceof RawName otherRawName) {
         * return Arrays.equals(otherRawName.rawName, this.rawName);
         * }
         * return false;
         */
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rawName);
    }

    @Override
    public String toString() {
        return new String(rawName, 0, rawName.length, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(RawName otherRawName) {
        int result = 0;
        // Math.min is SLIGHTLY less efficient, but we don't care at this point
        var lowerIndex = Math.min(rawName.length, otherRawName.rawName.length);
        for (int i = 0; i < lowerIndex && result == 0; ++i) {
            result = Byte.compareUnsigned(rawName[i], otherRawName.rawName[i]);
        }

        return result == 0 ? rawName.length - otherRawName.rawName.length : result;
    }
}

private static class TempResultStorage {
    public void insertMeasurement(byte[] dataRow, int from, int to) {
        // 1st parse measurement
        var sepIndex = from + 1;
        while (dataRow[sepIndex] != ';')
            ++sepIndex;

        var parsedMeasurement = parseMeasurement(dataRow, sepIndex + 1, to);

        // 2nd handle if city occurs the first time
        var rawName = new RawName(Arrays.copyOfRange(dataRow, from, sepIndex));
        var tempIndex = indexCache.get(rawName);
        if (tempIndex == null) {
            var aggregator = new MeasurementAggregator(parsedMeasurement);
            var tempStation = new Station(aggregator, rawName);
            aggregatedResults.add(tempStation);
            indexCache.put(rawName, aggregatedResults.size() - 1);
            aggregatedResultsPreOrdered.add(tempStation);
            return;
        }

        // or update already existing station
        var tempResults = aggregatedResults.get(tempIndex).results;
        // TODO: compare to: add simd vector storage and process once every 8 iterations

        tempResults.sum += parsedMeasurement;
        tempResults.count++;

        if (tempResults.max < parsedMeasurement) {
            tempResults.max = parsedMeasurement;
        }
        else if (tempResults.min > parsedMeasurement) {
            tempResults.min = parsedMeasurement;
        }
    }

    public TempResultStorage() {
        aggregatedResults = new ArrayList<>(INITIAL_RESULT_SIZE);
        indexCache = new HashMap<>(INITIAL_RESULT_SIZE);
        aggregatedResultsPreOrdered = new TreeSet<>();
    }

    public static TempResultStorage merge(TempResultStorage storage0, TempResultStorage storage1) {
        // default case
        if (storage0 == null) {
            return storage1;
        }

        // TODO: Implementation with SIMD commands
        for (var station1 : storage1.aggregatedResults) {
            // System.out.println(station1.results.count + " " + station1.results.sum);
            var key = storage0.indexCache.get(station1.rawName);
            if (key == null) {
                storage0.aggregatedResults.add(station1);
                storage0.indexCache.put(station1.rawName, storage0.aggregatedResults.size() - 1);
                storage0.aggregatedResultsPreOrdered.add(station1);
                continue;
            }

            var station0 = storage0.aggregatedResults.get(key);
            station0.results.count += station1.results.count;
            station0.results.sum += station1.results.sum;

            if (station0.results.min > station1.results.min) {
                station0.results.min = station1.results.min;
            }

            if (station1.results.max > station0.results.max) {
                station0.results.max = station1.results.max;
            }
        }

        return storage0;
    }

    // the closer it is to the actual value the better -> for 10_000 stations 10_000 is obviously better
    private static final int INITIAL_RESULT_SIZE = 420;

    // we use a custom name mapping for faster access to aggregatedResults and easier sorting
    private final List<Station> aggregatedResults;
    private final TreeSet<Station> aggregatedResultsPreOrdered;
    private final HashMap<RawName, Integer> indexCache;

    /**
     * Parses a char[] array to the contained number in a fixed point format.
     * The number can be between [-99.9, 99.9] (i.e. has either 2 or 3 digits and might contain a sign)
     * and represents a temperature measurement.
     * Note that no checking takes place. Incorrect formats yield unexpected results.
     *
     * @param dataRow char array actually containing the number
     * @param from    the start index of the number inside the array (included)
     * @param to      the end index of the number (not included, i.e. the char after the number or the length)
     * @return fixed point (int) representation of the contained measurement
     */
    private int parseMeasurement(byte[] dataRow, int from, int to) {
        // almost branch-less solution
        int sign = -1 + 2 * ((dataRow[from] >> 4) & 1);

        int floatingPoint = dataRow[to - 1] - 48;
        int lastIntDigit = dataRow[to - 3] - 48;
        int firstIntDigit = to - from - 4 >= 0 ? (sign + 1) / 2 * dataRow[to - 4] - 48 : 0;

        if (to - from >= 4) {
            firstIntDigit = dataRow[to - 4] - 48;

            if (to - from == 4 && sign == -1) {
                firstIntDigit = 0;
            }
        }

        return (firstIntDigit * 100 + lastIntDigit * 10 + floatingPoint) * sign;
    }

    }

    private static TempResultStorage processChunk(int i, long chunkSize) {
        var storage = new TempResultStorage();
        var readBuffer = new byte[INPUT_BUFFER_SIZE];

        try (var inputStream = new BufferedInputStream(new FileInputStream(FILE), INPUT_BUFFER_SIZE)) {
            var readBytes = 0L; // we set it to one because our first loop will not register last read byte
            var readBytesDelta = 0;

            // preparation
            if (i != 0) {
                --readBytes;
                inputStream.skip(i * chunkSize - 1);
                int c;
                while ((c = inputStream.read()) != '\n' && c != -1)
                    ++readBytes;
            }

            // actual parsing
            // worst case: only \n is missing for a whole line
            var carryOver = new byte[107];
            var carryOverSize = 0;

            while (readBytes < chunkSize && inputStream.available() > 0) {
                readBytes += (readBytesDelta = inputStream.read(readBuffer, 0, readBuffer.length));
                int from = 0, to = 0;

                if (carryOverSize != 0) {
                    while (readBuffer[to] != '\n')
                        ++to;
                    System.arraycopy(readBuffer, from, carryOver, carryOverSize, to - from + 1);

                    storage.insertMeasurement(carryOver, 0, carryOverSize + to - from);
                    from = ++to;
                }

                // Actually looking 5 ahead instead of 1 at each new line
                // Minimal line consists of: [name-byte];[first_digit].[last_digit]\n
                while (to <= readBytesDelta && (readBytes - readBytesDelta + to) < chunkSize) {
                    to += 5;

                    while (to < readBytesDelta && readBuffer[to] != '\n')
                        ++to;

                    if (to >= readBytesDelta) {
                        System.arraycopy(readBuffer, from, carryOver, 0, readBytesDelta - from);
                        carryOverSize = readBytesDelta - from;
                        break;
                    }

                    storage.insertMeasurement(readBuffer, from, to);
                    from = ++to;
                }
            }
        }
        catch (IOException e) {
            return null; // shouldn't happen
        }

        return storage;
    }

    private static long getFileSize() {
        return new File(CalculateAverage_cb0s.FILE).length();
    }
}
