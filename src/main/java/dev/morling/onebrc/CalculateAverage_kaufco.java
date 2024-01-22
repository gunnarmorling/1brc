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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** *     *                *    *     *
 *     *          *               *
 *  *      ThE BiT bAnG tHeOrY        *
 *      *      *            *
 *  *            *                    */
public class CalculateAverage_kaufco {

    /**
     * Input file.
     */
    private static final String FILE = "./measurements.txt";

    /**
     * Number of worker threads to use
     */
    private static final int THREAD_COUNT = 64;

    /**
     * Maximum number of keys (station names) supported.
     */
    private static final int MAX_KEY_COUNT = 10000;

    /**
     * Maximum key (station name) length supported.
     */
    private static final int MAX_KEY_SIZE = 100;

    /**
     * Chunk size in bytes in which to process the input file.
     */
    private static final int CHUNK_SIZE = 0x8000000; // 128MB

    /**
     * Number of bits used to form an index into the weather data hash table.
     * These least significant bits from the hash code for station name.
     */
    private static final int WEATHER_DATA_INDEX_BITS = 19;

    /**
     * Maximum line length in bytes.
     */
    private static final int MAX_LINE_LENGTH = MAX_KEY_SIZE + ";-99.9\n".length();

    /**
     * Number of data rows in the weather data hash table.
     */
    private static final int WEATHER_DATA_ROWS = 1 << WEATHER_DATA_INDEX_BITS;

    /**
     * Mask used to form an index into the weather data hash table.
     */
    private static final int WEATHER_DATA_INDEX_MASK = WEATHER_DATA_ROWS - 1;

    /**
     * Size of a row in the weather data hash table, in bytes.
     */
    private static final int WEATHER_DATA_ROW_SIZE = 16;

    /**
     * Pattern to scan fast for the `'\n' ` character.
     */
    private static final long PATTERN_NL = constructPattern((byte) '\n');

    /**
     * Pattern to scan fast for the `;' ` character.
     */
    private static final long PATTERN_SEPARATOR = constructPattern((byte) ';');

    /**
     * Thread specific aggregation results.
     */
    private static final ThreadLocal<AggregationResult> threadResults = new ThreadLocal<>();

    /**
     * Postprocessing of lines that cross chunk borders.
     */
    private static final TreeMap<Long, ChunkPostprocess> postProcess = new TreeMap<>();

    /**
     * Collected aggregation results from all threads.
     */
    private static final List<AggregationResult> resultQueue = new LinkedList<>();

    private static final PrintStream out = System.out;

    /**
     * Returns the pattern for a byte to scan for in the using the {@link #scanFast(long, long)}
     * function. The pattern consists of the negated byte, padded to 64 bit.
     *
     * @param b byte value to scan for in {@link #constructPattern(byte)}
     * @return
     */
    private static long constructPattern(byte b) {
        long pattern = b;
        pattern = pattern | (pattern << 8);
        pattern = pattern | (pattern << 16);
        pattern = pattern | (pattern << 32);
        return ~pattern;
    }

    /**
     * Returns the index of the first byte in `stringChunk` that matches the byte represented by the given pattern.
     * Returns -1 if the byte is not present in `stringChunk`.
     * This is a very fast method to scan for the occurrence of a character in a string, scanning 8 bytes at once.
     * The implementation principle is that the byte we look for becomes `0xff` in the expression, and then
     * we determine the index of that marker byte.
     * <br>
     * Note that this implementation also works for UTF-8 strings, but can be used only to scan for characters
     * that can be represented in 8 bit (i.e., all ASCII characters).
     *
     * @param stringChunk an 8 byte chunk from a string
     * @param pattern 64 bit pattern representing the byte to search for (see {@link #constructPattern(byte)})
     * @return the index of the first occurrence of the byte, or -1
     */
    private static int scanFast(long stringChunk, long pattern) {
        var bits = stringChunk ^ pattern;
        bits = bits & (bits << 1);
        bits = bits & (bits << 2);
        bits = bits & (bits << 4) & 0x8080808080808080L;

        // Note: the original implementation was: `return 7 - (Long.numberOfLeadingZeros(l) >>> 3);`
        // This, however, did find the last occurrence of the byte in the 64 bit word in case there are many.
        // This can happen because the minimum line size is < 8 bytes.
        return 7 - ((63 - Long.numberOfTrailingZeros(bits)) >>> 3);
    }

    /**
     * Updates a hash code by adding the next chunk of a string.
     *
     * @param stringChunk an 8 byte chunk from a string
     * @param hash current hash
     * @return updated hash
     */
    private static int updateHash(long stringChunk, int hash) {
        return hash * 31 + ((int) (stringChunk >>> 32)) * 57 + (int) stringChunk;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            // Using thomaswue's method to spawn a subprocess and return this process as soon as the output is finished.
            // Replacing my old method of doing that in the calculate_average shell script.
            spawnWorker();
            return;
        }

        var inputFile = new RandomAccessFile(FILE, "r");
        var len = inputFile.length();

        var queue = new ArrayList<Runnable>();
        var chunkStart = 0l;
        var remainBytes = len;
        while (remainBytes > 0) {
            long chunkSize = min(CHUNK_SIZE, remainBytes);
            remainBytes -= chunkSize;
            queue.add(new ChunkRunner(inputFile, chunkStart, (int) chunkSize));
            chunkStart += chunkSize;
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        for (var task : queue) {
            executor.execute(task);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            // Do nothing
        }

        postProcess();
        printResults();
        System.out.close();
    }

    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder()
                .command(workerCommand)
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    private static void processChunk(
                                     long chunkStart,
                                     MappedByteBuffer buf,
                                     int chunkEnd) {
        var results = threadResults.get();
        if (results == null) {
            results = new AggregationResult();
            threadResults.set(results);
        }

        var chunkPostProcess = parseChunkLines(chunkStart, chunkEnd, buf, results.weatherData, results.stationNames);
        synchronized (postProcess) {
            postProcess.put(chunkPostProcess.chunkStart, chunkPostProcess);
        }

        synchronized (resultQueue) {
            resultQueue.add(results);
        }
    }

    private static ChunkPostprocess parseChunkLines(
                                                    long chunkStart,
                                                    int chunkSize,
                                                    ByteBuffer buf,
                                                    ByteBuffer weatherData,
                                                    ByteBuffer stationNames) {

        int chunkEnd = chunkSize - MAX_LINE_LENGTH;
        if (chunkEnd <= 0) {
            var bytes = new byte[chunkSize];
            buf.get(0, bytes);
            return new ChunkPostprocess(chunkStart, bytes, null);
        }
        int startOfs = skipUntilFirstNewline(buf);
        if (startOfs >= chunkEnd) {
            var bytes = new byte[chunkSize];
            buf.get(0, bytes);
            return new ChunkPostprocess(chunkStart, bytes, null);
        }

        var prefixBytes = new byte[startOfs];
        buf.get(0, prefixBytes);
        chunkEnd = parseLines(buf, startOfs, chunkEnd, weatherData, stationNames);
        var postfixBytes = new byte[chunkSize - chunkEnd];
        buf.get(chunkEnd, postfixBytes);
        return new ChunkPostprocess(chunkStart, prefixBytes, postfixBytes);
    }

    private static int skipUntilFirstNewline(ByteBuffer buf) {
        int startOfs = 0;
        while (true) {
            final int len = scanFast(buf.getLong(startOfs), PATTERN_NL);
            if (len >= 0) {
                return startOfs + len + 1;
            }
            startOfs += 8;
        }
    }

    private static int parseLines(
                                  ByteBuffer buf,
                                  int startOfs,
                                  int chunkEnd,
                                  ByteBuffer weatherData,
                                  ByteBuffer stationNames) {
        int hash = 0;
        int nextOfs = startOfs;

        while (nextOfs < chunkEnd) {
            var lastWord = buf.getLong(nextOfs);
            int len = scanFast(lastWord, PATTERN_SEPARATOR);

            if (len >= 0) {
                nextOfs = parseSample(buf, startOfs, nextOfs + len, weatherData, stationNames, hash, lastWord);
                hash = 0;
                startOfs = nextOfs;
            }
            else {
                hash = updateHash(lastWord, hash);
                nextOfs += 8;
            }
        }
        return startOfs;
    }

    private static int parseSample(
                                   ByteBuffer buf,
                                   int startOfs,
                                   int endOfs,
                                   ByteBuffer weatherData,
                                   ByteBuffer stationNames,
                                   int hash,
                                   long stringChunk) {

        // Mask characters from stringChunk that do not belong to station name anymore. Only those must go into the hash code.
        stringChunk &= ((1L << (((endOfs - startOfs) & 7) << 3)) - 1L);
        hash = updateHash(stringChunk, hash);

        // Get temperature. Read 4 chars at once. Possible cases:
        // - A) "9.9\n" -> positive; digits in tempChars bytes #0, #2; next line at endOfs + 5
        // - B) "99.9" "\n" -> positive; digits in tempChars bytes #0, #1, #3; next line at endOfs + 6
        // - C) "-9.9" "\n" -> negative; digits in tempChars bytes #1, #3; next line at endOfs + 6
        // - D) "-99." "9\n" -> negative; digits in tempChars bytes #1, #2 + fetch next needed; next line at endOfs + 7

        int tempChars = buf.getInt(endOfs + 1);
        int temperature;

        int nameEnd = endOfs;
        boolean isNeg = (tempChars & 0xff) == 0x2d;
        if ((tempChars & 0xff0000) == 0x2e0000) {
            if (isNeg) {
                // C
                temperature = 1000 - ((tempChars >> 8) & 0xf) * 10 - ((tempChars >> 24) & 0xf);
                endOfs += 6;
            }
            else {
                // B
                temperature = 1000 + (tempChars & 0xf) * 100 + ((tempChars >> 8) & 0xf) * 10 + ((tempChars >> 24) & 0xf);
                endOfs += 6;
            }
        }
        else {
            if (isNeg) {
                // D
                temperature = 1000 - ((tempChars >> 8) & 0xf) * 100 - ((tempChars >> 16) & 0xf) * 10 - (buf.get(endOfs + 5) & 0xf);
                endOfs += 7;
            }
            else {
                // A
                temperature = 1000 + (tempChars & 0xf) * 10 + ((tempChars >> 16) & 0xf);
                endOfs += 5;
            }
        }

        recordSample(buf, startOfs, nameEnd, weatherData, stationNames, hash, temperature);
        return endOfs;
    }

    /**
     * Records a sample for a weather station.
     *
     * @param buf input file buffer
     * @param nameStart name start byte offset in the buffer
     * @param nameEnd name end byte offset in the buffer
     * @param weatherData weather data hash table (see {@link AggregationResult#weatherData} for detailed format).
     * @param stationNames buffer of weather station names (see {@link AggregationResult#stationNames} for detailed format).
     * @param hash hash code for station name
     * @param temperature temperature for the current sample
     */
    private static void recordSample(
                                     ByteBuffer buf,
                                     int nameStart,
                                     int nameEnd,
                                     ByteBuffer weatherData,
                                     ByteBuffer stationNames,
                                     int hash,
                                     int temperature) {
        int ofs = (hash & WEATHER_DATA_INDEX_MASK) << 4;
        long lo = weatherData.getLong(ofs);
        long hi = weatherData.getLong(ofs + 8);

        int prevHash = (int) (lo >> 32);
        if ((prevHash != hash) && (prevHash != 0)) {
            aggregateCollision(buf, nameStart, nameEnd, hash, temperature);
            return;
        }
        lo = (((long) hash) << 32) | ((lo + 1) & 0xffffffffL);

        // Note: for tempMin, we use negated max instead of min, se we can use 0 as the neutral element
        long tempMin = max(1999 - temperature, hi & 0x7ff);
        long tempMax = max(temperature, (hi >> 11) & 0x7ff);
        long tempSum = (hi & 0xffffffffffc00000L) + (((long) temperature) << 22);
        hi = tempSum | (tempMax << 11) | tempMin;

        weatherData.putLong(ofs, lo);
        weatherData.putLong(ofs + 8, hi);

        if (prevHash == 0) {
            recordStationName(buf, nameStart, nameEnd, stationNames, hash);
        }
    }

    private static ConcurrentHashMap<Integer, SampleAggregator> aggregatedCollisions = new ConcurrentHashMap<>();

    private static void aggregateCollision(
                                           ByteBuffer buf,
                                           int nameStart,
                                           int nameEnd,
                                           int hash,
                                           int temperature) {
        var sample = aggregatedCollisions.computeIfAbsent(hash, _ -> {
            var bytes = new byte[nameEnd - nameStart];
            buf.get(nameStart, bytes);
            var aggregator = new SampleAggregator();
            aggregator.stationName = new String(bytes);
            return aggregator;
        });
        sample.aggregateThreadSampleSynced(1, temperature, temperature, temperature);
    }

    /**
     * Records the name of a weather station (only needed once per hash).
     *
     * @param buf input file buffer
     * @param nameStart name start byte offset in the buffer
     * @param nameEnd name end byte offset in the buffer
     * @param stationNames buffer of weather station names (see {@link AggregationResult#stationNames} for format detail).
     * @param hash hash code for station name
     */
    private static void recordStationName(
                                          ByteBuffer buf,
                                          int nameStart,
                                          int nameEnd,
                                          ByteBuffer stationNames,
                                          int hash) {
        // set hash code and name length
        int endOfs = stationNames.getInt(0);
        stationNames.putInt(endOfs, hash);
        int len = nameEnd - nameStart;
        stationNames.putInt(endOfs + 4, len);
        endOfs += 8;

        // copy name bytes from input buffer
        int longCount = (len + 7) >>> 3;
        for (int i = 0; i < longCount; i++) {
            stationNames.putLong(endOfs, buf.getLong(nameStart + i * 8));
            endOfs += 8;
        }
        stationNames.putInt(0, endOfs);
    }

    private static void postProcess() {
        int size = 0;
        for (var p : postProcess.values()) {
            size += p.prefixBytes.length;
            if (p.postfixBytes != null) {
                size += p.postfixBytes.length;
            }
        }
        int chunkEnd = 0;
        var buf = ByteBuffer.allocateDirect(size + 8);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        for (var p : postProcess.values()) {
            buf.put(chunkEnd, p.prefixBytes);
            chunkEnd += p.prefixBytes.length;
            if (p.postfixBytes != null) {
                buf.put(chunkEnd, p.postfixBytes);
                chunkEnd += p.postfixBytes.length;
            }
        }

        var results = new AggregationResult();
        parseLines(buf, 0, chunkEnd, results.weatherData, results.stationNames);
        resultQueue.add(results);
    }

    static void printResults() {
        var aggregated = new HashMap<Integer, SampleAggregator>();
        if (!aggregatedCollisions.isEmpty()) {
            aggregated.putAll(aggregatedCollisions);
        }
        for (var threadResult : resultQueue) {
            var names = threadResult.stationNames;
            int endOfs = 4;

            int hash;
            while ((hash = names.getInt(endOfs)) != 0) {
                var ee = names.getInt(endOfs + 4);
                var entry = aggregated.get(hash);
                if (entry == null) {
                    entry = new SampleAggregator();
                    var nameLen = names.getInt(endOfs + 4);
                    var bytes = new byte[nameLen];
                    names.get(endOfs + 8, bytes);
                    entry.stationName = new String(bytes);
                    aggregated.put(hash, entry);
                }
                int ofs = (hash & WEATHER_DATA_INDEX_MASK) << 4;
                var count = (int) threadResult.weatherData.getLong(ofs);
                var t = threadResult.weatherData.getLong(ofs + 8);
                var temp = t >>> 22;
                var tempMin = 1999 - (((int) t) & 0x7ff);
                var tempMax = ((int) t) >> 11 & 0x7ff;
                entry.aggregateThreadSample(count, temp, tempMin, tempMax);

                endOfs += 8 + ((ee + 7) >>> 3) * 8;
            }
        }

        var sorted = new TreeMap<String, SampleAggregator>();
        for (var ag : aggregated.values()) {
            sorted.put(ag.stationName, ag);
        }

        var first = true;
        StringBuilder b = new StringBuilder();
        b.append("{");
        for (var e : sorted.entrySet()) {
            if (!first)
                b.append(", ");
            first = false;
            var ag = e.getValue();
            String s = e.getKey() + "=" + tempToString((double) ag.min) + "/" + tempToString((double) ag.sum / ag.count) + "/" + tempToString((double) ag.max);
            b.append(s);

        }
        b.append("}");
        out.println(b);
    }

    private static final String tempToString(Double temp) {
        return String.format(Locale.US, "%.1f", 0.1 * temp - 100);
    }

    /**
     * Aggregation result over a number of weather station samples.
     */
    private static final class AggregationResult {

        /**
         * Weather data hash table, indexed by the least significant bits
         * from the hash code for the station name.
         * Each data row consists of 16 bytes and stores the aggregated data for a weather station:
         *
         * ```
         *    64 Bit | Ofs | Field
         *   --------+-----+------------------------------------------------------------------------
         *       LO  |  0  | 32 bit sample count
         *           | 32  | 32 bit hash code for station name
         *   --------+-----+------------------------------------------------------------------------
         *       HI  |  0  | 11 bit minimum temperature (biased fixed point; -99.9..99.9 -> 1..1999)
         *           | 11  | 11 bit maximum temperature (biased fixed point; -99.9..99.9 -> 1..1999)
         *           | 22  | 42 bit sum of temperature for up to 2bn samples
         * ```
         */
        public final ByteBuffer weatherData;

        /**
         * Recorded weather station names. Buffer format:
         *
         * ```
         *     Pos | Field
         *   ------+------------------------------------------------------------------------
         *      0  | 32 bit position of @END
         *      4  | Entry #0
         *      ?  | Entry #1
         *         | ...
         *         | @END
         *
         * Entry format:
         *
         * ```
         *     Pos | Field
         *   ------+------------------------------------------------------------------------
         *      0  | 32 bit hash code for station name
         *      4  | 32 bit length of station name in bytes
         *      8  | up to 13 x 64 bit words station name (size is 64 bit word aligned)
         * ```
         */
        public final ByteBuffer stationNames;

        public AggregationResult() {
            weatherData = ByteBuffer.allocateDirect(WEATHER_DATA_ROWS * WEATHER_DATA_ROW_SIZE);
            weatherData.order(ByteOrder.LITTLE_ENDIAN);

            int maxEntrySize = 4 + ((MAX_KEY_SIZE + 7) & ~7);
            stationNames = ByteBuffer.allocateDirect(4 + MAX_KEY_COUNT * maxEntrySize);
            stationNames.order(ByteOrder.LITTLE_ENDIAN);
            stationNames.putInt(0, 4);
        }
    }

    /**
     * Lines do usually cross chunk borders, therefore every chunk has remains at the beginning
     * and the end (prefix and postfix) that cannot be processed by the worker thread in the context
     * of the current chunk alone, but that must be handled by a postprocessing step.
     */
    private static class ChunkPostprocess {

        /**
         * Byte offset of the chunk in the input file.
         */
        final long chunkStart;

        /**
         * Chunk prefix; unprocessed bytes at the beginning of the chunk.
         */
        final byte[] prefixBytes;

        /**
         * Chunk postfix; unprocessed bytes at the end of the chunk.
         */
        final byte[] postfixBytes;

        private ChunkPostprocess(long chunkStart, byte[] prefixBytes, byte[] postfixBytes) {
            this.chunkStart = chunkStart;
            this.prefixBytes = prefixBytes;
            this.postfixBytes = postfixBytes;
        }
    }

    private static class ChunkRunner implements Runnable {

        private final RandomAccessFile inputFile;
        private final long chunkStart;
        private final int chunkSize;

        ChunkRunner(RandomAccessFile f, long chunkStart, int chunkSize) {
            this.inputFile = f;
            this.chunkStart = chunkStart;
            this.chunkSize = chunkSize;
        }

        @Override
        public void run() {
            try {
                processFile();
            }
            catch (IOException e) {
                System.err.println("Could not read from file");
            }
        }

        private void processFile() throws IOException {
            MappedByteBuffer buf = inputFile.getChannel().map(FileChannel.MapMode.READ_ONLY, chunkStart, chunkSize);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            processChunk(chunkStart, buf, chunkSize);
        }
    }

    private static final class SampleAggregator {

        String stationName;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum;
        long count;

        public synchronized void aggregateThreadSampleSynced(int sampleCount, long tempSum, int tempMin, int tempMax) {
            aggregateThreadSample(sampleCount, tempSum, tempMin, tempMax);
        }

        public void aggregateThreadSample(int sampleCount, long tempSum, int tempMin, int tempMax) {
            count += sampleCount;
            sum += tempSum;
            min = min(min, tempMin);
            max = max(max, tempMax);
        }
    }
}
