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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * all credit to royvanrijn
 * my small change to his code
 * another Skipping string creation
 */
public class CalculateAverage_kgonia {

    private static final String FILE = "./measurements.txt";

    // mutable state now instead of records, ugh, less instantiation.
    static final class Measurement {
        int min, max, count;
        long sum;

        public Measurement() {
            this.min = 10000;
            this.max = -10000;
        }

        public Measurement updateWith(int measurement) {
            min = min(min, measurement);
            max = max(max, measurement);
            sum += measurement;
            count++;
            return this;
        }

        public Measurement updateWith(Measurement measurement) {
            min = min(min, measurement.min);
            max = max(max, measurement.max);
            sum += measurement.sum;
            count += measurement.count;
            return this;
        }

        public String toString() {
            return round(min) + "/" + round((1.0 * sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private static Map<String, Measurement> merge(Map<String, Measurement> map1,
                                                  Map<String, Measurement> map2) {
        for (var entry : map2.entrySet()) {
            map1.merge(entry.getKey(), entry.getValue(), (e1, e2) -> e1.updateWith(e2));
        }
        return map1;
    }

    public static final void main(String[] args) throws Exception {

        new CalculateAverage_kgonia().run();
    }

    private static BitTwiddledMap merge(BitTwiddledMap map1, BitTwiddledMap map2) {
        for (var entry : map2.values) {
            map1.getOrCreate(entry.key).updateWith(entry.measurement);
        }
        return map1;
    }

    public void run() throws Exception {

        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), EnumSet.of(StandardOpenOption.READ))) {

            var customMap = splitFileChannel(fileChannel)
                    .parallel()
                    .map(this::processBuffer)
                    .collect(Collectors.reducing(CalculateAverage_kgonia::merge));

            // Seems to perform better than actually using a TreeMap:
            System.out.println("{" + customMap.orElseThrow().values
                    .stream()
                    .peek(BitTwiddledMap.Entry::elevateKey)
                    .sorted(Comparator.comparing(e -> e.keyWrapped))
                    .map(Object::toString)
                    .collect(Collectors.joining(", ")) + "}");
        }
    }

    private BitTwiddledMap processBuffer(ByteBuffer bb) {

        BitTwiddledMap measurements = new BitTwiddledMap();

        final int limit = bb.limit();
        final byte[] buffer = new byte[64];

        while (bb.position() < limit) {

            // Find the correct positions in the bytebuffer:

            // Start:
            final int startPointer = bb.position();

            // Separator:
            int separatorPointer = startPointer + 3; // key is at least 3 long
            while (separatorPointer != limit && bb.get(separatorPointer) != ';') {
                separatorPointer++;
            }

            // EOL:
            int endPointer = separatorPointer + 3; // temperature is at least 3 long
            while (endPointer != limit && bb.get(endPointer) != '\n')
                endPointer++;

            // Extract the name of the key and move the bytebuffer:
            final int nameLength = separatorPointer - startPointer;
            byte[] name = new byte[nameLength];

            bb.get(name, 0, nameLength);
//            final String key = new String(buffer, 0, nameLength);

            bb.get(); // skip the separator

            // Extract the measurement value (10x), skip making a String altogether:
            final int valueLength = endPointer - separatorPointer - 1;
            bb.get(buffer, 0, valueLength);

            // and get rid of the new line (handle both kinds)
            byte newline = bb.get();
            if (newline == '\r')
                bb.get();

            int measured = branchlessParseInt(buffer, valueLength);

            // Update the map, computeIfAbsent has the least amount of branches I think, compared to get()/put() or merge() or compute():
            measurements.getOrCreate(name).updateWith(measured);
        }

        return measurements;
    }

    /**
     * Thanks to bjhara for the idea of using memory mapped files, TIL.
     * @param fileChannel
     * @return
     * @throws IOException
     */
    private static Stream<ByteBuffer> splitFileChannel(final FileChannel fileChannel) throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private static final long CHUNK_SIZE = (long) Math.pow(2, 19);

            private final long size = fileChannel.size();
            private long bytesRead = 0;

            @Override
            public ByteBuffer next() {
                try {
                    final MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, bytesRead, Math.min(CHUNK_SIZE, size - bytesRead));

                    // Adjust end to start of a line:
                    int realEnd = mappedByteBuffer.limit() - 1;
                    while (mappedByteBuffer.get(realEnd) != '\n') {
                        realEnd--;
                    }
                    mappedByteBuffer.limit(++realEnd);
                    bytesRead += realEnd;

                    return mappedByteBuffer;
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return bytesRead < size;
            }
        }, Spliterator.IMMUTABLE), false);
    }

    /**
     * Branchless parser, goes from String to int (10x):
     * "-1.2" to -12
     * "40.1" to 401
     * etc.
     *
     * @param input
     * @return int value x10
     */
    private static int branchlessParseInt(final byte[] input, int length) {
        // 0 if positive, 1 if negative
        final int negative = ~(input[0] >> 4) & 1;
        // 0 if nr length is 3, 1 if length is 4
        final int has4 = ((length - negative) >> 2) & 1;

        final int digit1 = input[negative] - '0';
        final int digit2 = input[negative + has4] - '0';
        final int digit3 = input[2 + negative + has4] - '0';

        return (-negative ^ (has4 * (digit1 * 100) + digit2 * 10 + digit3) - negative);
    }

    // branchless max (unprecise for large numbers, but good enough)
    static int max(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return a - (diff & dsgn);
    }

    // branchless min (unprecise for large numbers, but good enough)
    static int min(final int a, final int b) {
        final int diff = a - b;
        final int dsgn = diff >> 31;
        return b + (diff & dsgn);
    }

    /**
     * A normal Java HashMap does all these safety things like boundary checks... we don't need that, we need speeeed.
     *
     * So I've written an extremely simple linear probing hashmap that should work well enough.
     */
    class BitTwiddledMap {
        private static final int SIZE = 2048; // A bit larger than the number of keys, needs power of two
        private int[] indices = new int[SIZE]; // Hashtable is just an int[]

        BitTwiddledMap() {
            // Optimized fill with -1, fastest method:
            int len = indices.length;
            if (len > 0) {
                indices[0] = -1;
            }
            // Value of i will be [1, 2, 4, 8, 16, 32, ..., len]
            for (int i = 1; i < len; i += i) {
                System.arraycopy(indices, 0, indices, i, i);
            }
        }

        private List<Entry> values = new ArrayList<>(1024);

        public class Entry {
            private int hash;
            private byte[] key;

            private String keyWrapped;
            private Measurement measurement;

            public Entry(int hash, byte[] key, Measurement measurement) {
                this.hash = hash;
                this.key = key;
                this.measurement = measurement;
            }

            public void elevateKey(){
                keyWrapped = new String(key);
            }

            @Override
            public String toString() {
                return keyWrapped + "=" + measurement;
            }
        }

        /**
         * Who needs methods like add(), merge(), compute() etc, we need one, getOrCreate.
         * @param key
         * @return
         */
        public Measurement getOrCreate(byte[] key) {
            int inHash;
            int index = (SIZE - 1) & (inHash = Arrays.hashCode(key));
            int valueIndex;
            Entry retrievedEntry = null;
            while ((valueIndex = indices[index]) != -1 && (retrievedEntry = values.get(valueIndex)).hash != inHash) {
                index = (index + 1) % SIZE;
            }
            if (valueIndex >= 0) {
                return retrievedEntry.measurement;
            }
            // New entry, insert into table and return.
            indices[index] = values.size();
            Entry toAdd = new Entry(inHash, key, new Measurement());
            values.add(toAdd);
            return toAdd.measurement;
        }

        private int hash(String key) {
            // Implement your custom hash function here
            int h;
            return (h = key.hashCode()) ^ (h >>> 16);
        }
    }

}
