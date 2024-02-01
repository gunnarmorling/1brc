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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Math.round;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;

public class CalculateAverage_JurenIvan {

    private static final String FILE_NAME = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        long[] segments = getSegments(Runtime.getRuntime().availableProcessors());

        var result = IntStream.range(0, segments.length - 1)
                .parallel()
                .mapToObj(i -> processSegment(segments[i], segments[i + 1]))
                .flatMap(m -> Arrays.stream(m.hashTable).filter(Objects::nonNull))
                .collect(Collectors.toMap(m -> new String(m.city), m -> m, Measurement::merge, TreeMap::new));

        System.out.println(result);
    }

    private static LinearProbingHashMap processSegment(long start, long end) {
        var results = new LinearProbingHashMap(1 << 19);

        try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE_NAME), READ)) {
            var bb = fileChannel.map(READ_ONLY, start, end - start);
            var buffer = new byte[100];

            int limit = bb.limit();
            for (int startLine = bb.position(); startLine < limit; startLine = bb.position()) {
                int currentPosition = startLine;

                byte b;
                int hash = 7;
                int wordLen = 0;
                while (currentPosition < end && (b = bb.get(currentPosition++)) != ';') {
                    buffer[wordLen++] = b;
                    hash = hash * 31 + b;
                }

                int temp;
                int negative = 1;
                if (bb.get(currentPosition) == '-') {
                    negative = -1;
                    currentPosition++;
                }

                if (bb.get(currentPosition + 1) == '.') {
                    temp = negative * ((bb.get(currentPosition) - '0') * 10 + (bb.get(currentPosition + 2) - '0'));
                    currentPosition += 3;
                }
                else {
                    temp = negative * ((bb.get(currentPosition) - '0') * 100 + ((bb.get(currentPosition + 1) - '0') * 10 + (bb.get(currentPosition + 3) - '0')));
                    currentPosition += 4;
                }

                currentPosition++;

                results.put(hash, buffer, wordLen, temp);

                bb.position(currentPosition);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    private static long[] getSegments(int segmentCount) throws IOException {
        try (var raf = new RandomAccessFile(FILE_NAME, "r")) {
            long fileSize = raf.length();

            if (fileSize < 100000) {
                long[] chunks = new long[2];
                chunks[1] = fileSize;
                return chunks;
            }

            while (fileSize / segmentCount >= (Integer.MAX_VALUE - 150)) {
                segmentCount *= 2;
            }

            long[] chunks = new long[segmentCount + 1];

            chunks[0] = 0;
            long segmentSize = fileSize / segmentCount;

            for (int i = 1; i < segmentCount; i++) {
                long chunkOffset = chunks[i - 1] + segmentSize;
                raf.seek(chunkOffset);
                while (raf.readByte() != '\n') {
                }
                chunks[i] = raf.getFilePointer();
            }
            chunks[segmentCount] = fileSize;
            return chunks;
        }
    }

    public static class LinearProbingHashMap {
        final Measurement[] hashTable;
        int slots;

        public LinearProbingHashMap(int slots) {
            this.slots = slots;
            this.hashTable = new Measurement[slots];
        }

        void put(int hash, byte[] key, int len, int temperature) {
            hash = Math.abs(hash);
            int index = hash & (slots - 1);

            int i = index;
            while (hashTable[i] != null) {
                if (keyIsEqual(key, hashTable[i].city, len)) { // handling hash collisions
                    hashTable[i].add(temperature);
                    return;
                }
                i++;
                if (i == slots) {
                    i = 0;
                }
            }

            var cityArr = new byte[len];
            System.arraycopy(key, 0, cityArr, 0, len);
            hashTable[i] = new Measurement(cityArr, hash, temperature, temperature, 1, temperature);
        }

        private boolean keyIsEqual(byte[] one, byte[] other, int len) {
            if (len != other.length)
                return false;
            for (int i = 0; i < len; i++) {
                if (one[i] != other[i]) {
                    return false;
                }
            }
            return true;
        }

    }

    static class Measurement {
        byte[] city;
        int hash;
        int min;
        int max;
        int count;
        long sum;

        public Measurement(byte[] city, int hash, int min, int max, int count, long sum) {
            this.city = city;
            this.hash = hash;
            this.min = min;
            this.max = max;
            this.count = count;
            this.sum = sum;
        }

        public void add(int temperature) {
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
            count++;
            sum += temperature;
        }

        public Measurement merge(Measurement other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            count += other.count;
            sum += other.sum;
            return this;
        }

        @Override
        public String toString() {
            return (min * 1.0) / 10 + "/" + round((sum * 1.0) / count) / 10.0 + "/" + (max * 1.0) / 10;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            return Arrays.equals(city, ((Measurement) obj).city);
        }
    }
}
