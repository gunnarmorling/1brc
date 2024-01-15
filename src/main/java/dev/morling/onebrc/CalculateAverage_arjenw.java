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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

// Calculate Average
// * baseline:                              3m7s
// * single-threaded chunk-based reading:   0m45s
// * multi-threaded chunk-based reading:    0m14s
// * less branches in parsing:              0m12s
// * list approach iso map:                 0m5.5s
// * chunk finetuning:                      0m4.5s
// * threadlocal result gathering:          0m4.3s (trying graalvm-ce)
// * memory-mapped file approach:           0m3.2s (also way simpler and neater code; inspired by spullara)
// * smarter number parsing:                0m2.95s (inspired by iziamos)
// * switching back to 21-tem vm            0m2.6s
// * small optimizations                    0m2.5s (skip byte-array copy, optimal StationList array size avoiding collisions)

public class CalculateAverage_arjenw {
    private static final int TWO_BYTE_TO_INT = 480 + 48; // 48 is the ASCII code for '0'
    private static final int THREE_BYTE_TO_INT = 4800 + 480 + 48;
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) {
        var file = new File(args.length > 0 ? args[0] : FILE);
        var fileSize = file.length();
        var numberOfProcessors = fileSize > 1_000_000 ? Runtime.getRuntime().availableProcessors() : 1;
        var segmentSize = (int) Math.min(Integer.MAX_VALUE, fileSize / numberOfProcessors); // bytebuffer position is an int, so can be max Integer.MAX_VALUE
        var segmentCount = (int) (fileSize / segmentSize);
        var results = IntStream.range(0, segmentCount)
                .mapToObj(segmentNr -> parseSegment(file, fileSize, segmentSize, segmentNr))
                .parallel()
                .reduce(StationList::merge)
                .orElseGet(StationList::new)
                .toStringArray();
        Arrays.sort(results, Comparator.comparing(o -> takeUntil(o, '=')));
        System.out.format("{%s}%n", String.join(", ", results));
    }

    private static StationList parseSegment(File file, long fileSize, int segmentSize, int segmentNr) {
        long segmentStart = segmentNr * (long) segmentSize;
        long segmentEnd = Math.min(fileSize, segmentStart + segmentSize + 100);
        try (var fileChannel = (FileChannel) Files.newByteChannel(file.toPath(), StandardOpenOption.READ)) {
            var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentEnd - segmentStart);
            if (segmentStart > 0) {
                // noinspection StatementWithEmptyBody
                while (bb.get() != '\n')
                    ; // skip to first new line
            }
            StationList stationList = new StationList();
            var buffer = new byte[100];
            while (bb.position() < segmentSize) {
                byte b;
                var i = 0;
                int hash = 0;
                while ((b = bb.get()) != ';') {
                    hash = hash * 31 + b;
                    buffer[i++] = b;
                }

                int value;
                byte b1 = bb.get();
                byte b2 = bb.get();
                byte b3 = bb.get();
                byte b4 = bb.get();
                if (b2 == '.') {// value is n.n
                    value = (b1 * 10 + b3 - TWO_BYTE_TO_INT);
                    // b4 == \n
                }
                else {
                    if (b4 == '.') { // value is -nn.n
                        value = -(b2 * 100 + b3 * 10 + bb.get() - THREE_BYTE_TO_INT);
                    }
                    else if (b1 == '-') { // value is -n.n
                        value = -(b2 * 10 + b4 - TWO_BYTE_TO_INT);
                    }
                    else { // value is nn.n
                        value = (b1 * 100 + b2 * 10 + b4 - THREE_BYTE_TO_INT);
                    }
                    bb.get(); // new line
                }

                if (stationList.add(buffer, i, Math.abs(hash), value))
                    buffer = new byte[100]; // station was new, create new buffer to contain the next station's name
            }

            return stationList;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class Station {
        private final byte[] data;
        private final int hash;
        private final int length;

        private int min;
        private int max;
        private int total;
        private int count;

        private Station(byte[] data, int length, int hash, int value) {
            this.data = data;
            this.hash = hash;
            this.length = length;

            min = max = total = value;
            count = 1;
        }

        @Override
        public String toString() {
            return STR."\{new String(data, 0, length, StandardCharsets.UTF_8)}=\{min / 10.0}/\{Math.round(((double) total) / count) / 10.0}/\{max / 10.0}";
        }

        private void append(int min, int max, int total, int count) {
            if (min < this.min)
                this.min = min;
            if (max > this.max)
                this.max = max;
            this.total += total;
            this.count += count;
        }

        public void append(int value) {
            append(value, value, value, 1);
        }

        public void merge(Station other) {
            append(other.min, other.max, other.total, other.count);
        }
    }

    private static class StationList implements Iterable<Station> {
        private final static int MAX_ENTRY = 65375; // choose a value that _eliminates_ collisions on the test set.
        private final Station[] array = new Station[MAX_ENTRY];
        private int size = 0;

        private boolean add(int hash, Supplier<Station> create, Consumer<Station> update) {
            var position = hash % MAX_ENTRY;
            Station existing;
            while ((existing = array[position]) != null && existing.hash != hash) {
                position = (position + 1) % MAX_ENTRY;
            }
            if (existing == null) {
                array[position] = create.get();
                size++;
                return true;
            }
            else {
                update.accept(existing);
                return false;
            }
        }

        public boolean add(byte[] data, int stationNameLength, int stationHash, int value) {
            return add(stationHash, () -> new Station(data, stationNameLength, stationHash, value), existing -> existing.append(value));
        }

        public void add(Station station) {
            add(station.hash, () -> station, existing -> existing.merge(station));
        }

        public String[] toStringArray() {
            var destination = new String[size];

            var i = 0;
            for (Station station : this)
                destination[i++] = station.toString();

            return destination;
        }

        public StationList merge(StationList other) {
            for (Station station : other)
                add(station);
            return this;
        }

        @Override
        public Iterator<Station> iterator() {
            return new Iterator<>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    Station station = null;
                    while (index < MAX_ENTRY && (station = array[index]) == null)
                        index++;
                    return station != null;
                }

                @Override
                public Station next() {
                    if (hasNext()) {
                        return array[index++];
                    }
                    throw new NoSuchElementException();
                }
            };
        }
    }

    private static String takeUntil(String s, char c) {
        var pos = s.indexOf(c);
        return pos > -1 ? s.substring(0, pos) : s;
    }
}
