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

import java.io.*;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

// gunnar morling - 2:10
// roy van rijn -   1:01
//                  0:37

public class CalculateAverage_ddimtirov {


    @SuppressWarnings("RedundantSuppression")
    public static void main(String[] args) throws IOException {
//        var start = Instant.now();
        Instant start = null;
        var path = Path.of("./measurements.txt");
        var bufferSize = 512 * 64; // 64 blocks
        var tracker = new Tracker();
        var charset = StandardCharsets.UTF_8;

        // Files.lines() is optimized for files that can be indexed by int
        // For larger files it falls back to buffered reader, which we now
        // use directly to be able to tweak the buffer size.
        try (var stream = Files.newInputStream(path); var reader = new InputStreamReader(stream, charset)) {
            var buffered = new RecordReader(reader, bufferSize);

            InputRecord record = null;
            while (true) {
                record = buffered.readRecord(record);
                if (record==null) break;

                tracker.process(record);
            }
        }

        System.out.println(tracker.stationsToMetrics());

        //noinspection ConstantValue
        if (start!=null) System.err.println(Duration.between(start, Instant.now()));

        assert Files.readAllLines(Path.of("expected_result.txt")).getFirst().equals(tracker.stationsToMetrics().toString());
    }

    /**
     * <p>Reads records we can use to do the filtering from a stream of characters.
     * Takes care of framing, and I/O buffering.
     * This class in combination with {@link RecordReader} allows us to fully hide the
     * I/O and internal representation.
     *
     * <p>If used with recycled {@link InputRecord} instances, this class allocates no memory
     * after instantiation. The class is stateful, and the internals are not threadsafe
     * - use from single thread or with proper synchronization.
     */
    static class RecordReader {
        /**
         * The source of input data
         */
        private final Readable input;

        /**
         * Used for i/o buffering and record parsing.
         * @see #buf
         */
        private final CharBuffer buffer;

        /**
         * <p>Cached backing array from {@link #buffer}.
         * <p>This is optimization because {@link CharBuffer#array()} was showing on the CPU profile.
         */
        private final char[] buf;

        public RecordReader(Readable input, int bufferSize) {
            this.input = input;
            buffer = CharBuffer.allocate(bufferSize).flip();
            buf = buffer.array();
        }

        public InputRecord readRecord(InputRecord recycled) throws IOException {
            var record = parseRecord(recycled);
            if (record!=null) return record;

            if (input.read(buffer.compact())==-1) return null;
            buffer.flip();

            return parseRecord(recycled);
        }


        private InputRecord parseRecord(InputRecord recycled) {
            var lim = buffer.limit();
            if (buffer.isEmpty()) return null;

            var nameOff = buffer.position();
            var buff = buf;
            while (buff[nameOff]=='\n' || buff[nameOff]=='\r' || buff[nameOff]==' ' ) {
                nameOff++;
                if (nameOff>=lim) return null;
            }

            var nameHash = 0;
            var nameLen = 0;
            while (buff[nameOff+nameLen]!=';') {
                nameHash = nameHash*31 + buff[nameOff+nameLen];
                nameLen++;
                if (nameOff+nameLen>=lim) return null;
            }

            //noinspection DuplicateExpressions
            assert new String(buf, nameOff, nameLen).hashCode()==nameHash
                 : "'%s'@%d !-> %d".formatted(new String(buf, nameOff, nameLen), new String(buf, nameOff, nameLen).hashCode(), nameHash);

            var valCursor = nameOff + nameLen +1;
            int signum = 1;
            var acc = 0;
            while (true) {
                if (valCursor >= lim) {
                    return null;
                }
                char c = buff[valCursor++];
                if (c == '\n' || c == '\r') {
                    break;
                }
                if (c=='.') continue;
                if (acc == 0) {
                    if (c == '-') {
                        signum = -1;
                        continue;
                    }
                } else {
                    acc *= 10;
                }
                var v = c - '0';
                assert v>=0 && v<=9 : String.format("Character '%s', value %,d", c, v);
                acc += v;
            }

            buffer.position(valCursor);

            var record = recycled!=null ? recycled : new InputRecord(buf);
            record.init(nameHash, nameOff, nameLen, acc*signum);
            return record;
        }
    }


    static class Tracker {
        private static final int ADDRESS_NO_CLASH_MODULUS = 49999;
        private static final int OFFSET_MIN = 0;
        private static final int OFFSET_MAX = 1;
        private static final int OFFSET_COUNT = 2;

        private final int[] minMaxCount = new int[ADDRESS_NO_CLASH_MODULUS * 3];
        private final long[] sums = new long[ADDRESS_NO_CLASH_MODULUS];
        private final String[] names = new String[ADDRESS_NO_CLASH_MODULUS];

        public void process(InputRecord r) {
            var i = Math.abs(r.nameHash) % ADDRESS_NO_CLASH_MODULUS;

            if (names[i]==null) names[i] = r.name();

            sums[i] += r.value;

            int mmcIndex = i * 3;
            var min = minMaxCount[mmcIndex + OFFSET_MIN];
            var max = minMaxCount[mmcIndex + OFFSET_MAX];
            if (r.value < min) minMaxCount[mmcIndex + OFFSET_MIN] = r.value;
            if (r.value > max) minMaxCount[mmcIndex + OFFSET_MAX] = r.value;

            minMaxCount[mmcIndex + OFFSET_COUNT]++;
        }



        public Map<String, String> stationsToMetrics() {
            var m = new TreeMap<String, String>();
            for (int i = 0; i < names.length; i++) {
                var name = names[i];
                if (name==null) continue;

                var min = minMaxCount[i*3] / 10.0;
                var max = minMaxCount[i*3+1] / 10.0;
                var count = minMaxCount[i*3+2];
                var sum = sums[i];
                var mean = Math.round((double) sum / count) / 10.0;

                m.put(name, min + "/" + mean + "/" + max);
            }
            return m;
        }

    }

    static class InputRecord {
        private final char[] chars;
        private int idOffset;
        private int idLength;

        public int value; // fixpoint scaled by 10
        public int nameHash;

        public InputRecord(char[] chars) {
            this.chars = chars;
        }

        public void init(int nameHash, int nameOffset, int nameLength, int fixpointValue) {
            assert nameOffset+nameLength<chars.length : String.format("idOffset+idLength=%d < chars.length=%d", nameOffset+nameLength, chars.length);

            this.idOffset = nameOffset;
            this.idLength = nameLength;
            this.value = fixpointValue;
            this.nameHash = nameHash;
        }

        public String name() {
            return new String(chars, idOffset, idLength);
        }

        @Override
        public String toString() {
            return name() + ";" + (value / 10.0);
        }
    }
}
