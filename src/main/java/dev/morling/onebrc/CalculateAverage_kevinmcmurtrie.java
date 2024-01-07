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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Kevin McMurtrie https://github.com/kevinmcmurtrie
 * <p>
 * Code challenge submission for https://github.com/gunnarmorling/1brc
 */
public class CalculateAverage_kevinmcmurtrie implements AutoCloseable {
    private static final String FILE = "./measurements.txt";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final int THREADS = Runtime.getRuntime().availableProcessors() + 2;

    // This is used for push-back when fitting buffers to the last line break
    private static final int MAX_LINE_LENGTH = 1024;

    private static final int READ_CHUNK_SIZE = 4 * 1024 * 1024;
    private static final int CHAR_CHUNK_SIZE = Math.max(MAX_LINE_LENGTH, 64 * 1024);

    // Internal array size for hashing cities
    private static final int HASH_BUCKETS = 1039;

    // Fixed-point number parameters
    private static final int DIGITS_AFTER_DECIMAL_POINT_INPUT = 1;
    private static final int DIGITS_AFTER_DECIMAL_POINT_OUTPUT = 1;

    // City and Temperature delimiter
    private static final char DELIMITER = ';';

    private final LineAlignedInput in; // Must synchronize on this when multiple threads are reading. Use fillFromFile().

    public static class Accumulator {
        private static final Comparator<Element> cityComparator = new Comparator<>() {
            @Override
            public int compare(Element a, Element b) {
                return Arrays.compare(a.line, b.line);
            }
        };

        private final Element buckets[];

        public Accumulator(final int bucketCount) {
            buckets = new Element[bucketCount];
        }

        /**
         * Custom hash element.
         * <ul>
         * <li>Can avoid many expensive substring operations while parsing
         * <li>Combines the key and values into a single object
         * </ul>
         */
        public static class Element {
            private final char[] line;
            private final int hashCode;
            private final Element collision;
            private long min = 0;
            private long max = 0;
            private long sum = 0;
            private long count = 0;

            public Element(final Element collision, final char[] line, final int hashCode, final long value) {
                this.collision = collision;
                this.line = line;
                this.hashCode = hashCode;
                min = value;
                max = value;
                sum = value;
                count = 1;
            }

            public Element(final Element collision, final Element src) {
                this.collision = collision;
                this.line = src.line;
                this.hashCode = src.hashCode;
                min = src.min;
                max = src.max;
                sum = src.sum;
                count = src.count;
            }

            void accumulate(final long value) {
                min = Math.min(value, min);
                max = Math.max(value, max);
                sum = Math.addExact(sum, value);
                count++;
            }

            private void merge(final Element a) {
                min = Math.min(a.min, min);
                max = Math.max(a.max, max);
                sum = Math.addExact(sum, a.sum);
                count += a.count;
            }

            @Override
            /**
             * City=min/avg/max
             */
            public String toString() {
                return new String(line) + '=' + fixedToString(min) + "/" + fixedToString((double) sum / count) + "/" + fixedToString(max);
            }
        }

        /**
         * Hasher that operates on a buffer without generating a substring
         */
        static private final int hasher(final char[] buf, final int start, final int end) {
            int hc = buf[start];
            for (int i = start + 1; i < end; ++i) {
                hc = hc * 31 + buf[i];
            }
            return hc & Integer.MAX_VALUE;
        }

        /**
         * Tests if a pre-calculated hash and buffer area match an Element
         */
        static boolean matches(final Element o, final char[] str, final int start, final int end, final int hashCode) {
            return (hashCode == o.hashCode) && Arrays.equals(o.line, 0, o.line.length, str, start, end);
        }

        static boolean matches(final Element a, final Element b) {
            return (a.hashCode == b.hashCode) && Arrays.equals(a.line, b.line);
        }

        /**
         * Merge another Accumulator into this one
         *
         * @param src
         */
        void merge(final Accumulator src) {
            for (final Element srcElementHead : src.buckets) {
                for (Element srcElem = srcElementHead; srcElem != null; srcElem = srcElem.collision) {
                    final int idx = srcElem.hashCode % buckets.length;

                    final Element elementHead = buckets[idx];
                    boolean found = false;
                    for (Element e = elementHead; e != null; e = e.collision) {
                        if (matches(e, srcElem)) {
                            e.merge(srcElem);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        buckets[idx] = new Element(elementHead, srcElem);
                    }
                }
            }
        }

        /**
         * Accumulate a weather string
         *
         * @param str City;temperature
         */
        void accumulate(final char[] buf, final int delimiterPos, final int start, final int end) {
            final long value = readFixed(buf, delimiterPos + 1, end);

            final int hc = hasher(buf, start, delimiterPos);
            final int idx = hc % buckets.length;
            final Element elementHead = buckets[idx];

            for (Element e = elementHead; e != null; e = e.collision) {
                if (matches(e, buf, start, delimiterPos, hc)) {
                    e.accumulate(value);
                    return;
                }
            }

            buckets[idx] = new Element(elementHead, Arrays.copyOfRange(buf, start, delimiterPos), hc, value);
        }

        /**
         * @return A stream of Element.toString() values.
         */
        public Stream<Element> toStream() {
            final Spliterator<Element> sp = new Spliterator<>() {
                int idx = 0;
                Element elem = null;

                @Override
                public boolean tryAdvance(final Consumer<? super Element> action) {
                    while ((elem == null) && (idx < buckets.length)) {
                        elem = buckets[idx++];
                    }

                    if (elem != null) {
                        final Element result = elem;
                        elem = result.collision;
                        action.accept(result);
                        return true;
                    }
                    return false;
                }

                @Override
                public Spliterator<Element> trySplit() {
                    return null;
                }

                @Override
                public long estimateSize() {
                    return buckets.length;
                }

                @Override
                public int characteristics() {
                    return DISTINCT | NONNULL;
                }

            };
            return StreamSupport.stream(sp, false);
        }

        /**
         * Converts rounds a higher precision fixed-point to a string. Not optimized.
         */
        static String fixedToString(final double d) {
            return String.valueOf(
                    Math.round(d / Math.pow(10, DIGITS_AFTER_DECIMAL_POINT_INPUT - DIGITS_AFTER_DECIMAL_POINT_OUTPUT)) / Math.pow(10, DIGITS_AFTER_DECIMAL_POINT_OUTPUT));
        }

        /**
         * Read the suffix of a string as a fixed point number.
         * Doesn't allocate memory except for exceptions.
         */
        static long readFixed(final char[] str, final int offset, final int end) {
            char c;
            int pos = offset;
            while ((c = str[pos]) == ' ') {
                pos++;
            }

            final boolean negate = c == '-';
            if (negate) {
                pos++;
            }

            c = str[pos++];
            if ((c < '0') || (c > '9')) {
                throw new IllegalArgumentException(new String(str, offset, end - offset));
            }

            long v = c - '0';

            for (; pos < end; ++pos) {
                c = str[pos];
                if (c == '.') {
                    pos++;
                    break;
                }
                if ((c < '0') || (c > '9')) {
                    throw new IllegalArgumentException(new String(str, offset, end - offset));
                }
                v = v * 10 + c - '0';
            }

            final int fractLimit = pos + DIGITS_AFTER_DECIMAL_POINT_INPUT;
            for (; (pos < end) && (pos < fractLimit); ++pos) {
                c = str[pos];
                if ((c < '0') || (c > '9')) {
                    throw new IllegalArgumentException(new String(str, offset, end - offset));
                }
                v = v * 10 + c - '0';
            }

            for (; (pos < fractLimit); ++pos) {
                v = v * 10;
            }

            return negate ? -v : v;
        }
    }

    public CalculateAverage_kevinmcmurtrie(final String path) throws IOException {
        in = new LineAlignedInput(new FileInputStream(path), MAX_LINE_LENGTH);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    /**
     * Fill a byte buffer with the end aligned to a CR or LF.
     *
     * @param b A byte array at least large enough to hold one full line.
     * @return Number of bytes filled, or zero for EOF
     * @throws IOException
     */
    static class LineAlignedInput implements AutoCloseable {
        private final InputStream in;
        private final byte[] pushbackStack;
        private int pushedBackLen = 0;

        public LineAlignedInput(final InputStream in, final int maxLineLength) {
            this.in = in;
            pushbackStack = new byte[maxLineLength];
        }

        public int fillChunk(final byte buf[]) throws IOException {
            int offset = 0;
            // Recover last pushback
            while ((pushedBackLen > 0) && (offset < buf.length)) {
                buf[offset++] = pushbackStack[--pushedBackLen];
            }

            final int readSize = in.read(buf, offset, buf.length - offset);
            if (readSize <= 0) {
                return offset;
            }
            final int size = readSize + offset;

            // Roll back end of buffer to a line break so it's not truncated
            int rollbackPos = size - 1;
            if (rollbackPos > 0) {
                byte b;
                while (((b = buf[rollbackPos]) != '\n') && (b != '\r')) {
                    pushbackStack[pushedBackLen++] = b;
                    rollbackPos--;
                    if (rollbackPos == 0) {
                        return size; // Last entry. Return as-as.
                    }
                }
            }

            return rollbackPos + 1;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * Fill a char buffer with the end aligned to a CR or LF.
     *
     * @param b A char array at least large enough to hold one full line.
     * @return Number of bytes filled, or zero for EOF
     * @throws IOException
     */
    static class LineAlignedReader implements AutoCloseable {
        private final Reader in;
        private final char[] pushbackStack;
        private int pushedBackLen = 0;

        public LineAlignedReader(final Reader in, final int maxLineLength) {
            this.in = in;
            pushbackStack = new char[maxLineLength];
        }

        public int fillChunk(final char buf[]) throws IOException {
            int offset = 0;
            // Recover last pushback
            while ((pushedBackLen > 0) && (offset < buf.length)) {
                buf[offset++] = pushbackStack[--pushedBackLen];
            }

            final int readSize = in.read(buf, offset, buf.length - offset);
            if (readSize <= 0) {
                return offset;
            }
            final int size = readSize + offset;

            // Roll back end of buffer to a line break so it's not truncated
            int rollbackPos = size - 1;
            if (rollbackPos > 0) {
                char b;
                while (((b = buf[rollbackPos]) != '\n') && (b != '\r')) {
                    pushbackStack[pushedBackLen++] = b;
                    rollbackPos--;
                    if (rollbackPos == 0) {
                        return size; // Last entry. Return as-as.
                    }
                }
            }

            return rollbackPos + 1;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    private int fillFromFile(final byte buf[]) throws IOException {
        synchronized (in) {
            return in.fillChunk(buf);
        }
    }

    /**
     * Read as fast as possible and collect values.
     * There's some expensive charset and String work here so many of these may run in parallel.
     */
    public Accumulator collect() throws IOException {
        final Accumulator accumulation = new Accumulator(HASH_BUCKETS);
        final byte buf[] = new byte[READ_CHUNK_SIZE];
        final char cbuf[] = new char[CHAR_CHUNK_SIZE];

        int blen;
        while ((blen = fillFromFile(buf)) > 0) {
            try (LineAlignedReader reader = new LineAlignedReader(new InputStreamReader(new ByteArrayInputStream(buf, 0, blen), CHARSET), CHAR_CHUNK_SIZE)) {
                int length;
                while ((length = reader.fillChunk(cbuf)) > 0) {
                    int pos = 0;
                    do {
                        // Skip whitespace
                        while ((pos < length) && Character.isWhitespace(cbuf[pos])) {
                            pos++;
                        }
                        final int start = pos;
                        if (start < length) {
                            int lastDelimiterPos = -1;
                            int c;
                            while ((pos < length) && ((c = cbuf[pos]) != '\n') && (c != '\r')) {
                                if (c == DELIMITER) {
                                    lastDelimiterPos = pos;
                                }
                                pos++;
                            }

                            if (pos > start) {
                                if (lastDelimiterPos < 1) {
                                    throw new IllegalArgumentException("Malformed input: " + new String(cbuf, start, pos - start));
                                }
                                accumulation.accumulate(cbuf, lastDelimiterPos, start, pos);
                            }
                        }
                    } while (pos < length);
                }
            }
        }
        return accumulation;
    }

    /**
     * Run multiple collectors and merge the results. Performance
     * is optimized for reading many values but not for merging many results.
     *
     * @param threads How many threads to allocate
     * @return Accumulator
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public Accumulator collectParallel(final int threads) throws InterruptedException, ExecutionException {
        final Accumulator acc;
        // ForkJoinPool is somehow faster even without major work stealing. Class loading?
        try (final ExecutorService pool = new ForkJoinPool(threads)) {
            @SuppressWarnings("unchecked")
            final Future<Accumulator> tasks[] = new Future[threads];

            for (int i = 0; i < threads; ++i) {
                tasks[i] = pool.submit(this::collect);
            }
            acc = tasks[0].get();
            for (int i = 1; i < threads; ++i) {
                acc.merge(tasks[i].get());
            }
        }
        return acc;
    }

    public static void main(final String args[]) throws IOException, InterruptedException, ExecutionException {
        // final long startMillis = System.currentTimeMillis();

        final String path = args.length > 0 ? args[0] : FILE;

        final Accumulator acc;
        try (CalculateAverage_kevinmcmurtrie c = new CalculateAverage_kevinmcmurtrie(path)) {
            acc = c.collectParallel(THREADS);
        }

        System.out.println(acc.toStream().sorted(Accumulator.cityComparator).map(String::valueOf).collect(Collectors.joining(", ", "{", "}")));
        // System.out.println((System.currentTimeMillis() - startMillis) / 1000f);
    }

}
