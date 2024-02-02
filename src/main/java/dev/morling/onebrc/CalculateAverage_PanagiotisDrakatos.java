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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_PanagiotisDrakatos {
    private static final String FILE = "./measurements.txt";
    private static final long MAP_SIZE = 1024 * 1024 * 12L;
    private static TreeMap<String, MeasurementObject> sortedCities;

    public static void main(String[] args) throws IOException {
        SeekableByteRead(FILE);
        System.out.println(sortedCities.toString());
        boolean DEBUG = true;
    }

    private static void SeekableByteRead(String path) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File(FILE));
        FileChannel fileChannel = fileInputStream.getChannel();
        try {
            sortedCities = getFileSegments(new File(FILE), fileChannel).stream()
                    .map(CalculateAverage_PanagiotisDrakatos::SplitSeekableByteChannel)
                    .parallel()
                    .map(CalculateAverage_PanagiotisDrakatos::MappingByteBufferToData)
                    .flatMap(MeasurementRepository::get)
                    .collect(Collectors.toMap(e -> e.cityName, MeasurementRepository.Entry::measurement, MeasurementObject::updateWith, TreeMap::new));
        }
        catch (NullPointerException e) {
        }
        fileChannel.close();
    }

    record FileSegment(long start, long end, FileChannel fileChannel) {
    }

    private static List<FileSegment> getFileSegments(final File file, final FileChannel fileChannel) throws IOException {
        final int numberOfSegments = Runtime.getRuntime().availableProcessors();
        final long fileSize = file.length();
        final long segmentSize = fileSize / numberOfSegments;
        final List<FileSegment> segments = new ArrayList<>();
        if (segmentSize < 1000) {
            segments.add(new FileSegment(0, fileSize, fileChannel));
            return segments;
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long segStart = 0;
            long segEnd = segmentSize;
            while (segStart < fileSize) {
                segEnd = findSegment(randomAccessFile, segEnd, fileSize);
                segments.add(new FileSegment(segStart, segEnd, fileChannel));
                segStart = segEnd; // Just re-use the end and go from there.
                segEnd = Math.min(fileSize, segEnd + segmentSize);
            }
        }
        return segments;
    }

    private static long findSegment(RandomAccessFile raf, long location, final long fileSize) throws IOException {
        raf.seek(location);
        while (location < fileSize) {
            location++;
            if (raf.read() == '\n')
                return location;
        }
        return location;
    }

    private static ByteBuffer SplitSeekableByteChannel(FileSegment segment) {
        try {
            MappedByteBuffer buffer = segment.fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segment.end - segment.start());
            return buffer;
        }
        catch (Exception ex) {
            long start = segment.start;
            long end = 0;
            try {
                end = segment.fileChannel.size();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            MappedByteBuffer buffer = null;
            ArrayList<ByteBuffer> list = new ArrayList<>();
            while (start < end) {
                try {
                    buffer = segment.fileChannel.map(FileChannel.MapMode.READ_ONLY, start, Math.min(MAP_SIZE, end - start));
                    // don't split the data in the middle of lines
                    // find the closest previous newline
                    int realEnd = buffer.limit() - 1;
                    while (buffer.get(realEnd) != '\n')
                        realEnd--;

                    realEnd++;
                    buffer.limit(realEnd);
                    start += realEnd;
                    list.add(buffer.slice(0, realEnd - 1));
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sortedCities = list.stream().parallel().map(CalculateAverage_PanagiotisDrakatos::MappingByteBufferToData).flatMap(MeasurementRepository::get)
                    .collect(Collectors.toMap(e -> e.cityName, MeasurementRepository.Entry::measurement, MeasurementObject::updateWith, TreeMap::new));
            return null;
        }
    }

    public static ByteBuffer concat(ByteBuffer[] buffers) {
        int overAllCapacity = 0;
        for (int i = 0; i < buffers.length; i++)
            overAllCapacity += buffers[i].limit() - buffers[i].position();
        overAllCapacity += buffers[0].limit() - buffers[0].position();
        ByteBuffer all = ByteBuffer.allocate(overAllCapacity);
        for (int i = 0; i < buffers.length; i++) {
            ByteBuffer curr = buffers[i];
            all.put(curr);
        }

        all.flip();
        return all;
    }

    private static TreeMap<String, MeasurementObject> combineMaps(Stream<MeasurementRepository.Entry> stream1, Stream<MeasurementRepository.Entry> stream2) {
        Stream<MeasurementRepository.Entry> resultingStream = Stream.concat(stream1, stream2);
        return resultingStream.collect(Collectors.toMap(e -> e.cityName, MeasurementRepository.Entry::measurement, MeasurementObject::updateWith, TreeMap::new));
    }

    private static int longHashStep(final int hash, final long word) {
        return 31 * hash + (int) (word ^ (word >>> 32));
    }

    private static final long SEPARATOR_PATTERN = compilePattern((byte) ';');

    private static long compilePattern(final byte value) {
        return ((long) value << 56) | ((long) value << 48) | ((long) value << 40) | ((long) value << 32) | ((long) value << 24) | ((long) value << 16)
                | ((long) value << 8) | (long) value;
    }

    private static MeasurementRepository MappingByteBufferToData(ByteBuffer byteBuffer) {
        MeasurementRepository measurements = new MeasurementRepository();
        ByteBuffer bb = byteBuffer.duplicate();

        int start = 0;
        int limit = bb.limit();

        long[] cityNameAsLongArray = new long[16];
        int[] delimiterPointerAndHash = new int[2];

        bb.order(ByteOrder.nativeOrder());
        final boolean bufferIsBigEndian = bb.order().equals(ByteOrder.BIG_ENDIAN);

        while ((start = bb.position()) < limit + 1) {

            int delimiterPointer;

            findNextDelimiterAndCalculateHash(bb, SEPARATOR_PATTERN, start, limit, delimiterPointerAndHash, cityNameAsLongArray, bufferIsBigEndian);
            delimiterPointer = delimiterPointerAndHash[0];
            // Simple lookup is faster for '\n' (just three options)
            if (delimiterPointer >= limit) {
                return measurements;
            }
            final int cityNameLength = delimiterPointer - start;

            int temp_counter = 0;
            int temp_end = delimiterPointer + 1;
            try {
                // bb.position(delimiterPointer++);
                while (bb.get(temp_end) != '\n') {
                    temp_counter++;
                    temp_end++;
                }
            }
            catch (IndexOutOfBoundsException e) {
                // temp_counter--;
                // temp_end--;
            }
            ByteBuffer temp = bb.duplicate().slice(delimiterPointer + 1, temp_counter);
            int tempPointer = 0;
            int abs = 1;
            if (temp.get(0) == '-') {
                abs = -1;
                tempPointer++;
            }
            int measuredValue;
            if (temp.get(tempPointer + 1) == '.') {
                measuredValue = abs * ((temp.get(tempPointer)) * 10 + (temp.get(tempPointer + 2)) - 528);
            }
            else {
                measuredValue = abs * (temp.get(tempPointer) * 100 + temp.get(tempPointer + 1) * 10 + temp.get(tempPointer + 3) - 5328);
            }

            measurements.update(cityNameAsLongArray, bb, cityNameLength, delimiterPointerAndHash[1]).updateWith(measuredValue);

            if (temp_end + 1 > limit)
                return measurements;
            bb.position(temp_end + 1);
        }
        return measurements;
    }

    private static void findNextDelimiterAndCalculateHash(final ByteBuffer bb, final long pattern, final int start, final int limit, final int[] output,
                                                          final long[] asLong, final boolean bufferBigEndian) {
        int hash = 1;
        int i;
        int lCnt = 0;
        for (i = start; i <= limit - 8; i += 8) {
            long word = bb.getLong(i);
            if (bufferBigEndian) {
                word = Long.reverseBytes(word); // Reversing the bytes is the cheapest way to do this
            }
            final long match = word ^ pattern;
            long mask = ((match - 0x0101010101010101L) & ~match) & 0x8080808080808080L;

            if (mask != 0) {
                final int index = Long.numberOfTrailingZeros(mask) >> 3;
                output[0] = (i + index);

                final long partialHash = word & ((mask >> 7) - 1);
                asLong[lCnt] = partialHash;
                output[1] = longHashStep(hash, partialHash);
                return;
            }
            asLong[lCnt++] = word;
            hash = longHashStep(hash, word);
        }
        // Handle remaining bytes near the limit of the buffer:
        long partialHash = 0;
        int len = 0;
        for (; i < limit; i++) {
            byte read;
            if ((read = bb.get(i)) == (byte) pattern) {
                asLong[lCnt] = partialHash;
                output[0] = i;
                output[1] = longHashStep(hash, partialHash);
                return;
            }
            partialHash = partialHash | ((long) read << (len << 3));
            len++;
        }
        output[0] = limit; // delimiter not found
    }

    static class MeasurementRepository {
        private int tableSize = 1 << 20; // can grow in theory, made large enough not to (this is faster)
        private int tableMask = (tableSize - 1);
        private int tableLimit = (int) (tableSize * LOAD_FACTOR);
        private int tableFilled = 0;
        private static final float LOAD_FACTOR = 0.8f;

        private Entry[] table = new Entry[tableSize];

        record Entry(int hash, long[] nameBytesInLong, String cityName, MeasurementObject measurement) {
            @Override
            public String toString() {
                return cityName + "=" + measurement;
            }
        }

        public MeasurementObject update(long[] nameBytesInLong, ByteBuffer bb, int length, int calculatedHash) {

            final int nameBytesInLongLength = 1 + (length >>> 3);

            int index = calculatedHash & tableMask;
            Entry tableEntry;
            while ((tableEntry = table[index]) != null
                    && (tableEntry.hash != calculatedHash || !arrayEquals(tableEntry.nameBytesInLong, nameBytesInLong, nameBytesInLongLength))) { // search for the right spot
                index = (index + 1) & tableMask;
            }

            if (tableEntry != null) {
                return tableEntry.measurement;
            }

            // --- This is a brand new entry, insert into the hashtable and do the extra calculations (once!) do slower calculations here.
            MeasurementObject measurement = new MeasurementObject();

            // Now create a string:
            byte[] buffer = new byte[length];
            bb.get(buffer, 0, length);
            String cityName = new String(buffer, 0, length);

            // Store the long[] for faster equals:
            long[] nameBytesInLongCopy = new long[nameBytesInLongLength];
            System.arraycopy(nameBytesInLong, 0, nameBytesInLongCopy, 0, nameBytesInLongLength);

            // And add entry:
            Entry toAdd = new Entry(calculatedHash, nameBytesInLongCopy, cityName, measurement);
            table[index] = toAdd;

            // Resize the table if filled too much:
            if (++tableFilled > tableLimit) {
                resizeTable();
            }

            return toAdd.measurement;
        }

        private void resizeTable() {
            // Resize the table:
            Entry[] oldEntries = table;
            table = new Entry[tableSize <<= 2]; // x2
            tableMask = (tableSize - 1);
            tableLimit = (int) (tableSize * LOAD_FACTOR);

            for (Entry entry : oldEntries) {
                if (entry != null) {
                    int updatedTableIndex = entry.hash & tableMask;
                    while (table[updatedTableIndex] != null) {
                        updatedTableIndex = (updatedTableIndex + 1) & tableMask;
                    }
                    table[updatedTableIndex] = entry;
                }
            }
        }

        public Stream<Entry> get() {
            return Arrays.stream(table).filter(Objects::nonNull);
        }
    }

    private static boolean arrayEquals(final long[] a, final long[] b, final int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }

    private static final class MeasurementObject {

        private int MAX;
        private int MIN;

        private long SUM;

        private int REPEAT;

        public MeasurementObject(int MAX, int MIN, long SUM, int REPEAT) {
            this.MAX = MAX;
            this.MIN = MIN;
            this.SUM = SUM;
            this.REPEAT = REPEAT;
        }

        public MeasurementObject() {
            this.MAX = -999;
            this.MIN = 9999;
            this.SUM = 0;
            this.REPEAT = 0;
        }

        public MeasurementObject(int MAX, int MIN, long SUM) {
            this.MAX = MAX;
            this.MIN = MIN;
            this.SUM = SUM;
        }

        public MeasurementObject(int MAX, int MIN) {
            this.MAX = MAX;
            this.MIN = MIN;
        }

        public static MeasurementObject combine(MeasurementObject m1, MeasurementObject m2) {
            var mres = new MeasurementObject();
            mres.MIN = MeasurementObject.min(m1.MIN, m2.MIN);
            mres.MAX = MeasurementObject.max(m1.MAX, m2.MAX);
            mres.SUM = m1.SUM + m2.SUM;
            mres.REPEAT = m1.REPEAT + m2.REPEAT;
            return mres;
        }

        public static MeasurementObject updateWith(MeasurementObject m1, MeasurementObject m2) {
            var mres = new MeasurementObject();
            mres.MIN = MeasurementObject.min(m1.MIN, m2.MIN);
            mres.MAX = MeasurementObject.max(m1.MAX, m2.MAX);
            mres.SUM = m1.SUM + m2.SUM;
            mres.REPEAT = m1.REPEAT + m2.REPEAT;
            return mres;
        }

        public MeasurementObject updateWith(int measurement) {
            MIN = MeasurementObject.min(MIN, measurement);
            MAX = MeasurementObject.max(MAX, measurement);
            SUM += measurement;
            REPEAT++;
            return this;
        }

        private static int max(final int a, final int b) {
            final int diff = a - b;
            final int dsgn = diff >> 31;
            return a - (diff & dsgn);
        }

        private static int min(final int a, final int b) {
            final int diff = a - b;
            final int dsgn = diff >> 31;
            return b + (diff & dsgn);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MeasurementObject that = (MeasurementObject) o;
            return MAX == that.MAX && MIN == that.MIN && REPEAT == that.REPEAT;
        }

        @Override
        public int hashCode() {
            return Objects.hash(MAX, MIN, REPEAT);
        }

        @Override
        public String toString() {
            return round(MIN) + "/" + round((1.0 * SUM) / REPEAT) + "/" + round(MAX);
        }
    }
}