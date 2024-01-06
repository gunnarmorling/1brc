package dev.morling.onebrc;

import java.io.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

public class CalculateAverage_raipc {
    private static final String FILE = "./measurements.txt";
    private static final int BUFFER_SIZE = 16 * 1024;

    private static final class AggregatedMeasurement {
        private final ByteArrayWrapper station;
        private String stationStringCached;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int count;
        private long sum;

        private AggregatedMeasurement(ByteArrayWrapper station) {
            this.station = station;
        }

        public String station() {
            String s = this.stationStringCached;
            if (s == null) {
                this.stationStringCached = s = station.toString();
            }
            return s;
        }

        public AggregatedMeasurement merge(AggregatedMeasurement other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        public void add(int value) {
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
            this.sum += value;
            this.count++;
        }

        public void format(StringBuilder out) {
            out.append(station()).append('=')
                    .append(min / 10.0).append('/')
                    .append(Math.round(1.0 * sum / count) / 10.0).append('/')
                    .append(max / 10.0);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        File inputFile = new File(FILE);
        ParsingTask parsingTask = new ParsingTask(inputFile, 0, inputFile.length());
        AggregatedMeasurement[] results = parsingTask.fork().join().values().toArray(AggregatedMeasurement[]::new);
        System.out.println(formatResult(results));
    }

    private static String formatResult(AggregatedMeasurement[] result) {
        Arrays.sort(result, Comparator.comparing(AggregatedMeasurement::station));
        StringBuilder out = new StringBuilder().append('{');
        for (AggregatedMeasurement item : result) {
            item.format(out);
            out.append(", ");
        }
        out.setLength(out.length() - 2);
        return out.append('}').toString();
    }

    private static class ParsingTask extends RecursiveTask<Map<ByteArrayWrapper, AggregatedMeasurement>> {
        private static final int SPLIT_FACTOR = 10;
        private final File file;
        private final long startPosition;
        private final long endPosition;

        private ParsingTask(File file, long startPosition, long endPosition) {
            this.file = file;
            this.startPosition = startPosition;
            this.endPosition = endPosition;
        }

        @Override
        protected Map<ByteArrayWrapper, AggregatedMeasurement> compute() {
            long size = endPosition - startPosition;
            if (size <= BUFFER_SIZE || size < +file.length() / Runtime.getRuntime().availableProcessors() / SPLIT_FACTOR) {
                return doCompute();
            }
            var firstHalf = new ParsingTask(file, startPosition, (startPosition + endPosition) / 2).fork();
            var secondHalf = new ParsingTask(file, (startPosition + endPosition) / 2, endPosition).fork();
            var firstHalfResults = firstHalf.join();
            var secondHalfResults = secondHalf.join();
            secondHalfResults.forEach((k, v) -> {
                firstHalfResults.merge(k, v, AggregatedMeasurement::merge);
            });
            return firstHalfResults;
        }

        private Map<ByteArrayWrapper, AggregatedMeasurement> doCompute() {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                return new ParsingRoutine(startPosition, endPosition).parse(raf);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class ParsingRoutine {
        private final MyHashMap result = new MyHashMap();
        private final byte[] partialContentBuff = new byte[128];
        private final ByteArrayWrapper reusableWrapper = new ByteArrayWrapper(partialContentBuff, 0, 0, 0);
        private int partialSize;
        private final long startPosition;
        private final long endPosition;

        private ParsingRoutine(long startPosition, long endPosition) {
            this.startPosition = startPosition;
            this.endPosition = endPosition;
        }

        private boolean hasPartialContent() {
            return partialSize > 0;
        }

        MyHashMap parse(RandomAccessFile raf) throws IOException {
            byte[] buffer = new byte[BUFFER_SIZE];
            long offset = findStartOffset(raf, buffer);
            boolean readMore = offset <= endPosition;
            while (readMore) {
                raf.seek(offset);
                int length = raf.read(buffer);
                if (length == -1) {
                    break;
                }
                int bufStart = 0;
                if (hasPartialContent()) {
                    int idxOfLf = indexOf(buffer, '\n', 0, length);
                    if (idxOfLf >= 0) {
                        bufStart = idxOfLf + 1;
                        System.arraycopy(buffer, 0, partialContentBuff, partialSize, bufStart);
                        int toProcess = partialSize + bufStart;
                        partialSize = 0;
                        processPart(offset - toProcess, partialContentBuff, 0, toProcess);
                    }
                    else {
                        System.arraycopy(buffer, 0, partialContentBuff, partialSize, bufStart);
                        offset += length;
                        continue;
                    }
                }
                readMore = processPart(offset, buffer, bufStart, length);
                offset += length;
            }
            return result;
        }

        long findStartOffset(RandomAccessFile raf, byte[] buffer) throws IOException {
            if (startPosition == 0) {
                return 0;
            }
            long offset = startPosition - 1;
            int length = 0;
            int idxOfLf = -1;
            while (offset < endPosition) {
                raf.seek(offset);
                length = raf.read(buffer);
                if (length == -1) {
                    throw new IllegalStateException("No content read on position " + offset);
                }
                offset += length;
                idxOfLf = indexOf(buffer, '\n', 0, length);
                if (idxOfLf >= 0) {
                    break;
                }
            }
            if (offset > startPosition) {
                int start = idxOfLf + 1;
                processPart(offset + start - length, buffer, start, length);
            }
            return offset;
        }

        boolean processPart(long position, byte[] buf, int start, int end) {
            ByteArrayWrapper key = reusableWrapper;
            key.content = buf;
            while (position <= endPosition) {
                int idxOfSemicolon = indexOf(buf, ';', start, end);
                if (idxOfSemicolon >= 0) {
                    int idxOfLf = indexOf(buf, '\n', idxOfSemicolon + 2, Math.max(idxOfSemicolon + 2, end));
                    if (idxOfLf >= 0) {
                        key.start = start;
                        key.length = idxOfSemicolon - start;
                        var aggregatedMeasurement = result.getOrCreate(key);
                        aggregatedMeasurement.add(parseInt(buf, idxOfSemicolon + 1, idxOfLf - 1));
                        int prevStart = start;
                        start = idxOfLf + 1;
                        position += start - prevStart;
                        continue;
                    }
                }
                partialSize = end - start;
                if (partialSize > 0) {
                    System.arraycopy(buf, start, partialContentBuff, 0, partialSize);
                }
                break;
            }
            return hasPartialContent() || position < endPosition;
        }

        private int parseInt(byte[] buf, int from, int toIncl) {
            int mul = 1;
            if (buf[from] == '-') {
                mul = -1;
                ++from;
            }
            int res = buf[toIncl] - '0';
            int dec = 10;
            for (int i = toIncl - 2; i >= from; --i) {
                res += (buf[i] - '0') * dec;
                dec *= 10;
            }
            return mul * res;
        }
    }

    private static final MethodHandle indexOfMH;
    private static final MethodHandle vectorizedHashCodeMH;

    static {
        try {
            Class<?> stringLatin1 = Class.forName("java.lang.StringLatin1");
            var lookup = MethodHandles.privateLookupIn(stringLatin1, MethodHandles.lookup());
            // int indexOf(byte[] value, int ch, int fromIndex, int toIndex)
            indexOfMH = lookup.findStatic(stringLatin1, "indexOf",
                    MethodType.methodType(int.class, byte[].class, int.class, int.class, int.class));

            Class<?> arraysSupport = Class.forName("jdk.internal.util.ArraysSupport");
            lookup = MethodHandles.privateLookupIn(arraysSupport, MethodHandles.lookup());
            // int vectorizedHashCode(Object array, int fromIndex, int length, int initialValue, int basicType)
            vectorizedHashCodeMH = lookup.findStatic(arraysSupport, "vectorizedHashCode",
                    MethodType.methodType(int.class, Object.class, int.class, int.class, int.class, int.class));
        }
        catch (Exception e) {
            throw new Error(e);
        }
    }

    static int indexOf(byte[] src, int ch, int fromIndex, int toIndex) {
        try {
            return (int) indexOfMH.invoke(src, ch, fromIndex, toIndex);
        }
        catch (Throwable e) {
            throw new Error(e);
        }
    }

    static int vectorizedHashCode(byte[] array, int fromIndex, int length) {
        try {
            return (int) vectorizedHashCodeMH.invoke((Object) array, fromIndex, length, 0, 8);
        }
        catch (Throwable e) {
            throw new Error(e);
        }

    }

    private static class ByteArrayWrapper {
        private byte[] content;
        private int start;
        private int length;
        int hashCodeCached;

        ByteArrayWrapper(byte[] content, int start, int length, int hashCodeCached) {
            this.content = content;
            this.start = start;
            this.length = length;
            this.hashCodeCached = hashCodeCached;
        }

        void calculateHashCode() {
            this.hashCodeCached = vectorizedHashCode(content, start, length);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ByteArrayWrapper bw &&
                    Arrays.equals(content, start, start + length, bw.content, bw.start, bw.start + bw.length);
        }

        @Override
        public int hashCode() {
            return hashCodeCached;
        }

        @Override
        public String toString() {
            return new String(content, start, length, StandardCharsets.UTF_8);
        }

        ByteArrayWrapper copy() {
            byte[] copy = Arrays.copyOfRange(content, start, start + length);
            return new ByteArrayWrapper(copy, 0, copy.length, hashCodeCached);
        }

    }

    private static class MyHashMap extends HashMap<ByteArrayWrapper, AggregatedMeasurement> {
        AggregatedMeasurement getOrCreate(ByteArrayWrapper key) {
            key.calculateHashCode();
            var aggregatedMeasurement = get(key);
            if (aggregatedMeasurement == null) {
                var keyCopy = key.copy();
                put(keyCopy, aggregatedMeasurement = new AggregatedMeasurement(keyCopy));
            }
            return aggregatedMeasurement;
        }
    }

}
