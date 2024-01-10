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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CalculateAverage_AbstractKamen {

    private static final String FILE = "./measurements.txt";

    private static class Measurement {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum;
        private long count;

        public String toString() {
            return round(min / 10.0) + "/" + round(sum / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        try (final FileChannel fc = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
                final RandomAccessFile raf = new RandomAccessFile(new File(FILE), "r")) {
            final Map<String, Measurement> res = getParallelBufferStream(raf, fc)
                    .map(CalculateAverage_AbstractKamen::getMeasurements)
                    .flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.collectingAndThen(
                            Collectors.toMap(Map.Entry::getKey,
                                    Map.Entry::getValue,
                                    CalculateAverage_AbstractKamen::aggregateMeasurements),
                            TreeMap::new));
            System.out.println(res);
        }
    }

    private static Measurement aggregateMeasurements(Measurement src, Measurement target) {
        target.min = Math.min(src.min, target.min);
        target.max = Math.max(src.max, target.max);
        target.sum = src.sum + target.sum;
        target.count = src.count + target.count;
        return target;
    }

    private static Map<String, Measurement> getMeasurements(BufferSupplier getBuffer) {
        final Map<String, Measurement> map = new HashMap<>(50_000);
        final ByteBuffer byteBuffer = getBuffer.get();
        final byte[] bytes = new byte[512];
        while (byteBuffer.hasRemaining()) {
            int nameLen = 0;
            String name;
            byte b;
            while ((b = byteBuffer.get()) != ';') {
                bytes[nameLen++] = b;
            }
            name = new String(bytes, 0, nameLen, StandardCharsets.UTF_8);
            int valueLen = 0;
            int neg = 1;
            while (byteBuffer.hasRemaining() && ((b = byteBuffer.get()) != '\n')) {
                if (b == '-') {
                    neg = -1;
                }
                else if (b == '.' || b == '\r') {
                    // skip the dot and retart char
                }
                else {
                    bytes[valueLen++] = b;
                }
            }
            final int val = parseAsInt(valueLen, bytes);
            takeMeasurement(val * neg, map, name);
        }
        return map;
    }

    private static int parseAsInt(int valueLen, byte[] bytes) {
        int val;
        switch (valueLen) {
            case 2 -> val = (bytes[0] - 48) * 10 + (bytes[1] - 48);
            case 3 -> val = (bytes[0] - 48) * 100 + (bytes[1] - 48) * 10 + (bytes[2] - 48);
            default -> val = 0;
        }
        return val;
    }

    private static void takeMeasurement(int temperature, Map<String, Measurement> map, String name) {
        Measurement measurement = map.get(name);
        if (measurement != null) {
            measurement.min = Math.min(measurement.min, temperature);
            measurement.max = Math.max(measurement.max, temperature);
            measurement.sum += temperature;
            measurement.count++;
        }
        else {
            measurement = new Measurement();
            map.put(name, measurement);
            measurement.min = temperature;
            measurement.max = temperature;
            measurement.sum = temperature;
            measurement.count = 1;
        }
    }

    private static Stream<BufferSupplier> getParallelBufferStream(RandomAccessFile raf, FileChannel fc) throws IOException {
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        return StreamSupport.stream(
                StreamSupport.stream(
                        Spliterators.spliterator(
                                new BufferSupplierIterator(raf, fc, availableProcessors), availableProcessors,
                                Spliterator.IMMUTABLE | Spliterator.SIZED | Spliterator.SUBSIZED),
                        false)
                        .spliterator(),
                true);
    }

}

interface BufferSupplier extends Supplier<ByteBuffer> {
}

class BufferSupplierIterator implements Iterator<BufferSupplier> {
    private long start;
    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final long fileLength;
    private final long chunkSize;

    public BufferSupplierIterator(RandomAccessFile raf, FileChannel fc, int numberOfParts) throws IOException {
        this.raf = raf;
        this.fc = fc;
        this.fileLength = fc.size();
        this.chunkSize = Math.min(fileLength / numberOfParts, 1073741824);
    }

    @Override
    public boolean hasNext() {
        return start < fileLength;
    }

    @Override
    public BufferSupplier next() {
        try {
            if (hasNext()) {
                final long end = getEnd();
                long s = start;
                this.start = end;
                return getBufferSupplier(s, end);
            }
            else {
                throw new NoSuchElementException();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long getEnd() throws IOException {
        long end = Math.min(start + chunkSize, fileLength);
        while (end < fileLength) {
            raf.seek(end++);
            if (raf.read() == '\n')
                break;
        }
        return end;
    }

    private BufferSupplier getBufferSupplier(long position, long end) {
        final long size = end - position;
        return new BufferSupplier() {

            private ByteBuffer bb;

            @Override
            public ByteBuffer get() {
                try {
                    if (bb == null) {
                        return (bb = fc.map(MapMode.READ_ONLY, position, size));
                    }
                    else {
                        return bb;
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}