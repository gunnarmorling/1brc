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

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * Results on Mac mini (Apple M2 with 8-core CPU / 8GB unified memory):
 * <pre>
 *   using AIO and multiple threads:
 *     120.15s user 4.33s system 710% cpu 17.522 total
 *
 *   reduce the number of memory copies:
 *      45.87s user 2.82s system 530% cpu  9.185 total
 *
 *   processing byte array backwards and using bitwise operation to find specific byte (inspired by thomaswue):
 *      25.38s user 3.44s system 342% cpu  8.406 total
 * </pre>
 *
 * @author Xylitol
 */
@SuppressWarnings("unchecked")
public class CalculateAverage_C5H12O5 {
    private static final int AVAILABLE_PROCESSOR_NUM = Runtime.getRuntime().availableProcessors();
    private static final int TRANSFER_QUEUE_CAPACITY = 1024 / 16 / AVAILABLE_PROCESSOR_NUM; // 1GB memory max
    private static final int BYTE_BUFFER_CAPACITY = 1024 * 1024 * 16; // 16MB one time
    private static final int EXPECTED_MAPPINGS_NUM = 10000;

    /**
     * Fragment the file into chunks.
     */
    private static long[] fragment(Path path) throws IOException {
        long size = Files.size(path);
        long chunk = size / AVAILABLE_PROCESSOR_NUM;
        List<Long> positions = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            long position = chunk;
            for (int i = 0; i < AVAILABLE_PROCESSOR_NUM - 1; i++) {
                if (position >= size) {
                    break;
                }
                file.seek(position);
                // move the position to the next newline byte
                while (file.read() != '\n') {
                    position++;
                }
                positions.add(++position);
                position += chunk;
            }
        }
        if (positions.isEmpty() || positions.getLast() < size) {
            positions.add(size);
        }
        return positions.stream().mapToLong(Long::longValue).toArray();
    }

    public static void main(String[] args) throws Exception {
        // fragment the input file
        Path path = Path.of("./measurements.txt");
        long[] positions = fragment(path);

        // start the calculation tasks
        FutureTask<Map<Station, MeasurementData>>[] tasks = new FutureTask[positions.length];
        for (int i = 0; i < positions.length; i++) {
            tasks[i] = new FutureTask<>(new Calculator(path, (i == 0 ? 0 : positions[i - 1]), positions[i]));
            new Thread(tasks[i]).start();
        }

        // wait for the results
        Map<Station, MeasurementData> result = HashMap.newHashMap(EXPECTED_MAPPINGS_NUM);
        for (FutureTask<Map<Station, MeasurementData>> task : tasks) {
            task.get().forEach((k, v) -> result.merge(k, v, MeasurementData::merge));
        }

        // sort and print the results
        TreeMap<String, MeasurementData> sorted = new TreeMap<>();
        for (Map.Entry<Station, MeasurementData> entry : result.entrySet()) {
            sorted.put(new String(entry.getKey().bytes, StandardCharsets.UTF_8), entry.getValue());
        }
        System.out.println(sorted);
    }

    /**
     * The calculation task.
     */
    private static class Calculator implements Callable<Map<Station, MeasurementData>> {
        private final TransferQueue<byte[]> transfer = new LinkedTransferQueue<>();
        private final AsynchronousFileChannel asyncChannel;
        private final long limit;
        private long position;

        public Calculator(Path file, long position, long limit) throws IOException {
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            this.asyncChannel = AsynchronousFileChannel.open(file, Set.of(StandardOpenOption.READ), executor);
            this.position = position;
            this.limit = limit;
        }

        @Override
        public Map<Station, MeasurementData> call() throws InterruptedException {
            ByteBuffer buffer = ByteBuffer.allocateDirect(BYTE_BUFFER_CAPACITY);
            asyncChannel.read(buffer, position, buffer, new CompletionHandler<>() {
                @Override
                public void completed(Integer readSize, ByteBuffer buffer) {
                    if (position + readSize >= limit) {
                        buffer.limit(readSize - (int) (position + readSize - limit));
                    }
                    else {
                        for (int i = buffer.position() - 1; i >= 0; i--) {
                            if (buffer.get(i) == '\n') {
                                // truncate the buffer to the last newline byte
                                buffer.limit(i + 1);
                                break;
                            }
                        }
                    }
                    buffer.flip();
                    byte[] bytes = new byte[buffer.limit() + 1];
                    // add a newline byte at the beginning
                    bytes[0] = '\n';
                    buffer.get(bytes, 1, buffer.limit());
                    transfer(bytes);
                    if ((position += buffer.limit()) < limit) {
                        buffer.clear();
                        asyncChannel.read(buffer, position, buffer, this);
                    }
                    else {
                        // stop signal
                        transfer(new byte[0]);
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer buffer) {
                    transfer(new byte[0]);
                }
            });
            return process();
        }

        /**
         * Transfer or put the bytes to the queue.
         */
        private void transfer(byte[] bytes) {
            try {
                if (transfer.size() >= TRANSFER_QUEUE_CAPACITY) {
                    transfer.transfer(bytes);
                }
                else {
                    transfer.put(bytes);
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Take and process the bytes from the queue.
         */
        private Map<Station, MeasurementData> process() throws InterruptedException {
            Map<Station, MeasurementData> result = HashMap.newHashMap(EXPECTED_MAPPINGS_NUM);
            for (byte[] bytes = transfer.take(); bytes.length > 0; bytes = transfer.take()) {
                Station station = new Station(bytes);
                // read the bytes backwards
                for (int position = bytes.length - 2; position >= 1; position--) {

                    // calculate the temperature value
                    int temperature = bytes[position] - '0' + (bytes[position -= 2] - '0') * 10;
                    byte unknownByte = bytes[--position];
                    int semicolon = switch (unknownByte) {
                        case ';' -> position;
                        case '-' -> {
                            temperature = -temperature;
                            yield --position;
                        }
                        default -> {
                            temperature += (unknownByte - '0') * 100;
                            if (bytes[--position] == '-') {
                                temperature = -temperature;
                                --position;
                            }
                            yield position;
                        }
                    };

                    // calculate the station name hash
                    int hash = 1;
                    while (true) {
                        long temp = LineFinder.previousLong(bytes, position);
                        int distance = LineFinder.NATIVE.fromRight(temp);
                        if (distance == 0) {
                            // current byte is '\n'
                            break;
                        }
                        position -= distance;
                        if (distance == 8) {
                            // can't find '\n' in previous 8 bytes
                            hash = 31 * hash + (int) (temp ^ (temp >>> 32));
                            continue;
                        }
                        // clear the redundant bytes
                        temp = LineFinder.NATIVE.clearLeft(temp, distance);
                        hash = 31 * hash + (int) (temp ^ (temp >>> 32));
                    }

                    // merge data to the result map
                    MeasurementData data = result.get(station.slice(hash, position + 1, semicolon));
                    if (data == null) {
                        result.put(station.copy(), new MeasurementData(temperature));
                    } else {
                        data.merge(temperature);
                    }
                }
            }
            return result;
        }
    }

    /**
     * To find the nearest newline byte position in a long.
     */
    private interface LineFinder {
        // choose the implementation according to the native byte order
        LineFinder NATIVE = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? LELineFinder.INST : BELineFinder.INST;

        Unsafe UNSAFE = initUnsafe();
        int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        int LONG_BYTES = Long.SIZE / Byte.SIZE;

        static Unsafe initUnsafe() {
            try {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(Unsafe.class);
            }
            catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        static long previousLong(byte[] bytes, long offset) {
            return UNSAFE.getLong(bytes, BYTE_ARRAY_BASE_OFFSET + offset + 1 - LONG_BYTES);
        }

        /**
         * Mark the highest bit of newline byte (0x0A) to 1.
         */
        static long markHighestBit(long longBytes) {
            long temp = longBytes ^ 0x0A0A0A0A0A0A0A0AL;
            return (temp - 0x0101010101010101L) & ~temp & 0x8080808080808080L;
        }

        /**
         * Find the nearest newline byte position from right to left.
         */
        int fromRight(long longBytes);

        /**
         * Clear the left bytes out of the range.
         */
        long clearLeft(long longBytes, int keepNum);

        enum LELineFinder implements LineFinder {
            INST;

            private static final long[] MASKS = new long[8];

            static {
                for (int i = 1; i <= 7; i++) {
                    MASKS[i] = 0xFFFFFFFFFFFFFFFFL << ((8 - i) << 3);
                }
            }

            @Override
            public int fromRight(long longBytes) {
                return Long.numberOfLeadingZeros(markHighestBit(longBytes)) >>> 3;
            }

            @Override
            public long clearLeft(long longBytes, int keepNum) {
                return longBytes & MASKS[keepNum];
            }
        }

        enum BELineFinder implements LineFinder {
            INST;

            private static final long[] MASKS = new long[8];

            static {
                for (int i = 1; i <= 7; i++) {
                    MASKS[i] = 0xFFFFFFFFFFFFFFFFL >>> ((8 - i) << 3);
                }
            }

            @Override
            public int fromRight(long longBytes) {
                return Long.numberOfTrailingZeros(markHighestBit(longBytes)) >>> 3;
            }

            @Override
            public long clearLeft(long longBytes, int keepNum) {
                return longBytes & MASKS[keepNum];
            }
        }
    }

    /**
     * The station name wrapper ( bytes[from, to) ).
     */
    private static class Station {
        private final byte[] bytes;
        private int from;
        private int to;
        private int hash;

        public Station(byte[] bytes) {
            this(bytes, 0, 0, 0);
        }

        public Station(byte[] bytes, int hash, int from, int to) {
            this.bytes = bytes;
            this.slice(hash, from, to);
        }

        public Station slice(int hash, int from, int to) {
            this.hash = hash;
            this.from = from;
            this.to = to;
            return this;
        }

        public Station copy() {
            int length = to - from;
            byte[] newBytes = new byte[length];
            System.arraycopy(bytes, from, newBytes, 0, length);
            return new Station(newBytes, hash, 0, length);
        }

        @Override
        public boolean equals(Object station) {
            Station other = (Station) station;
            return Arrays.equals(bytes, from, to, other.bytes, other.from, other.to);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * The measurement data wrapper ( temperature * 10 ).
     */
    private static class MeasurementData {
        private int min;
        private int max;
        private long sum;
        private int count;

        public MeasurementData(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        public MeasurementData merge(int value) {
            return merge(value, value, value, 1);
        }

        public MeasurementData merge(MeasurementData other) {
            return merge(other.min, other.max, other.sum, other.count);
        }

        public MeasurementData merge(int min, int max, long sum, int count) {
            this.min = Math.min(this.min, min);
            this.max = Math.max(this.max, max);
            this.sum += sum;
            this.count += count;
            return this;
        }

        @Override
        public String toString() {
            return STR."\{min / 10.0}/\{Math.round((double) sum / count) / 10.0}/\{max / 10.0}";
        }
    }
}
