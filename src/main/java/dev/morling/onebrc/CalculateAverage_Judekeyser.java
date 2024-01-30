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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_Judekeyser {
    private static final String FILE = "./measurements.txt";
    private static final int chunkSize = (1 << 7) << 13; // This can't go beyond 2^21, because otherwise we might exceed int capacity

    private static final int numberOfIOWorkers = 1 << 11; // We are going to need (numberOfIOWorkers-1) * chunkSize capacity
    private static final int numberOfParallelWorkers = 2 * Runtime.getRuntime().availableProcessors() - 1;

    public static void main(String[] args) throws Exception {
        class SimpleStatistics {
            int min, max, sum, count;
            SimpleStatistics() {
                min = Integer.MAX_VALUE;
                max = Integer.MIN_VALUE;
                sum = 0;
                count = 0;
            }

            void accept(int value) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                count++;
            }
        }
        class Statistics {
            double min, max, avg;
            long count;
            Statistics() {
                min = Double.MAX_VALUE;
                max = Double.MIN_VALUE;
                avg = 0.;
                count = 0L;
            }

            void accept(SimpleStatistics simple) {
                min = Math.min(min, simple.min/10.);
                max = Math.max(max, simple.max/10.);
                var nextCount = count + simple.count;
                avg = (avg * count + simple.sum/10.)/nextCount;
                count = nextCount;
            }

            static final DecimalFormat format;
            static {
                var decimalFormatSymbols = DecimalFormatSymbols.getInstance();
                decimalFormatSymbols.setDecimalSeparator('.');
                format = new DecimalFormat("#0.0", decimalFormatSymbols);
            }
            @Override
            public String toString() {
                return STR."\{format.format(min)}/\{format.format(avg)}/\{format.format(max)}";
            }
        }
        class Name {
            final byte[] data;
            final int hash;
            Name(byte[] data) {
                this.data = data;
                {
                    var hash = 0;
                    for (var d : data) {
                        hash = 31 * hash + d;
                    }
                    this.hash = hash;
                }
            }

            @Override
            public int hashCode() {
                return hash;
            }

            @Override
            public boolean equals(Object obj) {
                if(obj == this) return true;
                else if(obj instanceof Name name && name.data.length == data.length) {
                    for(int i = 0; i < data.length; i++) {
                        if(data[i] != name.data[i]) {
                            return false;
                        }
                    }
                    return true;
                } else return false;
            }

            @Override
            public String toString() {
                return new String(data, StandardCharsets.UTF_8);
            }
        }

        record Line(Name name, int value) {}

        var results = new HashMap<Name, Statistics>();
        try(var file = new RandomAccessFile(Paths.get(FILE).toFile(), "r")) {
            class Ls implements Iterator<MemorySegment> {
                final int M = chunkSize;
                final Arena arena = Arena.ofShared();
                final long length;

                long offset;

                Ls() throws IOException {
                    offset = 0L;
                    length = file.length();
                }

                @Override
                public MemorySegment next() {
                    MemorySegment memorySegment;
                    try {
                        memorySegment = file.getChannel().map(
                                FileChannel.MapMode.READ_ONLY,
                                offset, Math.min(M + 128L, file.getChannel().size() - offset),
                                arena
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    var size = M;
                    if (offset + M < length) {
                        b:
                        {
                            for (int N = 0; N < 128; N++) {
                                var b = memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, size);
                                size += 1;
                                if (b == '\n') {
                                    break b;
                                }
                            }
                            assert false : "Lines are smaller than 128 bytes";
                        }
                        offset += size;
                    } else {
                        size = (int) (length - offset);
                        offset = length;
                    }

                    return memorySegment.asSlice(0, size);
                }

                @Override
                public boolean hasNext() {
                    return offset < length;
                }
            }

            class It implements Iterator<Line> {
                int offset;
                final int length;
                final MemorySegment memorySegment;

                final VectorSpecies<Byte> preferred;
                final ByteOrder endian;

                It(MemorySegment memorySegment) {
                    offset = 0;
                    preferred = ByteVector.SPECIES_PREFERRED;
                    endian = ByteOrder.nativeOrder();
                    this.memorySegment = memorySegment;
                    length = (int) memorySegment.byteSize();
                    assert '\n' == memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, length - 1);
                }

                @Override
                public boolean hasNext() {
                    return offset < length;
                }

                @Override
                public Line next() {
                    int size;
                    var chunk = new byte[128];
                    b: {
                        /*
                         * Vectorization does not seem to bring anything interesting.
                         * This is a bit disappointing. What am I doing wrong?
                         */
                        size = 0;

                        while (offset + preferred.length() <= length) {
                            var vector = ByteVector.fromMemorySegment(
                                    preferred, memorySegment,
                                    offset + size, endian
                            );
                            vector.intoArray(chunk, size);
                            var j = vector.eq((byte) '\n').firstTrue();
                            if (j < preferred.length()) {
                                assert j >= 0;
                                size += j;
                                assert memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, offset + size) == '\n';
                                break b;
                            } else {
                                assert j == preferred.length();
                                size += preferred.length();
                            }
                        }
                        {
                            byte b;
                            for (; size < 128; size++) {
                                b = memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, offset + size);
                                chunk[size] = b;
                                if (b == '\n') break b;
                            }
                            assert false : "Lines are smaller than 128 bytes";
                        }
                        assert memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, offset + size) == '\n';
                        assert size < 128;
                    }

                    Name name;
                    int value;
                    {
                        int cursor = size - 1;
                        {
                            value = chunk[cursor] - '0';
                            value += (chunk[cursor - 2] - '0') * 10;
                            cursor -= 3;
                            if (chunk[cursor] == '-') {
                                value *= -1;
                                cursor -= 1;
                            } else if (chunk[cursor] != ';') {
                                value += (chunk[cursor] - '0') * 100;
                                cursor -= 1;
                                if (chunk[cursor] == '-') {
                                    value *= -1;
                                    cursor -= 1;
                                }
                            }
                        }
                        var data = new byte[cursor];
                        System.arraycopy(chunk, 0, data, 0, data.length);
                        assert ';' != data[data.length - 1];
                        name = new Name(data);
                    }
                    /*
                    var stack = new byte[size];
                    //memorySegment.asSlice(offset, size).asByteBuffer().get(stack);
                    System.arraycopy(chunk, 0, stack, 0, stack.length);
                    assert '\n' != stack[size - 1];
                    offset += size + 1;
                    return stack;

                     */
                    offset += size + 1;
                    return new Line(name, value);
                }
            }

            record Pair(MemorySegment segment, Map<Name, SimpleStatistics> simple) {
                Pair(MemorySegment segment) {
                    this(segment, apply(segment));
                }

                private static Map<Name, SimpleStatistics> apply(MemorySegment memorySegment) {
                    try {
                        return call(memorySegment);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                private static Map<Name, SimpleStatistics> call(MemorySegment memorySegment) throws IOException {
                    var it = new It(memorySegment);
                    var simple = new HashMap<Name, SimpleStatistics>();
                    while (it.hasNext()) {
                        var line = it.next();
                        var name = line.name();
                        var value = line.value();

                        var statistics = simple.get(name);
                        if (statistics == null) {
                            statistics = new SimpleStatistics();
                            simple.put(name, statistics);
                        }
                        statistics.accept(value);
                    }
                    return simple;
                }
            }


            var ls = new Ls();

            try(
                    var nioService = Executors.newVirtualThreadPerTaskExecutor();
                    var parallelService =Executors.newFixedThreadPool(numberOfParallelWorkers)
            ) {
                var tasksQueue = new ArrayList<Future<Pair>>();
                for(;;) {
                    assert tasksQueue.size() <= numberOfIOWorkers;
                    if(tasksQueue.size() < numberOfIOWorkers) {
                        if(ls.hasNext()) {
                            var memseg = ls.next();
                            var task = CompletableFuture.supplyAsync(
                                    () -> {
                                        memseg.load();
                                        return memseg;
                                    }, nioService
                            ).thenApplyAsync(Pair::new, parallelService);

                            tasksQueue.add(task);
                        } else if(tasksQueue.isEmpty()) break;
                    }
                    /*
                     * Wait for the tasks and merge what's ready
                     */
                    {
                        var copy = new ArrayList<Future<Pair>>(tasksQueue.size());
                        for(var worker: tasksQueue) {
                            if(worker.isDone()) {
                                /*
                                 * Merge the maps
                                 */
                                var p = worker.get();
                                var simple = p.simple();
                                p.segment().unload();
                                for (var entry : simple.entrySet()) {
                                    var name = entry.getKey();

                                    var statistics = results.get(name);
                                    if (statistics == null) {
                                        statistics = new Statistics();
                                        results.put(name, statistics);
                                    }
                                    statistics.accept(entry.getValue());
                                }
                            } else copy.add(worker);
                        }
                        tasksQueue.clear();
                        tasksQueue.addAll(copy);
                    }
                }
            }
        }

        /*
         * Print
         */
        {
            var sortedMap = new TreeMap<String, Statistics>();
            for(var entry: results.entrySet()) {
                sortedMap.put(
                        entry.getKey().toString(),
                        entry.getValue()
                );
            }
            var joiner = new StringJoiner(", ", "{", "}");
            for (var entry : sortedMap.entrySet()) {
                joiner.add(STR. "\{ entry.getKey() }=\{ entry.getValue() }" );
            }
            System.out.println(joiner);
        }
    }
}
