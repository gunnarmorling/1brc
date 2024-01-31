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

import static java.lang.foreign.ValueLayout.OfByte.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.OfByte.JAVA_INT_UNALIGNED;

public class CalculateAverage_Judekeyser {
    private static final String FILE = "./measurements.txt";
    private static final int chunkSize = (1 << 7) << 12; // This can't go beyond 2^21, because otherwise we might exceed int capacity

    private static final int numberOfIOWorkers = 1 << 8; // We are going to need (numberOfIOWorkers-1) * chunkSize capacity
    private static final int numberOfParallelWorkers = Runtime.getRuntime().availableProcessors() - 1;

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

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
            Statistics(SimpleStatistics simple) {
                min = simple.min/10.;
                max = simple.max/10.;
                avg = simple.sum/10./simple.count;
                count = simple.count;
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
                return STR."\{format.format(round(min))}/\{format.format(round(avg))}/\{format.format(round(max))}";
            }

            static double round(double d) {
                return Math.round(d*10.)/10.;
            }
        }
        class Name {
            final int[] data;
            final int hash;
            Name(int[] data) {
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
                    int size  = 0;
                    while(size < data.length) {
                        if(data[size] != name.data[size]) {
                            return false;
                        } else size++;
                    }
                    return true;
                } else return false;
            }

            @Override
            public String toString() {
                var bdata = new byte[data.length * 4];
                int j = 0;
                for(int i = 0;i < data.length; i++) {
                    bdata[j++] = (byte)((data[i] >>>  0) & 255);
                    bdata[j++] = (byte)((data[i] >>>  8) & 255);
                    bdata[j++] = (byte)((data[i] >>> 16) & 255);
                    bdata[j++] = (byte)((data[i] >>> 24) & 255);
                }
                while(bdata[--j] == 0);
                return new String(bdata, 0, j+1, StandardCharsets.UTF_8);
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
                                var b = memorySegment.get(JAVA_BYTE, size);
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
                final ByteOrder endian;

                It(MemorySegment memorySegment) {
                    offset = 0;
                    endian = ByteOrder.nativeOrder();
                    this.memorySegment = memorySegment;
                    length = (int) memorySegment.byteSize();
                    assert '\n' == memorySegment.get(JAVA_BYTE, length - 1);
                }

                @Override
                public boolean hasNext() {
                    return offset < length;
                }

                @Override
                public Line next() {
                    int size;
                    b: {
                        /*
                         * Vectorization does not seem to bring anything interesting.
                         * This is a bit disappointing. What am I doing wrong?
                         */

                        size = 0;

                        while (offset+size+SPECIES.length() <= length) {
                            var vector = ByteVector.fromMemorySegment(
                                    SPECIES, memorySegment,
                                    offset+size, endian
                            );
                            var j = vector.eq((byte) '\n').firstTrue();
                            if (j < SPECIES.length()) {
                                assert j >= 0;
                                size += j;
                                assert memorySegment.get(JAVA_BYTE, offset+size) == '\n';
                                break b;
                            } else {
                                assert j == SPECIES.length();
                                size += SPECIES.length();
                            }
                        }
                        {
                            byte b;
                            for (; size < 128; size++) {
                                b = memorySegment.get(JAVA_BYTE, offset+size);
                                if (b == '\n') break b;
                            }
                            assert false : "Lines are smaller than 128 bytes";
                        }
                        assert memorySegment.get(JAVA_BYTE, offset+size) == '\n';
                        assert size < 128;
                    }

                    Name name;
                    int value;
                    {
                        long cursor = offset+size - 1L;
                        {
                            value = memorySegment.get(JAVA_BYTE, cursor) - '0';
                            value += (memorySegment.get(JAVA_BYTE, cursor-2L) - '0') * 10;
                            cursor -= 3L;
                            if (memorySegment.get(JAVA_BYTE, cursor) == '-') {
                                value *= -1;
                                cursor -= 1L;
                            } else if (memorySegment.get(JAVA_BYTE, cursor) != ';') {
                                value += (memorySegment.get(JAVA_BYTE, cursor) - '0') * 100;
                                cursor -= 1L;
                                if (memorySegment.get(JAVA_BYTE, cursor) == '-') {
                                    value *= -1;
                                    cursor -= 1L;
                                }
                            }
                        }
                        //var data = memorySegment.asSlice(offset, cursor-offset).toArray(JAVA_BYTE);
                        //System.arraycopy(chunk, 0, data, 0, data.length);
                        //assert ';' != data[data.length - 1];
                        //name = new Name(data);
                        {
                            int mod4StringSize = ((int)(cursor-offset+3))/4 * 4;
                            var data = memorySegment.asSlice(offset, mod4StringSize).toArray(JAVA_INT_UNALIGNED);
                            switch(((int)(cursor - offset)) % 4) {
                                case 0: break;
                                case 1: {
                                    data[data.length - 1] &= 255;
                                } break;
                                case 2: {
                                    data[data.length - 1] &= 65535;
                                } break;
                                case 3: {
                                    data[data.length - 1] &= 16777215;
                                } break;
                            }
                            name = new Name(data);
                        }
                    }
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
                                        statistics = new Statistics(entry.getValue());
                                        results.put(name, statistics);
                                    } else {
                                        statistics.accept(entry.getValue());
                                    }
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
