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
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CalculateAverage_michaljonko {

    private static final String FILE = "./measurements.txt";
    private static final Path PATH = Paths.get(FILE);

    public static void main(String[] args) throws IOException {
        final var cache = new ParsingCache();
        try (var lines = Files.lines(PATH).limit(10_000)) {
            lines
                    .map(line -> line.split(";")[1])
                    .map(cache::parseIfAbsent)
                    .forEach(System.out::println);
        }

        System.out.println("cpus:" + Runtime.getRuntime().availableProcessors());
        System.out.println("file size:" + Files.size(PATH));
        System.out.println("partition size:~" + Files.size(PATH) / Runtime.getRuntime().availableProcessors());

        // try (var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
        // Arena arena = Arena.ofShared();) {
        // final var memorySegment = channel.map(MapMode.READ_ONLY, 0, Files.size(PATH), arena);
        // final var mappedByteBuffer = channel.map(MapMode.READ_ONLY, 0, Files.size(PATH));
        // final var decoder = StandardCharsets.UTF_8.newDecoder();
        // final var charBuffer = decoder.decode(mappedByteBuffer);
        // }
    }

    static final class FilePartitioner {

        public Collection<MappedFile> partitionsByAvailableCpus(Path path) {
            try {
                final var size = Files.size(path);
                final var partitions = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

                if(partitions < 2){
                    return List.of(new MappedFile(PATH, 0, size));
                }

                final var partitionSize = size / partitions;

//                try (var channel = Files.newByteChannel(PATH, StandardOpenOption.READ)) {
//                    var position = partitionSize;
//                    FileChannel.open(PATH, StandardOpenOption.READ).position(position).
//                    channel.position(position);
//                }
                return List.of();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static final class MappedFile implements AutoCloseable {

        private final FileChannel channel;
        private final Arena arena;
        private volatile MemorySegment memorySegment;
        private final long position;
        private final long size;
        private final AtomicBoolean mapped = new AtomicBoolean(false);

        public MappedFile(Path path, long position, long size) throws IOException {
            this.position = position;
            this.size = size;
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.arena = Arena.ofShared();
        }

        public MemorySegment memorySegment() throws IOException {
            return (memorySegment ==null && mapped.compareAndSet(false, true))
                    ? (memorySegment = channel.map(MapMode.READ_ONLY, position, size, arena))
                    : memorySegment;
        }

        @Override
        public void close() throws Exception {
            arena.close();
            channel.close();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MappedFile that = (MappedFile) o;
            return channel.equals(that.channel) && arena.equals(that.arena);
        }

        @Override
        public int hashCode() {
            return Objects.hash(channel, arena);
        }
    }

    public static final class ParsingCache {

        private final ConcurrentHashMap<String, Double> cache = new ConcurrentHashMap<>(10_000);

        private static double parseRawDouble(String rawValue) {
            if (rawValue == null) {
                return Double.NaN;
            }
            final var rawValueArray = rawValue.toCharArray();
            if (rawValueArray.length == 0) {
                return Double.NaN;
            }

            final var sign = (rawValueArray[0] - '-') == 0 ? -1 : 1;
            final var arrayBeginning = sign > 0 ? 0 : 1;
            var separatorIndex = -1;
            var integer = 0;

            for (int index = rawValueArray.length - 1, factor = 1; index >= arrayBeginning; index--) {
                if (rawValueArray[index] == '.') {
                    separatorIndex = index;
                }
                else {
                    integer += (rawValueArray[index] - '0') * factor;
                    factor *= 10;
                }
            }

            if (separatorIndex < 0) {
                return sign * integer;
            }

            var div = 1;
            while (rawValueArray.length - (++separatorIndex) > 0) {
                div *= 10;
            }

            return 1.0d * sign * integer / div;
        }

        public double parseIfAbsent(String rawValue) {
            return cache.computeIfAbsent(rawValue, ParsingCache::parseRawDouble);
        }
    }
}
