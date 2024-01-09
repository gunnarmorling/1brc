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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.Consumer;

public class CalculateAverage_vaidhy {

    private final FileService fileService;
    private final Consumer<String> lineConsumer;

    interface FileService {
        /**
         * Returns the size of the file in number of characters.
         * (Extra credit: assume byte size instead of char size)
         */
        // Possible implementation for byte case in HTTP:
        // byte size = Content-Length header. using HEAD or empty Range.
        int length();

        /**
         * Returns substring of the file from character indices.
         * Expects 0 <= start <= start + length <= fileSize
         * (Extra credit: assume sub-byte array instead of sub char array)
         */
        // Possible implementation for byte case in HTTP:
        // Using Http Request header "Range", typically used for continuing
        // partial downloads.
        byte[] range(int offset, int length);

    }

    public CalculateAverage_vaidhy(FileService fileService,
                                   Consumer<String> lineConsumer) {
        this.fileService = fileService;
        this.lineConsumer = lineConsumer;
    }

    /// SAMPLE CANDIDATE CODE STARTS

    /**
     * Reads from a given offset till the end, it calls server in
     * blocks of scanSize whenever cursor passes the current block.
     * Typically when hasNext() is called. hasNext() is efficient
     * in the sense calling second time is cheap if next() is not
     * called in between. Cheap in the sense no call to server is
     * made.
     */
    // Space complexity = O(scanSize)
    static class CharStream implements Iterator<Character> {

        private final FileService fileService;
        private int offset;
        private final int scanSize;
        private int index = 0;
        private String currentChunk = "";
        private final int fileLength;

        public CharStream(FileService fileService, int offset, int scanSize) {
            this.fileService = fileService;
            this.offset = offset;
            this.scanSize = scanSize;
            this.fileLength = fileService.length();
            if (scanSize <= 0) {
                throw new IllegalArgumentException("scan size must be > 0");
            }
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
        }

        @Override
        public boolean hasNext() {
            while (index >= currentChunk.length()) {
                if (offset < fileLength) {
                    int scanWindow = Math.min(offset + scanSize, fileLength) - offset;
                    currentChunk = fileService.range(offset, scanWindow);
                    offset += scanWindow;
                    index = 0;
                }
                else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Character next() {
            if (hasNext()) {
                char ch = currentChunk.charAt(index);
                index++;
                return ch;
            }
            else {
                throw new NoSuchElementException();
            }
        }
    }

    /**
     * Reads lines from a given character stream, hasNext() is always
     * efficient, all work is done only in next().
     */
    // Space complexity: O(max line length) in next() call, structure is O(1)
    // not counting charStream as it is only a reference, we will count that
    // in worker space.
    static class LineStream implements Iterator<String> {
        private final Iterator<Character> charStream;
        private int readIndex;
        private final int length;

        public LineStream(Iterator<Character> charStream, int length) {
            this.charStream = charStream;
            this.readIndex = 0;
            this.length = length;
        }

        @Override
        public boolean hasNext() {
            return readIndex <= length && charStream.hasNext();
        }

        @Override
        public String next() {
            if (hasNext()) {
                StringBuilder builder = new StringBuilder();
                while (charStream.hasNext()) {
                    char ch = charStream.next();
                    readIndex++;
                    if (ch == '\n') {
                        break;
                    }
                    builder.append(ch);
                }
                return builder.toString();
            }
            else {
                throw new NoSuchElementException();
            }
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    public void worker(int offset, int chunkSize, int scanSize) {
        Iterator<Character> charStream = new CharStream(fileService, offset, scanSize);
        Iterator<String> lineStream = new LineStream(charStream, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.next();
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            lineConsumer.accept(lineStream.next());
        }
    }

    // Space complexity: O(number of workers), not counting
    // workers space assuming they are running in different hosts.
    public void master(int chunkSize, int scanSize) {
        int len = fileService.length();
        for (int offset = 0; offset < len; offset += chunkSize) {
            int workerLength = Math.min(len, offset + chunkSize) - offset;
            worker(offset, workerLength, scanSize);
        }
    }

    /// SAMPLE CANDIDATE CODE ENDS

    static class MockFileService implements FileService {
        private final String contents;

        public MockFileService(String contents) {
            this.contents = contents;
        }

        // Provided by the file hosting service:
        @Override
        public int length() {
            // Mock implementation:
            return contents.length();
        }

        public String range(int offset, int length) {
            // Mock implementation
            return contents.substring(offset, offset + length);
        }
    }

    private static final String FILE = "./measurements_1M.txt";

    public interface ChunkProcessor {

        void process(byte[] line);

        Map<String, IntSummaryStatistics> summary();
    }

    public static class ChunkProcessorImpl implements ChunkProcessor {

        private final Map<String, IntSummaryStatistics> statistics = new TreeMap<>();

        @Override
        public void process(byte[] line) {
            String lineStr = new String(line, StandardCharsets.UTF_8);
            int indexSemi = lineStr.indexOf(';');
            String station = lineStr.substring(0, indexSemi);
            String value = lineStr.substring(indexSemi + 1);
            double val = Double.parseDouble(value);
            int normalized = (int) (val * 10);
            statistics.computeIfAbsent(station, (ignore) -> new IntSummaryStatistics());

            statistics.get(station).accept(normalized);
        }

        @Override
        public Map<String, IntSummaryStatistics> summary() {
            return statistics;
        }
    }

    public static void main(String[] args) throws IOException {

        ChunkProcessor chunkProcessor = new ChunkProcessorImpl();

        try (FileInputStream fis = new FileInputStream(FILE)) {

            Reader io = new InputStreamReader(fis, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(io);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                chunkProcessor.process(line.getBytes(StandardCharsets.UTF_8));
            }
        }

        System.out.println(chunkProcessor.summary());
    }
}
