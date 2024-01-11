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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class CalculateAverage_hallvard {

    private static class ResultRow {

        private String name;
        private int min, max, sum;
        private int count;

        public ResultRow(String name) {
            this.name = name;
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
            this.sum = 0;
            this.count = 0;
        }

        public ResultRow(String name, int value) {
            this.name = name;
            this.sum = this.max = this.min = value;
            this.count = 1;
        }

        @Override
        public String toString() {
            return (min / 10.0d) + "/" + (Math.round((double) sum / count) / 10.0d) + "/" + (max / 10.0d);
        }

        void update(int value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
        }

        void update(ResultRow row) {
            if (row.min < min) {
                min = row.min;
            }
            if (row.max > max) {
                max = row.max;
            }
            sum += row.sum;
            count += row.count;
        }
    };

    private static class Trie<T> {

        private final Node<T> root = new Node();

        String toString(String prefix, String separator, String suffix, Function<T, String> formatter, Comparator<T> comparator) {
            StringBuilder builder = new StringBuilder();
            List<T> payloads = new ArrayList<>();
            forEach(payloads::add);
            if (comparator != null) {
                Collections.sort(payloads, comparator);
            }
            for (var item : payloads) {
                if (builder.isEmpty()) {
                    if (prefix != null) {
                        builder.append(prefix);
                    }
                }
                else {
                    if (separator != null) {
                        builder.append(separator);
                    }
                }
                builder.append(formatter != null ? formatter.apply(item) : item.toString());
            }
            if (suffix != null) {
                builder.append(suffix);
            }
            return builder.toString();
        }

        void forEach(Consumer<T> consumer) {
            forEach(root, consumer);
        }

        private void forEach(Node<T> node, Consumer<T> consumer) {
            if (node.payload != null) {
                consumer.accept(node.payload);
            }
            for (int nodeIdx = 0; nodeIdx < node.rests.length; nodeIdx++) {
                Node<T> rest = node.rests[nodeIdx];
                if (rest != null) {
                    forEach(rest, consumer);
                }
            }
        }

        Node<T> getNode(ByteBuffer byteBuffer, int start, int end) {
            Node<T> node = root;
            next: for (int byteIdx = start; byteIdx < end; byteIdx++) {
                byte b = byteBuffer.get(byteIdx);
                if (node.nexts != null) {
                    for (int nodeIdx = 0; nodeIdx < node.nexts.length; nodeIdx++) {
                        byte next = node.nexts[nodeIdx];
                        if (next == b) {
                            // if found byte value, use corresponding node
                            node = node.rests[nodeIdx];
                            continue next;
                        }
                        else if (next == 0) {
                            // if empty slot add new node
                            node.nexts[nodeIdx] = b;
                            node = (node.rests[nodeIdx] = createDefaultNode());
                            continue next;
                        }
                    }
                    // convert to full node
                    Node<T>[] newRests = new Node[Byte.MAX_VALUE - Byte.MIN_VALUE];
                    for (int i = 0; i < node.nexts.length; i++) {
                        newRests[Node.idx(node.nexts[i])] = node.rests[i];
                    }
                    // new entry
                    Node<T> newNode = createDefaultNode();
                    newRests[Node.idx(b)] = newNode;
                    node.nexts = null;
                    node.rests = newRests;
                    node = newNode;
                }
                else {
                    int idx = Node.idx(b);
                    Node<T> rest = node.rests[idx];
                    node = (rest != null ? rest : (node.rests[idx] = createDefaultNode()));
                }
            }
            return node;
        }

        final Node<T> createDefaultNode() {
            return new Node(4);
        }

        private static class Node<T> {
            private T payload;
            private byte[] nexts;
            private Node<T>[] rests;

            // full node that covers all byte values, with byte as index
            Node() {
                nexts = null;
                rests = new Node[Byte.MAX_VALUE - Byte.MIN_VALUE];
            }

            // sparse node that covers some byte values, index of value (in nexts) gives index of node (in rests)
            Node(int length) {
                nexts = new byte[length];
                rests = new Node[length];
            }

            static final int idx(byte b) {
                return b - Byte.MIN_VALUE;
            }
        }
    }

    private static boolean computeAverages(ByteBuffer byteBuffer, int start, Trie<ResultRow> results) {
        // search backwards to first newline
        int startPos = start;
        while (startPos > 0 && byteBuffer.get(startPos - 1) != '\n') {
            startPos--;
        }
        byteBuffer.position(startPos);
        while (byteBuffer.hasRemaining()) {
            // find name range
            int nameStart = byteBuffer.position(), limit = byteBuffer.limit(), pos = nameStart;
            while (pos < limit && byteBuffer.get(pos) != ';') {
                pos++;
            }
            // is there room for ; a digit, decimal point, a decimal and the final newline
            if (pos + 4 >= limit) {
                return false;
            }
            int nameEnd = pos++;

            // parse value
            byte next = byteBuffer.get(pos++);
            boolean negative = false;
            if (next == '-') {
                negative = true;
                next = byteBuffer.get(pos++);
            }
            int value = next - '0';
            int decimalPos = -1;
            while (pos < limit && (next = byteBuffer.get(pos)) != '\n') {
                if (next == '.') {
                    if (decimalPos >= 0) {
                        return false;
                    }
                    decimalPos = pos;
                }
                else {
                    value = value * 10 + (next - '0');
                }
                pos++;
            }
            if (next != '\n') {
                return false;
            }
            if (negative) {
                value = -value;
            }
            // skip newline
            byteBuffer.position(pos + 1);
            Trie.Node<ResultRow> node = results.getNode(byteBuffer, nameStart, nameEnd);
            ResultRow result = node.payload;
            if (result == null) {
                byte[] bytes = new byte[nameEnd - nameStart];
                byteBuffer.get(nameStart, bytes);
                result = new ResultRow(new String(bytes), value);
                node.payload = result;
            }
            else {
                result.update(value);
            }
        }
        return true;
    }

    private record TaskInfo(long chunkStart, int chunkSize, int start) {
        Trie<ResultRow> doTask(FileChannel channel) {
            Trie<ResultRow> results = new Trie<>();
            try {
                //System.err.println("Mapping bytes " + chunkStart + " - " + (chunkStart + chunkSize));
                //System.err.flush();
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunkStart, chunkSize);
                //System.err.println("Computing averages from " + (chunkStart + start) + " (" + start + ")");
                //System.err.flush();
                computeAverages(buffer, start, results);
                //System.err.println("Read upto " + (chunkStart + buffer.position()));
                //System.err.flush();
            } catch (IOException e) {
                throw new RuntimeException("Exception while doing " + this + ": " + e);
            }
            return results;
        }
    }

    public static void main(String[] args) throws IOException {
        Path measurementsPath = Paths.get("./measurements.txt");
        try (FileChannel channel = FileChannel.open(measurementsPath)) {
            int ROW_SIZE = 50, CHUNK_SIZE = 100_000_000;
            long size = channel.size(), pos = 0;
            List<TaskInfo> tasks = new ArrayList<>();
            while (pos >= 0 && pos < size) {
                long chunkStart = Math.max(pos - ROW_SIZE, 0);
                int chunkSize = (int) Math.min(size - chunkStart, CHUNK_SIZE + (pos - chunkStart));
                tasks.add(new TaskInfo(chunkStart, chunkSize, (int) (pos - chunkStart)));
                pos = chunkStart + chunkSize;
            }
            Map<String, ResultRow> results = new TreeMap<>();
            tasks.parallelStream()
                    .map(task -> task.doTask(channel))
                    .forEach(result -> {
                        result.forEach(resultRow -> {
                            synchronized (results) {
                                ResultRow existing = results.get(resultRow.name);
                                if (existing != null) {
                                    existing.update(resultRow);
                                }
                                else {
                                    results.put(resultRow.name, resultRow);
                                }
                            }
                        });
                    });
            System.out.println(results);
        }
    }
}
