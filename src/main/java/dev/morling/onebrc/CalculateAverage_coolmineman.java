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

import static java.util.stream.Collectors.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_coolmineman {

    private static final String FILE = "./measurements.txt";

    // TODO maybe just write a byte arraylist
    static class ByteArrayOutputStreamEx extends ByteArrayOutputStream {
        byte[] buf() {
            return buf;
        }

        void shrink(int i) {
            count -= i;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class TrieNode {
        TrieNode[] next;
        MeasurementAggregator leaf;

        TrieNode get(int c) {
            if (next == null)
                next = new TrieNode[256];
            var n = next[c];
            if (n == null) {
                n = next[c] = new TrieNode();
            }
            return n;
        }

        MeasurementAggregator leaf() {
            if (leaf == null)
                leaf = new MeasurementAggregator();
            return leaf;
        }

        void write(OutputStream os) throws IOException {
            os.write('{' & 0xFF);
            write(os, new ByteArrayOutputStreamEx(), true);
            os.write('}' & 0xFF);
        }

        boolean write(OutputStream os, ByteArrayOutputStreamEx namestack, boolean first) throws IOException {
            if (leaf != null) {
                if (!first) {
                    os.write(',' & 0xFF);
                    os.write(' ' & 0xFF);
                }
                os.write(namestack.buf(), 0, namestack.size());
                os.write('=');
                os.write(leaf.toString().getBytes(StandardCharsets.UTF_8));
                first = false;
            }
            if (next != null) {
                for (int i = 0; i < 256; i++) {
                    var n = next[i];
                    if (n != null) {
                        namestack.write(i);
                        first = n.write(os, namestack, first);
                        namestack.shrink(1);
                    }
                }
            }
            return first;
        }
    }

    public static void main(String[] args) throws IOException {
        TrieNode measurements = new TrieNode();

        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(FILE)))) {
            ByteArrayOutputStreamEx os = new ByteArrayOutputStreamEx();
            TrieNode node = measurements;
            boolean parsingDouble = false;
            int r;
            while ((r = in.read()) > 0) {
                if (parsingDouble) {
                    if (((char) r) == '\n') {
                        node.leaf().add(Double.parseDouble(new String(os.buf(), 0, os.size(), StandardCharsets.UTF_8)));
                        os.reset();
                        node = measurements;
                        parsingDouble = false;
                    } else {
                        os.write(r);
                    }
                } else {
                    if (((char) r) == ';') {
                        parsingDouble = true;
                    } else {
                        node = node.get(r);
                    }
                }
            }
        }

        try (OutputStream os = new BufferedOutputStream(System.out)) {
            measurements.write(os);
        }
    }
}
