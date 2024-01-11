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

import jdk.incubator.vector.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.TreeMap;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.foreign.ValueLayout.*;

/**
 * Broad experiments in this implementation:
 * - Memory-Map the file with new MemorySegments
 * - Use SIMD/vectorized search for the semicolon and new line feeds
 * - Use SIMD/vectorized comparison for the 'key'
 * <p>
 * Absolute stupid things / performance left on the table
 * - Single Threaded! Multi threading planned.
 * - The hash map/table is super basic.
 * - Hash table implementation / hashing has no resizing and is quite basic
 * - Zero time spend on profiling =)
 * <p>
 * <p>
 * Cheats used:
 * - Only works with Unix line feed \n
 * - double parsing is only accepting XX.X and X.X
 * - HashMap has no resizing, check, horrible hash etc.
 * - Used the double parsing from yemreinci
 */
public class CalculateAverage_gamlerhart {

    private static final String FILE = "./measurements.txt";
    final static VectorSpecies<Byte> byteVec = ByteVector.SPECIES_PREFERRED;
    final static Vector<Byte> zero = byteVec.zero();
    final static int vecLen = byteVec.length();
    final static Vector<Byte> semiColon = byteVec.broadcast(';');
    final static VectorMask<Byte> allTrue = byteVec.maskAll(true);
    final static ValueLayout.OfInt INT_UNALIGNED_BIG_ENDIAN = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    public static void main(String[] args) throws Exception {
        try (var arena = Arena.ofConfined();
                FileChannel fc = FileChannel.open(Path.of(FILE))) {
            var map = new PrivateHashMap();
            long fileSize = fc.size();
            MemorySegment fileContent = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);

            var loopBound = byteVec.loopBound(fileSize) - vecLen;
            for (long i = 0; i < fileSize;) {
                long nameStart = i;
                int simdSearchEnd = 0;
                int nameLen = 0;
                // Vectorized Search
                if (i < loopBound) {
                    do {
                        var vec = byteVec.fromMemorySegment(fileContent, i, ByteOrder.BIG_ENDIAN);
                        var hasSemi = vec.eq(semiColon);
                        simdSearchEnd = hasSemi.firstTrue();
                        i += simdSearchEnd;
                        nameLen += simdSearchEnd;
                    } while (simdSearchEnd == vecLen && i < loopBound);
                }
                // Left-over search
                while (loopBound <= i && fileContent.get(JAVA_BYTE, i) != ';') {
                    nameLen++;
                    i++;
                }
                i++; // Consume ;
                // Copied from yemreinci. I mostly wanted to experiment the vector math, not with parsing =)
                double val;
                {
                    boolean negative = false;
                    if ((fileContent.get(JAVA_BYTE, i)) == '-') {
                        negative = true;
                        i++;
                    }
                    byte b;
                    double temp;
                    if ((b = fileContent.get(JAVA_BYTE, i + 1)) == '.') { // temperature is in either XX.X or X.X form
                        temp = (fileContent.get(JAVA_BYTE, i) - '0') + (fileContent.get(JAVA_BYTE, i + 2) - '0') / 10.0;
                        i += 3;
                    }
                    else {
                        temp = (fileContent.get(JAVA_BYTE, i) - '0') * 10 + (b - '0')
                                + (fileContent.get(JAVA_BYTE, i + 3) - '0') / 10.0;
                        i += 4;
                    }
                    val = (negative ? -temp : temp);
                }
                i++; // Consume \n
                map.add(fileContent, nameStart, nameLen, val);
            }
            // System.out.println(map.debug_reprobeMax);
            var measurements = new TreeMap<String, ResultRow>();
            map.fill(fileContent, measurements);
            System.out.println(measurements);
        }
    }

    private static class PrivateHashMap {
        private static final int SIZE_SHIFT = 14;
        public static final int SIZE = 1 << SIZE_SHIFT;
        public static int MASK = 0xFFFFFFFF >>> (32 - SIZE_SHIFT);

        public static long SHIFT_POS = 16;
        public static long MASK_POS = 0xFFFFFFFFFFFF0000L;
        public static long MASK_LEN = 0x000000000000FFFFL;
        // Encoding:
        // - Key: long
        // - 48 bits index, 16 bits length
        // - min: double
        // - max: double
        // - sum: double
        // - double: double
        final long[] keyValues = new long[SIZE * 5];

        // int debug_size = 0;

        // int debug_reprobeMax = 0;
        public PrivateHashMap() {

        }

        public void add(MemorySegment file, long pos, int len, double val) {
            int hashCode = 1;
            int i = 0;
            int intBound = (len / 4) * 4;
            for (; i < intBound; i += 4) {
                int v = file.get(INT_UNALIGNED_BIG_ENDIAN, pos + i);
                hashCode = 31 * hashCode + v;
            }
            for (; i < len; i++) {
                int v = file.get(JAVA_BYTE, pos + i);
                hashCode = 31 * hashCode + v;
            }

            doAdd(file, hashCode, pos, len, val);
        }

        private void doAdd(MemorySegment file, int hash, long pos, int len, double val) {
            // var debug = new String(file.asSlice(pos, len).toArray(ValueLayout.JAVA_BYTE));
            int slot = hash & MASK;
            for (var probe = 0; probe < 20000; probe++) {
                var iSl = ((slot + probe) & MASK) * 5;
                var slotEntry = keyValues[iSl];

                var emtpy = slotEntry == 0;
                if (emtpy) {
                    long keyInfo = pos << SHIFT_POS | len;
                    long valueBits = doubleToRawLongBits(val);
                    keyValues[iSl] = keyInfo;
                    keyValues[iSl + 1] = valueBits;
                    keyValues[iSl + 2] = valueBits;
                    keyValues[iSl + 3] = valueBits;
                    keyValues[iSl + 4] = 1;
                    // debug_size++;
                    return;
                }
                else if (isSameEntry(file, slotEntry, pos, len)) {
                    keyValues[iSl + 1] = doubleToRawLongBits(Math.min(longBitsToDouble(keyValues[iSl + 1]), val));
                    keyValues[iSl + 2] = doubleToRawLongBits(Math.max(longBitsToDouble(keyValues[iSl + 2]), val));
                    keyValues[iSl + 3] = doubleToRawLongBits(longBitsToDouble(keyValues[iSl + 3]) + val);
                    keyValues[iSl + 4] = keyValues[iSl + 4] + 1;
                    return;
                }
                else {
                    // long keyPos = (slotEntry & MASK_POS) >> SHIFT_POS;
                    // int keyLen = (int) (slotEntry & MASK_LEN);
                    // System.out.println("Colliding " + new String(file.asSlice(pos,len).toArray(ValueLayout.JAVA_BYTE)) +
                    // " with key" + new String(file.asSlice(keyPos,keyLen).toArray(ValueLayout.JAVA_BYTE)) +
                    // " hash " + hash + " slot " + slot + "+" + probe + " at " + iSl);
                    // debug_reprobeMax = Math.max(debug_reprobeMax, probe);
                }
            }
            throw new IllegalStateException("More than 20000 reprobes");
            // throw new IllegalStateException("More than 100 reprobes: At " + debug_size + "");
        }

        private boolean isSameEntry(MemorySegment file, long slotEntry, long pos, int len) {
            long keyPos = (slotEntry & MASK_POS) >> SHIFT_POS;
            int keyLen = (int) (slotEntry & MASK_LEN);
            var isSame = isSame(file, keyPos, pos, len);
            // System.out.println("Entry:" + new String(file.asSlice(pos, len).toArray(JAVA_BYTE)) +
            // ",Keys:" + new String(file.asSlice(keyPos, keyLen).toArray(JAVA_BYTE)) +
            // ",match " + isSame);
            return isSame;
        }

        private static boolean isSame(MemorySegment file, long i1, long i2, int len) {
            int i = 0;
            var i1len = i1 + vecLen;
            var i2len = i2 + vecLen;
            if (len < vecLen && i1len <= file.byteSize() && i2len <= file.byteSize()) {
                var v1 = byteVec.fromMemorySegment(file, i1, ByteOrder.BIG_ENDIAN);
                var v2 = byteVec.fromMemorySegment(file, i2, ByteOrder.BIG_ENDIAN);
                var isTrue = v1.compare(VectorOperators.EQ, v2, allTrue.indexInRange(0, len));
                return isTrue.trueCount() == len;
            }
            while (8 < (len - i)) {
                var v1 = file.get(JAVA_LONG_UNALIGNED, i1 + i);
                var v2 = file.get(JAVA_LONG_UNALIGNED, i2 + i);
                if (v1 != v2) {
                    return false;
                }
                i += 8;
            }
            while (i < len) {
                var v1 = file.get(JAVA_BYTE, i1 + i);
                var v2 = file.get(JAVA_BYTE, i2 + i);

                if (v1 != v2) {
                    return false;
                }
                i++;
            }
            return true;
        }

        public void fill(MemorySegment file, TreeMap<String, ResultRow> treeMap) {
            for (int i = 0; i < keyValues.length / 5; i++) {
                var ji = i * 5;
                long keyE = keyValues[ji];
                if (keyE != 0) {
                    long keyPos = (keyE & MASK_POS) >> SHIFT_POS;
                    int keyLen = (int) (keyE & MASK_LEN);
                    byte[] keyBytes = new byte[keyLen];
                    MemorySegment.copy(file, JAVA_BYTE, keyPos, keyBytes, 0, keyLen);
                    var key = new String(keyBytes);
                    var min = longBitsToDouble(keyValues[ji + 1]);
                    var max = longBitsToDouble(keyValues[ji + 2]);
                    var sum = longBitsToDouble(keyValues[ji + 3]);
                    var count = keyValues[ji + 4];
                    treeMap.put(key, new ResultRow(min, sum / count, max));
                }
            }
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    ;
}
