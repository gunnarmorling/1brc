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

import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

public class CalculateAverage_yourwass {
    static final class Record {
        private long cityAddr;
        private long cityLength;
        private int min;
        private int max;
        private int count;
        private long sum;

        Record(final long cityAddr, final long cityLength) {
            this.cityAddr = cityAddr;
            this.cityLength = cityLength;
            this.min = 1000;
            this.max = -1000;
            this.sum = 0;
            this.count = 0;
        }

        private Record merge(Record r) {
            if (r.min < this.min)
                this.min = r.min;
            if (r.max > this.max)
                this.max = r.max;
            this.sum += r.sum;
            this.count += r.count;
            return this;
        }
    }

    private final static Lock _mutex = new ReentrantLock(true);
    private final static TreeMap<String, Record> aggregateResults = new TreeMap<>();
    private static short lookupDecimal[];
    private static byte lookupFraction[];
    private static byte lookupDotPositive[];
    private static byte lookupDotNegative[];
    private static MemorySegment VAS;
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int MAXINDEX = (1 << 16) + 10000; // short hash + max allowed cities for collisions at the end :p
    private static final String FILE = "measurements.txt";
    private static long unsafeResults;
    private static int RECORDSIZE = 36;
    private static final Unsafe UNSAFE = getUnsafe();

    private static Unsafe getUnsafe() {
        try {
            final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Unsafe unsafe = (Unsafe) theUnsafe.get(null);
            return unsafe;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, Throwable {
        // prepare lookup tables
        // the parsing reads two shorts after possible '-'
        // first short, the Decimal part, can be N. or NN with N:[0..9]
        // second short, the Fraction part, can be N\n or .N
        lookupDecimal = new short[('9' << 8) + '9' + 1];
        lookupFraction = new byte[('9' << 8) + '.' + 1];
        lookupDotPositive = new byte[('9' << 8) + '.' + 1];
        lookupDotNegative = new byte[('9' << 8) + '.' + 1];
        for (short i = 0; i < 10; i++) {
            final int ones = i * 10;
            final int ix256 = i << 8;
            // case N. i.e. single digit decimals: skip to 11824 = ('.'<<8)+'0'
            lookupDecimal[11824 + i] = (short) ones;
            for (short j = 1; j < 10; j++) {
                // case NN i.e double digits decimals: skip to 12236 = ('0'<<8)+'0'
                lookupDecimal[12336 + ix256 + j] = (short) (j * 100 + ones);
            }
            // case N\n skip to 2608 = ('\n'<<8)+'0'
            lookupFraction[2608 + i] = (byte) i;
            lookupDotPositive[2608 + i] = 4;
            lookupDotNegative[2608 + i] = 5;
            // case .N skip to 12334 = ('0'<<8)+'.'
            lookupFraction[12334 + ix256] = (byte) i;
            lookupDotPositive[12334 + ix256] = 5;
            lookupDotNegative[12334 + ix256] = 6;
        }

        // open file
        final FileChannel fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
        final long fileSize = fileChannel.size();
        final long mmapAddr = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global()).address();
        // VAS: Virtual Address Space, as a MemorySegment upto and including the mmaped file.
        // If the mmaped MemorySegment is used for Vector creation as is, then there are two problems:
        // 1) fromMemorySegment takes an offset and not an address, so we have to do arithmetic
        // this is solved by creating a MemorySegment from Address=0
        // 2) fromMemorySegment checks bounds for memory segment's size - Vector size
        // this is solved by adding SPECIES.length() to the size of the segment, but
        // XXX there lies the possibility for an out of bounds read at the end of file, which is not handled here.
        VAS = MemorySegment.ofAddress(0).reinterpret(mmapAddr + fileSize + SPECIES.length());

        // allocate memory for results
        final int nThreads = Runtime.getRuntime().availableProcessors();
        unsafeResults = UNSAFE.allocateMemory(RECORDSIZE * MAXINDEX * nThreads);
        UNSAFE.setMemory(unsafeResults, RECORDSIZE * MAXINDEX * nThreads, (byte) 0);

        // start and wait for threads to finish
        Thread[] threadList = new Thread[nThreads];
        final long chunkSize = fileSize / nThreads;
        for (int i = 0; i < nThreads; i++) {
            final int threadIndex = i;
            final long startAddr = mmapAddr + i * chunkSize;
            final long endAddr = (i == nThreads - 1) ? mmapAddr + fileSize : mmapAddr + (i + 1) * chunkSize;
            threadList[i] = new Thread(() -> threadMain(threadIndex, startAddr, endAddr, nThreads));
            threadList[i].start();
        }
        for (int i = 0; i < nThreads; i++)
            threadList[i].join();

        // prepare string and print
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (var entry : aggregateResults.entrySet()) {
            Record record = entry.getValue();
            float min = record.min;
            min /= 10.f;
            float max = record.max;
            max /= 10.f;
            double avg = Math.round((record.sum * 1.0) / record.count) / 10.;
            sb.append(entry.getKey()).append("=").append(min).append("/").append(avg).append("/").append(max).append(", ");
        }
        int stringLength = sb.length();
        sb.setCharAt(stringLength - 2, '}');
        sb.setCharAt(stringLength - 1, '\n');
        System.out.print(sb.toString());
        System.out.close();
    }

    private static final boolean citiesDiffer(final long a, final long b, final long len) {
        int part = 0;
        for (; part < (len - 1) >> 3; part++)
            if (UNSAFE.getLong(a + (part << 3)) != UNSAFE.getLong(b + (part << 3)))
                return true;
        if (((UNSAFE.getLong(a + (part << 3)) ^ (UNSAFE.getLong(b + (part << 3)))) << ((8 - (len & 7)) << 3)) != 0)
            return true;
        return false;
    }

    private static void threadMain(int id, long startAddr, long endAddr, long nThreads) {
        // snap to newlines
        if (id != 0)
            while (UNSAFE.getByte(startAddr++) != '\n')
                ;
        if (id != nThreads - 1)
            while (UNSAFE.getByte(endAddr++) != '\n')
                ;

        final long threadResults = unsafeResults + id * MAXINDEX * RECORDSIZE;
        final Record[] results = new Record[MAXINDEX];
        final long VECTORBYTESIZE = SPECIES.length();
        final ByteOrder BYTEORDER = ByteOrder.nativeOrder();
        final ByteVector delim = ByteVector.broadcast(SPECIES, ';');
        long cityAddr = startAddr;
        long ptr = 0;
        while (cityAddr < endAddr) {
            // parse city
            ByteVector parsed = ByteVector.fromMemorySegment(SPECIES, VAS, cityAddr, BYTEORDER);
            long mask = parsed.compare(VectorOperators.EQ, delim).toLong();
            while (mask == 0) {
                ptr += VECTORBYTESIZE;
                mask = ByteVector.fromMemorySegment(SPECIES, VAS, cityAddr + ptr, BYTEORDER).compare(VectorOperators.EQ, delim).toLong();
            }
            final long cityLength = ptr + Long.numberOfTrailingZeros(mask);
            final long tempAddr = cityAddr + cityLength + 1;
            ptr = 0;

            // compute hash table index
            int index;
            if (cityLength > 1)
                index = (UNSAFE.getByte(cityAddr) // mix the first,
                        ^ (UNSAFE.getByte(cityAddr + 2) << 4) // the third (even if it is the delimiter ';')
                        ^ (UNSAFE.getByte(tempAddr - 2) << 8) // and the last two bytes of each city's name
                        ^ (UNSAFE.getByte(tempAddr - 3) << 12))
                        & 0xFFFF;
            else
                index = (UNSAFE.getByte(cityAddr) << 8) & 0xFF00;
            // resolve collisions with linear probing
            // use vector api here also, but only if city name fits in one vector length, for faster default case
            long record = threadResults + index * RECORDSIZE;
            long recordCityLength = UNSAFE.getLong(record);
            if (cityLength <= VECTORBYTESIZE) {
                while (recordCityLength > 0) {
                    if (cityLength == recordCityLength) {
                        long sameMask = ByteVector.fromMemorySegment(SPECIES, VAS, UNSAFE.getLong(record + 8), BYTEORDER)
                                .compare(VectorOperators.EQ, parsed).toLong();
                        if (Long.numberOfTrailingZeros(~sameMask) >= cityLength)
                            break;
                    }
                    index++;
                    record = threadResults + index * RECORDSIZE;
                    recordCityLength = UNSAFE.getLong(record);
                }
            }
            else { // slower normal case for city names with length > VECTORBYTESIZE
                while (recordCityLength > 0 && (cityLength != recordCityLength || citiesDiffer(UNSAFE.getLong(record + 8), cityAddr, cityLength))) {
                    index++;
                    record = threadResults + index * RECORDSIZE;
                    recordCityLength = UNSAFE.getLong(record);
                }
            }

            // add record for new key
            if (recordCityLength == 0) {
                UNSAFE.putLong(record, cityLength);
                UNSAFE.putLong(record + 8, cityAddr);
                UNSAFE.putInt(record + 16, 1000);
                UNSAFE.putInt(record + 20, -1000);
            }

            // parse temp with lookup tables
            int temp;
            if (UNSAFE.getByte(tempAddr) == '-') {
                temp = -lookupDecimal[UNSAFE.getShort(tempAddr + 1)] - lookupFraction[UNSAFE.getShort(tempAddr + 3)];
                cityAddr = tempAddr + lookupDotNegative[UNSAFE.getShort(tempAddr + 3)];
            }
            else {
                temp = lookupDecimal[UNSAFE.getShort(tempAddr)] + lookupFraction[UNSAFE.getShort(tempAddr + 2)];
                cityAddr = tempAddr + lookupDotPositive[UNSAFE.getShort(tempAddr + 2)];
            }

            // merge
            if (temp < UNSAFE.getInt(record + 16))
                UNSAFE.putInt(record + 16, temp);
            if (temp > UNSAFE.getInt(record + 20))
                UNSAFE.putInt(record + 20, temp);
            UNSAFE.putLong(record + 24, UNSAFE.getLong(record + 24) + temp);
            UNSAFE.putInt(record + 32, UNSAFE.getInt(record + 32) + 1);
        }

        // create strings from raw data
        // and aggregate results onto TreeMap
        int idx = 0;
        byte b[] = new byte[100];
        _mutex.lock();
        for (int i = 0; i < MAXINDEX; i++) {
            if (UNSAFE.getLong(threadResults + i * RECORDSIZE) == 0)
                continue;
            final long recordAddress = threadResults + i * RECORDSIZE;

            results[idx] = new Record(UNSAFE.getLong(recordAddress + 8), UNSAFE.getLong(recordAddress));
            results[idx].min = UNSAFE.getInt(recordAddress + 16);
            results[idx].max = UNSAFE.getInt(recordAddress + 20);
            results[idx].sum = UNSAFE.getLong(recordAddress + 24);
            results[idx].count = UNSAFE.getInt(recordAddress + 32);
            UNSAFE.copyMemory(null, UNSAFE.getLong(recordAddress + 8), b, Unsafe.ARRAY_BYTE_BASE_OFFSET, UNSAFE.getLong(recordAddress));
            final Record record = results[idx];
            aggregateResults.compute(new String(b, 0, (int) results[idx].cityLength, StandardCharsets.UTF_8), (k, v) -> (v == null) ? record : v.merge(record));
            idx++;
        }
        _mutex.unlock();
    }
}
