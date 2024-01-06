/*
 *  Copyright 2024 Cliff Click
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

import java.nio.file.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.*;

import java.util.List;
import java.util.Comparator;
import java.util.Arrays;
import java.util.HashMap;

abstract class CalculateAverage_cliffclick {
    public static final int NCPUS = Runtime.getRuntime().availableProcessors();
    public static final long HASSEMI = 0x3B3B3B3B3B3B3B3BL;

    public static int X0;

    public static void main(String[] args) throws Exception {
        if (args.length < 1)
            args = new String[]{ "./measurements.txt" };
        System.out.println(work(args));
    }

    // General work flow:

    // Spawn threads. Make empty hash for sums, counts.
    // Seek to offset. Skip till newline. Parse to end of chunk, plus rest of partial line.

    // SWAR out city name into n8; both hash and uhash name.
    // Lookup in sums; if miss: insert map uhash to 0 cnt; insert map uhash to String name also;
    // if hit: bump count; same index in sums array, bump sums with scaled decimal
    // At end, grab more work until done.

    // At end, across all threads total sums & counts.
    // Compute averages, lookup names and print.
    static Work work(String[] args) throws Exception {
        File f = new File(args[0]);

        // How many threads?
        int ncpus = (int) Math.min((f.length() >> 14) + 1, NCPUS); // Keep 1<<14 min work
        long len = (f.length() / ncpus) + 1;

        Work[] WS = new Work[ncpus];
        Thread[] TS = new Thread[ncpus];

        // Spawn work on threads
        for (int i = 0; i < ncpus; i++) {
            long s = i * len;
            Work w = WS[i] = new Work();
            Thread T = TS[i] = new Thread() {
                public void run() {
                    tstart(w, f, s, Math.min(len, f.length() - s));
                }
            };
            T.start();
            // T.join();
        }

        TS[0].join();
        Work W = WS[0];
        for (int i = 1; i < ncpus; i++) {
            TS[i].join();
            W = W.reduce(WS[i]);
        }
        return W;
    }

    static Work tstart(Work w, File f, long start, long len) {
        try {
            // Thread gets a chunk of work
            FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.READ);
            final int MAX_MAP = 1 << 30;

            for (long s = start; s < start + len; s += MAX_MAP) {
                int maxlen = (int) Math.min(len, MAX_MAP); // Length capped at MAX_MAP
                long rem = f.length() - s;
                int mlen = (int) Math.min(rem, maxlen + 100); // Add a little extra so can finish out a line
                int clen = (int) Math.min(rem, maxlen);
                // mmap is capped at MAX_MAP (plus change), or
                // to the end of the chosen parse length (plus change)
                // or the end of the file in any case
                MappedByteBuffer mmap = fc.map(FileChannel.MapMode.READ_ONLY, s, mlen);
                // Chunk runs to min(MAX_MAP, parse length, eof), plus it runs to the end
                // of any partial line.
                do_chunk(w, s > 0, clen, mmap);
            }
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return w;
    }

    // Has a zero byte in a long
    static long has0(long x) {
        return (x - 0x0101010101010101L) & (~x) & 0x8080808080808080L;
    }

    // Parse a chunk, from 0 to limit in mmap. Runs past limit to finish any
    // partial line. If skip1, then skip any leading partial line.
    static void do_chunk(Work w, boolean skip1, int limit, MappedByteBuffer mmap) {
        assert mmap.isDirect();
        int idx = 0;
        int max = mmap.limit();
        // If start>0, skip until first newline
        if (skip1) {
            while (mmap.get(idx) != '\n')
                idx++;
            // WINDOWS
            // idx++;
        }

        // For this chunk of file do...
        while (idx < limit) {
            int cityx = idx; // Used if we find a new city name

            // SWAR read and build n8; the long-as-a-string value. Also track start
            // and end of the string, in case it is new and needs to be inserted into
            // the n8->city_name map.
            long n8 = 0;
            // Read a misaligned long
            long x = mmap.getLong(idx);
            // Found semi ?
            long hasM = has0(x ^ HASSEMI);
            while (hasM == 0) {
                // Read 2nd word of city
                n8 ^= x;
                idx += 8;
                // Edge case for last few bytes in mmap
                if (idx + 8 > max) {
                    x = mmap.getLong(max - 8);
                    int skip = (8 - (max - idx));
                    idx -= skip; // Roll back skip amount
                    x = x & (-1L >>> (skip << 3)); // Get just the last few bytes in mmap
                }
                else {
                    // Read a misaligned long
                    x = mmap.getLong(idx);
                }
                // Found semi ?
                hasM = has0(x ^ HASSEMI);
            }
            // Found a semicolon this word.
            // There is only one, because city names are not that short.
            // The high bit of the byte in question is set.
            int semidx = Long.numberOfTrailingZeros(hasM);
            int shr = semidx + 1;
            if (shr != 64) {
                n8 ^= (x >> shr);
                idx += 8 - ((semidx + 1) >> 3);
            }

            // Skip semicolon
            idx++;

            // Reading tempature:
            int temp = 0;
            boolean neg = false;
            byte b = mmap.get(idx++);
            if (b == '-') {
                neg = true;
                b = mmap.get(idx++);
            }
            temp = b - '0';
            b = mmap.get(idx++);
            if (b != '.') {
                temp = temp * 10 + b - '0';
                idx++;
            }
            // Read fraction digit; scaled decimal temp
            b = mmap.get(idx++);
            temp = temp * 10 + b - '0';
            if (neg)
                temp = -temp;
            // Skip newline
            idx++;
            // F*KING WINDOWS.
            // Skip CR
            // idx++;

            w.insert(n8, temp, mmap, cityx);
        }
    }

    private static class Work {
        private static final int TAB_SIZE = 0x800; // 512 for 413 cities
        // Fixed size hashtable. Longs are packed to hold the data.
        // cnt uhash
        // 8 7 6 5 4 3 2 1
        // min max temp sum
        // 8 7 6 5 4 3 2 1
        long[] table = new long[TAB_SIZE * 2]; //
        String[] cities = new String[TAB_SIZE]; // Same index holds city names

        // Gather for city bits
        final byte[] city = new byte[32];

        void insert(long n8, int temp, MappedByteBuffer mmap, int idx) {
            // 3 bytes uniquely id city, left at 4
            int uhash = ((int) uhash_final(n8)) & 0xFFFFFF;
            // Index in small table
            int ihash = uhash;
            ihash = ihash ^ (ihash >> 17);
            ihash = ihash + 29 * uhash;
            ihash &= (TAB_SIZE - 1);

            long cnt_key = table[(ihash << 1)];
            long min_max = table[(ihash << 1) + 1];
            int key = key(cnt_key);
            int probes = 0;
            while (key != uhash) {
                if (key == 0) {
                    // Miss in hash table
                    cnt_key = uhash;
                    min_max = min_max(0x7FFF, 0, 0);
                    // Put city name in cities
                    new_city(ihash, mmap, idx);
                    break;
                }
                // Reprobe
                ihash = (ihash + (uhash | 1)) & (TAB_SIZE - 1);
                cnt_key = table[(ihash << 1)];
                min_max = table[(ihash << 1) + 1];
                key = (int) cnt_key;
            }

            // Break down parts
            int min = min(min_max);
            min = Math.min(min, temp);
            int max = max(min_max);
            max = Math.max(max, temp);
            int sum = temp(min_max);
            sum += temp;
            min_max = min_max(min, max, sum);
            // Back into table
            table[(ihash << 1)] = cnt_key + (1L << 32);
            table[(ihash << 1) + 1] = min_max;
        }

        // Hash the n8 value; the 3 bytes uniquely identify the city.
        static long uhash_final(long n8) {
            return n8 ^ (n8 >>> 26);
        }

        // New city
        void new_city(int ihash, MappedByteBuffer mmap, int idx) {
            // Put city name in cities
            int i = 0;
            byte c;
            while ((c = mmap.get(idx++)) != ';')
                city[i++] = c;
            cities[ihash] = new String(city, 0, i);
        }

        // Convert the large unique hash into a smaller table hash
        int ihash(int uhash) {
            // Index in small table
            int ihash = uhash;
            ihash = ihash ^ (ihash >> 17);
            ihash = ihash + 29 * uhash;
            ihash &= (TAB_SIZE - 1);

            long cnt_key = table[(ihash << 1)];
            int key = key(cnt_key);
            int probes = 0;
            while (key != uhash) {
                if (key == 0)
                    return ihash;
                // Reprobe
                ihash = (ihash + (uhash | 1)) & (TAB_SIZE - 1);
                cnt_key = table[(ihash << 1)];
                key = key(cnt_key);
            }
            return ihash;
        }

        Work reduce(Work w) {
            for (int i = 0; i < w.cities.length; i++) {
                if (w.cities[i] == null)
                    continue;

                // Break down parts
                long cnt_key = w.table[(i << 1)];
                long min_max = w.table[(i << 1) + 1];
                int cnt = cnt(cnt_key);
                int key = key(cnt_key);
                int min = min(min_max);
                int max = max(min_max);
                int sum = temp(min_max);

                // Find key in local table
                int ihash = ihash(key);
                long cnt_key0 = table[(ihash << 1)];
                long min_max0 = table[(ihash << 1) + 1];
                int cnt0 = cnt(cnt_key0);
                int key0 = key(cnt_key0);
                int min0 = min(min_max0);
                int max0 = max(min_max0);
                int sum0 = temp(min_max0);

                cnt0 += cnt;
                sum0 += sum;
                min0 = Math.min(min0, min);
                max0 = Math.max(max0, max);
                if (key0 != 0) {
                    assert key0 == key;
                }
                else {
                    key0 = key;
                    min0 = min;
                    cities[ihash] = w.cities[i];
                }
                table[(ihash << 1)] = cnt_key(cnt0, key0);
                table[(ihash << 1) + 1] = min_max(min0, max0, sum0);
            }
            return this;
        }

        static int key(long cnt_key) {
            return (int) cnt_key;
        }

        static int cnt(long cnt_key) {
            return (int) (cnt_key >> 32);
        }

        static int min(long min_max) {
            return (int) (min_max >> 48);
        } // Signed right shift; min often negative

        static int max(long min_max) {
            return (int) (min_max >>> 32) & 0xFFFF;
        }// Unsigned right shift;

        static int temp(long min_max) {
            return (int) min_max;
        }; // Low int

        static long cnt_key(int cnt, int key) {
            return ((long) cnt << 32) | (key & 0xFFFFFFFFL);
        }

        static long min_max(int min, int max, int sum) {
            return ((long) min << 48) | ((long) max << 32) | (((long) sum) & 0xFFFFFFFFL);
        }

        @Override
        public String toString() {
            int ncitys = 0; // totals 413
            for (int i = 0; i < TAB_SIZE; i++)
                if (cities[i] != null)
                    ncitys++;
            // Index of city entries
            Integer[] is = new Integer[ncitys];
            for (int i = 0, j = 0; i < TAB_SIZE; i++)
                if (cities[i] != null)
                    is[j++] = i;

            // Sort indices
            Arrays.sort(is, (x, y) -> cities[x].compareTo(cities[y]));

            StringBuilder sb = new StringBuilder().append("{");
            for (int i : is) {
                String city = cities[i];
                int cnt = (int) (table[i << 1] >> 32); // Cnts in high word; uhash in low word
                long min_max = table[(i << 1) + 1];
                double min = (min_max >> 48) / 10.0; // Keep the low sign, high sigh is always positive
                double max = ((min_max >> 32) & 0xFFFF) / 10.0;
                double temp = ((int) min_max) / 10.0;
                double mean = temp / cnt; // Unscale scaled decimal, then average
                sb.append(String.format("%s=%.1f/%.1f/%.1f, ", cities[i], min, mean, max));
                // sb.append(city).append('=').append(min).append('/').append(mean).append('/').append(max).append(", ");
            }
            sb.setLength(sb.length() - 2);
            return sb.append("}").toString();
        }
    }
}
