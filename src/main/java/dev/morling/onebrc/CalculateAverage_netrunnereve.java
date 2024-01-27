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
import java.util.Arrays;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.lang.Math;

public class CalculateAverage_netrunnereve {

    private static final String FILE = "./measurements.txt";
    private static final int NUM_THREADS = 8; // test machine
    private static final int LEN_EXTEND = 200; // guarantees a newline
    private static final int HASHT_SIZE = 16384; // size of hash table, adjust tradeoff between colisions and cache utilization
    private static final int DJB2_INIT = 5831;

    private static class MeasurementAggregator { // min, max, sum stored as 0.1/unit
        private MeasurementAggregator next = null; // linked list of entries for handling hash colisions
        private byte[] station = null;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0;
        private int count = 0;
    }

    private static class ThreadCalcs {
        private MeasurementAggregator[] hashSpace = null;
        private String[] staArr = null;
        private int numStations = 0;
    }

    // djb2 hash
    private static int calc_hash(byte[] input, int len) {
        int hash = DJB2_INIT;
        for (int i = 0; i < len; i++) {
            hash = ((hash << 5) + hash) + Byte.toUnsignedInt(input[i]);
        }
        return Math.abs(hash % HASHT_SIZE);
    }

    private static class ThreadedParser extends Thread {
        private MappedByteBuffer mbuf;
        private int mbs;
        private ThreadCalcs[] threadOut;
        private int threadID;
        private CountDownLatch tpLatch;

        private ThreadedParser(MappedByteBuffer mbuf, int mbs, ThreadCalcs[] threadOut, int threadID, CountDownLatch tpLatch) {
            this.mbuf = mbuf;
            this.mbs = mbs;
            this.threadOut = threadOut;
            this.threadID = threadID;
            this.tpLatch = tpLatch;
        }

        public void run() {
            MeasurementAggregator[] hashSpace = new MeasurementAggregator[HASHT_SIZE]; // hash table
            byte[] scratch = new byte[100]; // <= 100 characters in station name
            String[] staArr = new String[10000]; // max 10000 station names
            MeasurementAggregator ma = null;

            int numStations = 0;
            int negMul = 1;
            int head = 0;
            int tempCnt = -1; // 0 if 1 digit measurement, 1 if 2 digit
            int hash = DJB2_INIT; // do calc_hash manually in loop

            int i = 0; // byte by byte iterator
            while (true) {
                byte cur = mbuf.get(i);
                if (cur == 59) { // ;
                    hash = Math.abs(hash % HASHT_SIZE);

                    // this is faster than filling scratch immediately after each byte is read
                    int len = i - head;
                    mbuf.position(head);
                    mbuf.get(scratch, 0, len);

                    ma = hashSpace[hash];
                    MeasurementAggregator prev = null;

                    while (true) {
                        if (ma == null) {
                            ma = new MeasurementAggregator();
                            ma.station = Arrays.copyOfRange(scratch, 0, len);
                            staArr[numStations] = new String(scratch, 0, len, StandardCharsets.UTF_8);

                            if (prev != null) {
                                prev.next = ma;
                            }
                            else {
                                hashSpace[hash] = ma;
                            }

                            numStations++;
                            break;
                        }
                        else if ((len != ma.station.length) || (Arrays.compare(scratch, 0, len, ma.station, 0, len) != 0)) { // hash collision
                            prev = ma;
                            ma = ma.next;
                        }
                        else { // hit
                            break;
                        }
                    }

                    i++;
                    while (true) {
                        cur = mbuf.get(i);
                        if (cur == 46) { // .
                            int tempa = (negMul) * ((10 + 90 * tempCnt) * (scratch[0] - 48) + (10 * tempCnt) * (scratch[1] - 48) + (mbuf.get(i + 1) - 48)); // branchless

                            if (tempa < ma.min) {
                                ma.min = tempa;
                            }
                            if (tempa > ma.max) {
                                ma.max = tempa;
                            }
                            ma.sum += tempa;
                            ma.count++;

                            // this line is finished!
                            i += 2; // newline char
                            hash = DJB2_INIT;
                            negMul = 1;
                            head = i + 1; // start of next line
                            tempCnt = -1;
                            break;
                        }
                        else if (cur == 45) { // ascii -
                            negMul = -1;
                        }
                        else {
                            scratch[tempCnt + 1] = cur;
                            tempCnt++;
                        }
                        i++;
                    }
                    if (head >= mbs) {
                        break;
                    }
                }
                else {
                    hash = ((hash << 5) + hash) + Byte.toUnsignedInt(cur);
                }
                i++;
            }
            threadOut[threadID] = new ThreadCalcs();
            threadOut[threadID].hashSpace = hashSpace;
            threadOut[threadID].staArr = staArr;
            threadOut[threadID].numStations = numStations;
            tpLatch.countDown();
        }
    }

    public static void main(String[] args) {
        try {
            RandomAccessFile mraf = new RandomAccessFile(FILE, "r");
            long fileSize = mraf.getChannel().size();
            long threadNum = NUM_THREADS;

            long minThreads = (fileSize / Integer.MAX_VALUE) + 1; // minimum # of threads required due to MappedByteBuffer size limit
            if (threadNum < minThreads) {
                threadNum = minThreads;
            }
            long bufSize = fileSize / threadNum;

            // don't bother multithreading for small files
            if (bufSize < 1000000) {
                threadNum = 1;
                bufSize = Integer.MAX_VALUE;
            }

            ThreadCalcs[] threadOut = new ThreadCalcs[(int) threadNum];
            CountDownLatch tpLatch = new CountDownLatch((int) threadNum);
            int threadID = 0;

            long h = 0;
            while (h < fileSize) {
                long length = bufSize;
                boolean finished = false;

                if ((h == 0) && (length + LEN_EXTEND < Integer.MAX_VALUE)) { // add a bit of extra bytes to first thread to avoid generating new thread for the remainder
                    length += LEN_EXTEND; // arbitary bytes to guarantee a newline somewhere
                }
                if (h + length > fileSize) { // past the end
                    length = fileSize - h;
                    finished = true;
                }

                MappedByteBuffer mbuf = mraf.getChannel().map(FileChannel.MapMode.READ_ONLY, h, length);
                int mbs = mbuf.capacity();

                // check for last newline and split there, anything after goes to next buffer
                if (!finished) {
                    for (int i = mbs - 1; true; i--) {
                        byte cur = mbuf.get(i - 1);
                        if (cur == 10) { // \n
                            mbs = i;
                            break;
                        }
                    }
                }

                ThreadedParser tpThr = new ThreadedParser(mbuf, mbs, threadOut, threadID, tpLatch);
                tpThr.start();

                h += mbs;
                threadID++;
            }

            try {
                tpLatch.await();
            }
            catch (InterruptedException ex) {
                System.exit(1);
            }

            // use treemap to sort and uniquify
            Map<String, Boolean> staMap = new TreeMap<>();
            for (int i = 0; i < threadID; i++) {
                for (int j = 0; j < threadOut[i].numStations; j++) {
                    staMap.put(threadOut[i].staArr[j], false);
                }
            }

            boolean started = false;
            String out = "{";
            for (String i : staMap.keySet()) {
                if (started) {
                    out += ", ";
                }
                else {
                    started = true;
                }

                byte[] strBuf = i.getBytes(StandardCharsets.UTF_8);

                int hash = calc_hash(strBuf, strBuf.length);
                MeasurementAggregator mSum = new MeasurementAggregator();
                for (int j = 0; j < threadID; j++) {
                    MeasurementAggregator ma = threadOut[j].hashSpace[hash];

                    while (true) {
                        if ((strBuf.length != ma.station.length) || (Arrays.compare(strBuf, ma.station) != 0)) { // hash collision
                            ma = ma.next;
                            continue;
                        }
                        else { // hit
                            if (ma.min < mSum.min) {
                                mSum.min = ma.min;
                            }
                            if (ma.max > mSum.max) {
                                mSum.max = ma.max;
                            }
                            mSum.sum += ma.sum;
                            mSum.count += ma.count;
                            break;
                        }
                    }
                }
                double min = Math.round(Double.valueOf(mSum.min)) / 10.0;
                double avg = Math.round(Double.valueOf(mSum.sum) / Double.valueOf(mSum.count)) / 10.0;
                double max = Math.round(Double.valueOf(mSum.max)) / 10.0;
                out += i + "=" + min + "/" + avg + "/" + max;
            }
            out += "}\n";
            System.out.print(out);

            mraf.getChannel().close();
            mraf.close();
        }
        catch (IOException ex) {
            System.exit(1);
        }
        System.exit(0);
    }
}
