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
import java.util.HashMap;
import java.util.Arrays;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class CalculateAverage_netrunnereve {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator { // min, max, sum stored as 0.1/unit
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum = 0;
        private int count = 0;
    }

    public static void main(String[] args) {
        try {
            RandomAccessFile mraf = new RandomAccessFile(FILE, "r");
            long fileSize = mraf.getChannel().size();
            long bufSize = Integer.MAX_VALUE; // Java requirement is <= Integer.MAX_VALUE

            HashMap<String, MeasurementAggregator> staHash = new HashMap<String, MeasurementAggregator>();

            byte[] scratch = new byte[100]; // <= 100 characters in station name
            MeasurementAggregator ma = null;

            long h = 0;
            while (h < fileSize) {
                long length = bufSize;
                boolean finished = false;
                if (h + length > fileSize) {
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

                boolean state = false; // 0 for station pickup, 1 for measurement pickup
                int negMul = 1;
                int head = 0;
                int tempCnt = -1; // 0 if 1 digit measurement, 1 if 2 digit

                for (int i = 0; i < mbs; i++) {
                    byte cur = mbuf.get(i);
                    if (state == true) {
                        if (cur == 46) { // .
                            int tempa = mbuf.get(i + 1) - 48;
                            tempa += (scratch[0] - 48) * (10 + 90 * tempCnt) + (scratch[1] - 48) * (10 * tempCnt);

                            tempa *= negMul;

                            if (tempa < ma.min) {
                                ma.min = tempa;
                            }
                            if (tempa > ma.max) {
                                ma.max = tempa;
                            }
                            ma.sum += tempa;
                            ma.count++;

                            i += 2; // go to start of new line
                            state = false;
                            negMul = 1;
                            head = i + 1;
                            tempCnt = -1;
                        }
                        else if (cur == 45) { // ascii -
                            negMul = -1;
                        }
                        else {
                            scratch[tempCnt + 1] = cur;
                            tempCnt++;
                        }
                    }
                    else if (cur == 59) { // ;
                        int len = i - head;

                        // this is faster than filling scratch immediately after each byte is read
                        mbuf.position(head);
                        mbuf.get(scratch, 0, len);
                        String station = new String(scratch, 0, len, StandardCharsets.UTF_8);

                        ma = staHash.get(station);
                        if (ma == null) {
                            ma = new MeasurementAggregator();
                            staHash.put(station, ma);
                        }

                        state = true;
                        head = i + 1;
                    }
                }
                h += mbs;
            }

            // this is faster than filling staArr during file read
            String[] staArr = new String[staHash.size()];
            int j = 0;
            for (String i : staHash.keySet()) {
                staArr[j] = i;
                j++;
            }
            Arrays.sort(staArr);

            int staH_size = staHash.size();
            String out = "{";
            for (int i = 0; i < staH_size; i++) {
                ma = staHash.get(staArr[i]);
                double min = Math.round(Double.valueOf(ma.min)) / 10.0;
                double avg = Math.round(Double.valueOf(ma.sum) / Double.valueOf(ma.count)) / 10.0;
                double max = Math.round(Double.valueOf(ma.max)) / 10.0;
                out += staArr[i] + "=" + min + "/" + avg + "/" + max;
                if (i != (staH_size - 1)) {
                    out += ", ";
                }
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
