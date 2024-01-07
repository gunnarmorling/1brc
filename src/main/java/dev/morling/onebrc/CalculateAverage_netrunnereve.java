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

            byte[] scratch = new byte[50]; // this will be auto-enlarged if necessary
            MeasurementAggregator ma = null;

            long h = 0;
            while (h < fileSize) {
                long end = bufSize;
                boolean finished = false;
                if (h + end > fileSize) {
                    end = fileSize - h;
                    finished = true;
                }

                MappedByteBuffer mbuf = mraf.getChannel().map(FileChannel.MapMode.READ_ONLY, h, end);
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
                boolean negate = false;
                int head = 0;
                int tempCnt = 0;

                h += mbs;
                for (int i = 0; i < mbs; i++) {
                    byte cur = mbuf.get(i);
                    if (cur == 59) { // ;
                        int len = i - head;
                        if (scratch.length < len) { // enlarge scratch if it's too small for us
                            scratch = new byte[scratch.length * 10];
                        }

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
                    else if (cur == 10) { // \n
                        state = false;
                        negate = false;
                        head = i + 1;
                        tempCnt = 0;
                    }
                    else if (state == true) {
                        if (cur == 46) { // .
                            int tempa = mbuf.get(i + 1) - 48;
                            if (tempCnt == 2) { // tens
                                tempa += (scratch[0] - 48) * 100 + (scratch[1] - 48) * 10;
                            }
                            else { // ones
                                tempa += (scratch[0] - 48) * 10;
                            }
                            if (negate) {
                                tempa *= -1;
                            }

                            if (tempa < ma.min) {
                                ma.min = tempa;
                            }
                            if (tempa > ma.max) {
                                ma.max = tempa;
                            }
                            ma.sum += tempa;
                            ma.count++;
                        }
                        else if (cur == 45) { // ascii -
                            negate = true;
                        }
                        else {
                            scratch[tempCnt] = cur;
                            tempCnt++;
                        }
                    }
                }
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
