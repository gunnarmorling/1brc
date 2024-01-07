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
            MappedByteBuffer mbuf = mraf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, mraf.getChannel().size());

            HashMap<String, MeasurementAggregator> staHash = new HashMap<String, MeasurementAggregator>();

            int mbs = mbuf.capacity();
            boolean state = false; // 0 for station pickup, 1 for measurement pickup
            boolean negate = false;
            int head = 0;
            int tempCnt = 0;
            byte[] scratch = new byte[50]; // this will be auto-enlarged if necessary
            MeasurementAggregator ma = null;

            for (int i = 0; i < mbs; i++) {
                byte cur = mbuf.get(i);
                if (cur == 59) { // ascii ;
                    int len = i - head;
                    if (scratch.length < len) { // enlarge scratch if it's too small for us
                        scratch = new byte[scratch.length * 10];
                    }

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
                else if (cur == 10) { // ascii \n
                    state = false;
                    negate = false;
                    head = i + 1;
                    tempCnt = 0;
                }
                else if (state == true) {
                    if (cur == 46) { // ascii .
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

            String[] staArr = new String[staHash.size()];
            int j = 0;
            for (String i : staHash.keySet()) {
                staArr[j] = i;
                j++;
            }
            Arrays.sort(staArr);

            String out = "{";
            for (int i = 0; i < staHash.size(); i++) {
                ma = staHash.get(staArr[i]);
                double min = Math.round(Double.valueOf(ma.min)) / 10.0;
                double avg = Math.round(Double.valueOf(ma.sum) / Double.valueOf(ma.count)) / 10.0;
                double max = Math.round(Double.valueOf(ma.max)) / 10.0;
                out += staArr[i] + "=" + min + "/" + avg + "/" + max;
                if (i != (staHash.size() - 1)) {
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
