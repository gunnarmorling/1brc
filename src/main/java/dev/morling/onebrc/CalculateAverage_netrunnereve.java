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
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Arrays;

public class CalculateAverage_netrunnereve {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private String station = "";
        private short min = Short.MAX_VALUE;
        private short max = Short.MIN_VALUE;
        private long sum = 0;
        private int count = 0;
    }

    public static void main(String[] args) {
        try {
            BufferedReader filBuf = new BufferedReader(new FileReader(FILE));
            String line = filBuf.readLine();

            MeasurementAggregator[] maArr = new MeasurementAggregator[10000];
            int maCnt = 0;

            while (line != null) {
                String[] linSpl = line.split(";", 2); // station, measurement
                String station = linSpl[0];

                MeasurementAggregator ma = null;
                for (int i = 0; i < maCnt; i++) {
                    if (station.equals(maArr[i].station)) {
                        ma = maArr[i];
                        break;
                    }
                }
                if (ma == null) {
                    ma = new MeasurementAggregator();
                    ma.station = station;
                    maArr[maCnt] = ma;
                    maCnt++;
                }

                short tempa = Short.parseShort(linSpl[1].replace(".", "")); // x10
                if (tempa < ma.min) {
                    ma.min = tempa;
                }
                if (tempa > ma.max) {
                    ma.max = tempa;
                }
                ma.sum += tempa;
                ma.count++;

                line = filBuf.readLine();
            }

            /*
             * String[] staArr = new String[staHash.size()];
             * int j = 0;
             * for (String i : staHash.keySet()) {
             * staArr[j] = i;
             * j++;
             * }
             * Arrays.sort(staArr);
             * 
             * String out = "{";
             * for (int i = 0; i < staHash.size(); i++) {
             * MeasurementAggregator ma = staHash.get(staArr[i]);
             * double min = Math.round(Double.valueOf(ma.min)) / 10.0;
             * double avg = Math.round(Double.valueOf(ma.sum) / Double.valueOf(ma.count)) / 10.0;
             * double max = Math.round(Double.valueOf(ma.max)) / 10.0;
             * out += staArr[i] + "=" + min + "/" + avg + "/" + max;
             * if (i != (staHash.size() - 1)) {
             * out += ", ";
             * }
             * }
             * out += "}\n";
             * System.out.print(out);
             */

            filBuf.close();
        }
        catch (IOException ex) {
            System.exit(1);
        }
        System.exit(0);
    }
}
