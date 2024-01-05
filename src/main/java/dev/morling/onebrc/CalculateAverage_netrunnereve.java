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

public class CalculateAverage_netrunnereve {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum = 0;
        private int count = 0;
    }

    public static void main(String[] args) {
        try {
            HashMap<String, MeasurementAggregator> staHash = new HashMap<String, MeasurementAggregator>();

            BufferedReader filBuf = new BufferedReader(new FileReader(FILE));
            String line = filBuf.readLine();

            while (line != null) {
                String[] linSpl = line.split(";", 2); // station, measurement
                String station = linSpl[0];
                String temperature = linSpl[1];

                MeasurementAggregator ma = staHash.get(station);
                if (ma == null) {
                    ma = new MeasurementAggregator();
                }

                int tempI = Integer.parseInt(temperature.replace(".", "")); // x10
                if (tempI < ma.min) {
                    ma.min = tempI;
                }
                if (tempI > ma.max) {
                    ma.max = tempI;
                }
                ma.sum += tempI;
                ma.count++;

                staHash.put(station, ma);

                line = filBuf.readLine();
            }

            System.out.print("{");
            for (String i : staHash.keySet()) {
                MeasurementAggregator ma = staHash.get(i);
                float avg = ma.sum / ma.count;
                System.out.print(i + "=" + ma.min + "/" + avg + "/" + ma.max + ", ");
            }
            System.out.print("}\n");

            filBuf.close();
        }
        catch (IOException ex) {
            System.exit(1);
        }
        System.exit(0);
    }
}
