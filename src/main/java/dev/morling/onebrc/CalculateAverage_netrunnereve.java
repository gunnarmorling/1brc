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
import java.util.TreeMap;

public class CalculateAverage_netrunnereve {

    private static final String FILE = "./measurements.txt";

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private int count = 0;
    }

    public static void main(String[] args) {
        try {
            TreeMap<String, MeasurementAggregator> staTree = new TreeMap<String, MeasurementAggregator>();

            BufferedReader filBuf = new BufferedReader(new FileReader(FILE));
            String line = filBuf.readLine();

            while (line != null) {
                String[] linSpl = line.split(";", 2); // station, measurement
                String station = linSpl[0];

                MeasurementAggregator ma = staTree.get(station);
                if (ma == null) {
                    ma = new MeasurementAggregator();
                }

                double tempa = Double.parseDouble(linSpl[1]);
                if (tempa < ma.min) {
                    ma.min = tempa;
                }
                if (tempa > ma.max) {
                    ma.max = tempa;
                }
                ma.sum += tempa;
                ma.count++;

                staTree.put(linSpl[0], ma);

                line = filBuf.readLine();
            }

            String out = "{";
            for (String i : staTree.keySet()) {
                MeasurementAggregator ma = staTree.get(i);
                double avg = Math.round(ma.sum / ma.count * 10.0) / 10.0;
                out += i + "=" + ma.min + "/" + avg + "/" + ma.max + ", ";
            }
            out = out.replaceAll(", $", "");
            out += "}\n";
            System.out.print(out);

            filBuf.close();
        }
        catch (IOException ex) {
            System.exit(1);
        }
        System.exit(0);
    }
}
