/*
 *  Copyright 2024 The original authors
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class CalculateAverage_yehwankim23 {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("./measurements.txt"));
        HashMap<String, Measurement> hm = new HashMap<>();

        for (String line = br.readLine(); line != null; line = br.readLine()) {
            StringTokenizer st = new StringTokenizer(line, ";");
            String stationName = st.nextToken();

            if (hm.containsKey(stationName)) {
                hm.get(stationName).update(Double.parseDouble(st.nextToken()));
                continue;
            }

            hm.put(stationName, new Measurement(Double.parseDouble(st.nextToken())));
        }

        br.close();
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        bw.write(new TreeMap<>(hm).toString());
        bw.newLine();
        bw.flush();
        bw.close();
    }

    private static class Measurement {
        public double min;
        public double sum;
        public int count;
        public double max;

        public Measurement(double measurement) {
            min = measurement;
            sum = measurement;
            count = 1;
            max = measurement;
        }

        public void update(double measurement) {
            if (measurement < min) {
                min = measurement;
            }

            sum += measurement;
            count++;

            if (max < measurement) {
                max = measurement;
            }
        }

        @Override
        public String toString() {
            return min + "/" + Math.round(sum / count * 10) / 10.0 + "/" + max;
        }
    }
}
