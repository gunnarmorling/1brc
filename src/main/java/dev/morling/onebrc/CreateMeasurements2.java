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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.rschwietzke.CheaperCharBuffer;
import org.rschwietzke.FastRandom;

/**
 * Faster version with some data faking instead of a real Gaussian distribution
 * Good enough for our purppose I guess.
 */
public class CreateMeasurements2 {

    private static final String FILE = "./measurements2.txt";

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length < 1) {
            System.out.println("Usage: create_measurements2.sh <number of records to create> [seed]");
            System.exit(1);
        }

        int size = 0;
        try {
            size = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid value for <number of records to create>");
            System.out.println("Usage: CreateMeasurements2 <number of records to create> [seed]");
            System.exit(1);
        }

        // Default seed is 1brc1brc converted to hexadecimal
        long seed = 0x3162726331627263L;
        if (args.length == 2) {
            try {
                seed = Long.parseLong(args[1]);
            }
            catch (NumberFormatException e) {
                System.out.println("Invalid value for [seed]");
                System.out.println("Usage: CreateMeasurements2 <number of records to create> [seed]");
                System.exit(1);
            }
        }

        final List<WeatherStation> stations = WeatherStationFactory.getWeatherStationsList(seed);

        File file = new File(FILE);

        // break the loop and unroll it manually
        int strideSize = 50_000_000;
        int outer = size / strideSize;
        int remainder = size - (outer * strideSize);
        FastRandom random = new FastRandom(seed);

        try (final BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (int i = 0; i < outer; i++) {
                produce(random, bw, stations, strideSize);

                // we avoid a modulo if here and use the stride size to print and update
                System.out.println("Wrote %,d measurements in %s ms".formatted((i + 1) * strideSize, System.currentTimeMillis() - start));
            }
            // there might be a rest
            produce(random, bw, stations, remainder);

            // write fully before taking measurements
            bw.flush();
            System.out.println("Created file with %,d measurements in %s ms".formatted(size, System.currentTimeMillis() - start));
        }
    }

    private static void produce(FastRandom random, BufferedWriter bw, List<WeatherStation> stations, int count) throws IOException {
        final int stationCount = stations.size();
        final int rest = count % 8;

        // write to a fix buffer first, don't create strings ever
        // reuse buffer
        final CheaperCharBuffer sb = new CheaperCharBuffer(200);

        // manual loop unroll for less jumps
        for (int i = 0; i < count; i = i + 8) {
            {
                // try to fill teh cpu pipeline as much as possible with
                // independent operations
                int s1 = random.nextInt(stationCount);
                int s2 = random.nextInt(stationCount);
                int s3 = random.nextInt(stationCount);
                int s4 = random.nextInt(stationCount);
                // get us the ojects one after the other to have the array
                // in our L1 cache and not push it out with other data
                var w1 = stations.get(s1);
                var w2 = stations.get(s2);
                var w3 = stations.get(s3);
                var w4 = stations.get(s4);

                // write our data to our buffer
                sb.append(w1.idChars, 0, w1.idChars.length).append(';');
                w1.measurement(sb);
                sb.append('\n');

                sb.append(w2.idChars, 0, w2.idChars.length).append(';');
                w2.measurement(sb);
                sb.append('\n');

                sb.append(w3.idChars, 0, w3.idChars.length).append(';');
                w3.measurement(sb);
                sb.append('\n');

                sb.append(w4.idChars, 0, w4.idChars.length).append(';');
                w4.measurement(sb);
                sb.append('\n');
            }
            {
                int s1 = random.nextInt(stationCount);
                int s2 = random.nextInt(stationCount);
                int s3 = random.nextInt(stationCount);
                int s4 = random.nextInt(stationCount);
                var w1 = stations.get(s1);
                var w2 = stations.get(s2);
                var w3 = stations.get(s3);
                var w4 = stations.get(s4);
                sb.append(w1.idChars, 0, w1.idChars.length).append(';');
                w1.measurement(sb);
                sb.append('\n');

                sb.append(w2.idChars, 0, w2.idChars.length).append(';');
                w2.measurement(sb);
                sb.append('\n');

                sb.append(w3.idChars, 0, w3.idChars.length).append(';');
                w3.measurement(sb);
                sb.append('\n');

                sb.append(w4.idChars, 0, w4.idChars.length).append(';');
                w4.measurement(sb);
                sb.append('\n');
            }
            // write the buffer directly, no intermediate string copy
            bw.write(sb.data_, 0, sb.length_);

            // reuse buffer, reset only, no cleaning
            sb.clear();
        }

        // there might be a rest to write
        for (int i = 0; i < rest; i++) {
            sb.clear();

            int s = random.nextInt(stationCount);
            var w = stations.get(s);
            sb.append(w.idChars, 0, w.idChars.length).append(';');
            w.measurement(sb);
            sb.append('\n');

            bw.write(sb.data_, 0, sb.length_);
        }
    }
}
