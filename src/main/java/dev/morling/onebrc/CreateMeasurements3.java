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
import java.io.FileWriter;

import org.rschwietzke.FastRandom;

public class CreateMeasurements3 {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: create_measurements3.sh <number of records to create> [seed]");
            System.exit(1);
        }

        int size = 0;
        try {
            size = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid value for <number of records to create>");
            System.out.println("Usage: create_measurements3.sh <number of records to create> [seed]");
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

        final var weatherStations = WeatherStationFactory.getWeatherStationsList(seed);
        final var start = System.currentTimeMillis();
        final var rnd = new FastRandom(seed);
        try (var out = new BufferedWriter(new FileWriter("measurements.txt"))) {
            for (int i = 1; i <= size; i++) {
                var station = weatherStations.get(rnd.nextInt(weatherStations.size()));
                double temp = station.measurement();
                out.write(station.id);
                out.write(';');
                out.write(Double.toString(temp));
                out.write('\n');
                if (i % 50_000_000 == 0) {
                    System.out.printf("Wrote %,d measurements in %,d ms%n", i, System.currentTimeMillis() - start);
                }
            }
        }
    }
}
