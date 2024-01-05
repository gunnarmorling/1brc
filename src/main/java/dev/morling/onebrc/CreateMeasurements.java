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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Random;

public class CreateMeasurements {

    private static final Path MEASUREMENT_FILE = Path.of("./measurements.txt");

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length < 1) {
            System.out.println("Usage: create_measurements.sh <number of records to create> [seed]");
            System.exit(1);
        }

        int size = 0;
        try {
            size = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid value for <number of records to create>");
            System.out.println("Usage: CreateMeasurements <number of records to create> [seed]");
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
                System.out.println("Usage: CreateMeasurements <number of records to create> [seed]");
                System.exit(1);
            }
        }

        List<WeatherStation> stations = WeatherStationFactory.getWeatherStationsList(seed);

        Random random = new Random(seed);
        try (BufferedWriter bw = Files.newBufferedWriter(MEASUREMENT_FILE)) {
            for (int i = 0; i < size; i++) {
                if (i > 0 && i % 50_000_000 == 0) {
                    System.out.printf("Wrote %,d measurements in %s ms%n", i, System.currentTimeMillis() - start);
                }
                WeatherStation station = stations.get(random.nextInt(stations.size()));
                bw.write(station.id);
                bw.write(";");
                bw.write(Double.toString(station.measurement()));
                bw.write('\n');
            }
        }
        System.out.printf("Created file with %,d measurements in %s ms%n", size, System.currentTimeMillis() - start);
    }
}
