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
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CreateMeasurements {

    private static final String FILE = "./measurements.txt";

    private record WeatherStation(String id, double meanTemperature) {
        double measurement() {
            double m = ThreadLocalRandom.current().nextGaussian(meanTemperature, 10);
            return Math.round(m * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("Usage: create_measurements.sh <number of records to create>");
            System.exit(1);
        }

        int size = 0;
        try {
            size = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid value for <number of records to create>");
            System.out.println("Usage: CreateMeasurements <number of records to create>");
            System.exit(1);
        }

        List<WeatherStation> stations = Arrays.asList(
                new WeatherStation("auckland", 15.2),
                new WeatherStation("concordia", -51.7),
                new WeatherStation("lima", 19.2),
                new WeatherStation("hamburg", 9.4),
                new WeatherStation("hammerfest", 2.7),
                new WeatherStation("maui", 15.2),
                new WeatherStation("miami", 24.5),
                new WeatherStation("nairobi", 17.8),
                new WeatherStation("newdelhi", 25.3),
                new WeatherStation("tokio", 15.8));

        File measurements = new File(FILE);
        try (FileOutputStream fos = new FileOutputStream(measurements); BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));) {
            for (int i = 0; i < size; i++) {
                if (i > 0 && i % 50_000_000 == 0) {
                    System.out.println("Wrote %,d measurements in %s ms".formatted(i, System.currentTimeMillis() - start));
                }
                WeatherStation station = stations.get(ThreadLocalRandom.current().nextInt(stations.size()));
                bw.write(station.id());
                bw.write(";" + station.measurement());
                bw.newLine();
            }
            bw.flush();

            System.out.println("Created file with %,d measurements in %s ms".formatted(size, System.currentTimeMillis() - start));
        }
    }
}
