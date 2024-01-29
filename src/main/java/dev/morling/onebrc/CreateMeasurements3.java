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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

public class CreateMeasurements3 {

    public static final int MAX_NAME_LEN = 100;
    public static final int KEYSET_SIZE = 10_000;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: create_measurements3.sh <number of records to create>");
            System.exit(1);
        }
        int size = 0;
        try {
            size = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.out.println("Invalid value for <number of records to create>");
            System.out.println("Usage: create_measurements3.sh <number of records to create>");
            System.exit(1);
        }
        final var weatherStations = generateWeatherStations();
        final var start = System.currentTimeMillis();
        final var rnd = ThreadLocalRandom.current();
        try (var out = new BufferedWriter(new FileWriter("measurements3.txt"))) {
            for (int i = 1; i <= size; i++) {
                var station = weatherStations.get(rnd.nextInt(KEYSET_SIZE));
                double temp = rnd.nextGaussian(station.avgTemp, 7.0);
                out.write(station.name);
                out.write(';');
                out.write(Double.toString(Math.round(temp * 10.0) / 10.0));
                out.write('\n');
                if (i % 50_000_000 == 0) {
                    System.out.printf("Wrote %,d measurements in %,d ms%n", i, System.currentTimeMillis() - start);
                }
            }
        }
    }

    record WeatherStation(String name, float avgTemp) {
    }

    private static ArrayList<WeatherStation> generateWeatherStations() throws Exception {
        // Use a public list of city names and concatenate them all into a long string,
        // which we'll use as a "source of city name randomness"
        var bigName = new StringBuilder(1 << 20);
        try (var rows = new BufferedReader(new FileReader("data/weather_stations.csv"));) {
            skipComments(rows);
            while (true) {
                var row = rows.readLine();
                if (row == null) {
                    break;
                }
                bigName.append(row, 0, row.indexOf(';'));
            }
        }
        final var weatherStations = new ArrayList<WeatherStation>();
        final var names = new HashSet<String>();
        var minLen = Integer.MAX_VALUE;
        var maxLen = Integer.MIN_VALUE;
        try (var rows = new BufferedReader(new FileReader("data/weather_stations.csv"))) {
            skipComments(rows);
            final var nameSource = new StringReader(bigName.toString());
            final var buf = new char[MAX_NAME_LEN];
            final var rnd = ThreadLocalRandom.current();
            final double yOffset = 4;
            final double factor = 2500;
            final double xOffset = 0.372;
            final double power = 7;
            for (int i = 0; i < KEYSET_SIZE; i++) {
                var row = rows.readLine();
                if (row == null) {
                    break;
                }
                // Use a 7th-order curve to simulate the name length distribution.
                // It gives us mostly short names, but with large outliers.
                var nameLen = (int) (yOffset + factor * Math.pow(rnd.nextDouble() - xOffset, power));
                var count = nameSource.read(buf, 0, nameLen);
                if (count == -1) {
                    throw new Exception("Name source exhausted");
                }
                var nameBuf = new StringBuilder(nameLen);
                nameBuf.append(buf, 0, nameLen);
                if (Character.isWhitespace(nameBuf.charAt(0))) {
                    nameBuf.setCharAt(0, readNonSpace(nameSource));
                }
                if (Character.isWhitespace(nameBuf.charAt(nameBuf.length() - 1))) {
                    nameBuf.setCharAt(nameBuf.length() - 1, readNonSpace(nameSource));
                }
                var name = nameBuf.toString();
                while (names.contains(name)) {
                    nameBuf.setCharAt(rnd.nextInt(nameBuf.length()), readNonSpace(nameSource));
                    name = nameBuf.toString();
                }
                int actualLen;
                while (true) {
                    actualLen = name.getBytes(StandardCharsets.UTF_8).length;
                    if (actualLen <= 100) {
                        break;
                    }
                    nameBuf.deleteCharAt(nameBuf.length() - 1);
                    if (Character.isWhitespace(nameBuf.charAt(nameBuf.length() - 1))) {
                        nameBuf.setCharAt(nameBuf.length() - 1, readNonSpace(nameSource));
                    }
                    name = nameBuf.toString();
                }
                if (name.indexOf(';') != -1) {
                    throw new Exception("Station name contains a semicolon!");
                }
                names.add(name);
                minLen = Integer.min(minLen, actualLen);
                maxLen = Integer.max(maxLen, actualLen);
                var lat = Float.parseFloat(row.substring(row.indexOf(';') + 1));
                // Guesstimate mean temperature using cosine of latitude
                var avgTemp = (float) (30 * Math.cos(Math.toRadians(lat))) - 10;
                weatherStations.add(new WeatherStation(name, avgTemp));
            }
        }
        System.out.format("Generated %,d station names with length from %,d to %,d%n", KEYSET_SIZE, minLen, maxLen);
        return weatherStations;
    }

    private static void skipComments(BufferedReader rows) throws IOException {
        while (rows.readLine().startsWith("#")) {
        }
    }

    private static char readNonSpace(StringReader nameSource) throws IOException {
        while (true) {
            var n = nameSource.read();
            if (n == -1) {
                throw new IOException("Name source exhausted");
            }
            var ch = (char) n;
            if (ch != ' ') {
                return ch;
            }
        }
    }
}
