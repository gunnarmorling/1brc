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
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.rschwietzke.FastRandom;

public class WeatherStationFactory {

    private static final int MAX_NAME_LEN = 100;
    private static final int KEYSET_SIZE = 10_000;

    public static List<WeatherStation> getRandomWeatherStationsList() throws Exception {
        return WeatherStationFactory
                .getWeatherStationsList(java.util.concurrent.ThreadLocalRandom.current().nextLong());
    }

    public static List<WeatherStation> getWeatherStationsList(long seed) throws Exception {
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
            final var rnd = new FastRandom(seed);
            final var nameSource = new StringReader(bigName.toString());
            final var buf = new char[MAX_NAME_LEN];
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
                    // TODO: Reset nameSource and read instead of throwing?
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
                int nameByteLen = name.getBytes(StandardCharsets.UTF_8).length;

                while (names.contains(name) || nameByteLen > 100) {
                    // If the character length is over 100
                    if (nameByteLen > 100) {
                        nameBuf.deleteCharAt(nameBuf.length() - 1);
                        if (Character.isWhitespace(nameBuf.charAt(nameBuf.length() - 1))) {
                            nameBuf.setCharAt(nameBuf.length() - 1, readNonSpace(nameSource));
                        }
                    }
                    // Else: name is not unique
                    else {
                        nameBuf.setCharAt(rnd.nextInt(nameBuf.length()), readNonSpace(nameSource));
                    }

                    name = nameBuf.toString();
                    nameByteLen = name.getBytes(StandardCharsets.UTF_8).length;
                }

                if (name.indexOf(';') != -1) {
                    throw new Exception("Station name contains a semicolon!");
                }

                names.add(name);
                minLen = Integer.min(minLen, nameByteLen);
                maxLen = Integer.max(maxLen, nameByteLen);
                var lat = Double.parseDouble(row.substring(row.indexOf(';') + 1));
                // Guesstimate mean temperature using cosine of latitude
                var avgTemp = (double) (30 * Math.cos(Math.toRadians(lat))) - 10;
                weatherStations.add(new WeatherStation(seed, name, avgTemp));
            }
        }
        System.out.format("Generated %,d station names with length from %,d to %,d%n", KEYSET_SIZE, minLen, maxLen);
        return weatherStations;
    }

    private static void skipComments(BufferedReader rows) throws IOException {
        while (rows.readLine().startsWith("#")) {
        }
    }

    private static char readNonSpace(StringReader nameSource) throws Exception {
        char c;
        do {
            var n = nameSource.read();
            if (n == -1) {
                // TODO: Reset nameSource and read instead of throwing?
                throw new Exception("Name source exhausted");
            }
            c = (char) n;
        } while (Character.isWhitespace(c));

        return c;
    }
}