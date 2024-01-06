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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_anandmattikopp {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, StationStatistics> stationStatisticsMap = Files.lines(Paths.get(FILE)).parallel()
                .map(entry -> {
                    String[] tokens = entry.split(";");
                    return new Station(tokens[0], Double.parseDouble(tokens[1]));
                })
                .collect(
                        Collectors.toConcurrentMap(
                                station -> station.stationName,
                                station -> new StationStatistics(station),
                                StationStatistics::merge));

        System.out.println(new TreeMap<>(stationStatisticsMap));
    }

    private record Station(String stationName, double temperature) {
    }

    private record StationStatistics(String stationName, double minTemp, double meanTemp, double maxTemp,
                                     long totalCount) {
        StationStatistics(Station station) {
            //Calling canonical constructor
            this(station.stationName, station.temperature, station.temperature, station.temperature, 1);
        }

        //Merging two stats to create new stats
        public static StationStatistics merge(StationStatistics stats1, StationStatistics stats2) {
            assert stats1.stationName.equals(stats2.stationName);
            return new StationStatistics(
                    stats1.stationName,
                    Math.min(stats1.minTemp, stats2.minTemp), // minimum of both the temp
                    (stats1.meanTemp * stats1.totalCount + stats2.meanTemp * stats2.totalCount) / (stats1.totalCount + stats2.totalCount), // average of both the average temps from stats
                    Math.max(stats1.maxTemp, stats2.maxTemp), // maximum of both the temp
                    stats1.totalCount + stats2.totalCount // increment the totalCount
            );
        }

        @Override
        public String toString() {
            return round(minTemp) + "/" + round(meanTemp) + "/" + round(maxTemp);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
