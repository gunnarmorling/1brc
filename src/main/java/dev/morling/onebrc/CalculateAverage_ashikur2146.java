package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class CalculateAverage_ashikur2146 {
    private static final String FILE_PATH = "./measurements.txt";

    public static void main(String[] args) {
        try {
            Map<String, TemperatureStats> stationStats = processFile(FILE_PATH);
            System.out.println(stationStats);

        }
        catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, TemperatureStats> processFile(String filePath) throws IOException {
        try {
            Path path = Paths.get(filePath);

            ConcurrentMap<String, TemperatureStats> stationStats = Files.lines(path).parallel()
                    .map(line -> line.split(";")).filter(parts -> parts.length == 2)
                    .collect(Collectors.toConcurrentMap(parts -> parts[0],
                            parts -> new TemperatureStats(Double.parseDouble(parts[1])), TemperatureStats::merge));

            return new TreeMap<>(stationStats);

        }
        catch (IOException e) {
            throw new IOException("Error reading file: " + e.getMessage(), e);
        }
    }

}

record TemperatureStats(double min, double mean, double max, int count) {

    public TemperatureStats(double temperature) {
		this(temperature, temperature, temperature, 1);
	}

    public TemperatureStats merge(TemperatureStats other) {
        double newMin = Math.min(min, other.min);
        double newMax = Math.max(max, other.max);
        double newMean = ((mean * count) + (other.mean * other.count)) / (count + other.count);
        return new TemperatureStats(newMin, newMean, newMax, count + other.count);
    }

    @Override
    public String toString() {
        return String.format("%.1f/%.1f/%.1f", min, mean, max);
    }
}