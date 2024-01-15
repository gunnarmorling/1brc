package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;

public class CalculateAverage_JuanMorenoDeveloper {
    private static final Path MEASUREMENT_FILES = Path.of("./measurements.txt");

    public static void main(String[] args) throws IOException {
        try (Stream<String> lines = Files.lines(MEASUREMENT_FILES)) {
            var results = lines
                    .parallel()
                    .map(line -> line.split(";"))
                    .map(values -> Map.entry(values[0], Double.parseDouble(values[1])))
                    .collect(groupingBy(Map.Entry::getKey, summarizingDouble(Map.Entry::getValue)))
                    .entrySet()
                    .stream()
                    .collect(Collectors
                            .toMap(Map.Entry::getKey,
                                    entry -> "%.1f/%.1f/%.1f".formatted(entry.getValue().getMin(), entry.getValue().getAverage(), entry.getValue().getMax()),
                                    (o1, o2) -> o1,
                                    TreeMap::new));

            System.out.println(results);
        }
    }
}
