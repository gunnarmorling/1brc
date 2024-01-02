package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_lluismf {

	private static class Aggregate {
		int count;
		double min;
		double max;
		double total;

		@Override
		public String toString() {
			return String.format("%.1f/%.1f/%.1f", min, max, total / count);
		}
	}

	public static void main(String[] args) throws IOException {

		long startTime = System.currentTimeMillis();

		Map<String, Aggregate> aggregates = new TreeMap();

		try (BufferedReader reader = Files.newBufferedReader(Paths.get("measures.txt"))) {
			String line;

			while ((line = reader.readLine()) != null) {
				int pos = line.indexOf(";");
				String city = line.substring(0, pos);
				double measure = Double.valueOf(line.substring(pos + 1));

				Aggregate aggregate = aggregates.get(city);
				if (aggregate == null) {
					aggregate = new Aggregate();
					aggregate.count = 0;
					aggregate.min = Double.MAX_VALUE;
					aggregate.max = Double.MIN_VALUE;
					aggregate.total = 0;
					aggregates.put(city, aggregate);
				}

				if (measure <= aggregate.min) {
					aggregate.min = measure;
				} else if (measure >= aggregate.max) {
					aggregate.max = measure;
				}
				aggregate.count = aggregate.count + 1;
				aggregate.total = aggregate.total + measure;
			}

			System.out.println(aggregates);

			System.out.println("Time ellapsed (milliseconds): " + (System.currentTimeMillis() - startTime));

		}
	}
}
