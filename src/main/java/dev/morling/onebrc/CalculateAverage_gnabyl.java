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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

public class CalculateAverage_gnabyl {

	private static final String FILE = "./measurements.txt";

	private static final int NB_CHUNKS = 8;

	private static record Chunk(int index, long start, int bytesCount, MappedByteBuffer mappedByteBuffer) {
	}

	private static record Measurement(String station, double value) {
		private Measurement(String[] parts) {
			this(parts[0], Double.parseDouble(parts[1]));
		}
	}

	private static int reduceSizeToFitLineBreak(FileChannel channel, long startPosition, int startSize)
			throws IOException {
		long currentPosition = startPosition + startSize - 1;
		int realSize = startSize;

		if (currentPosition >= channel.size()) {
			currentPosition = channel.size() - 1;
			realSize = (int) (currentPosition - startPosition);
		}

		while (currentPosition >= startPosition) {
			channel.position(currentPosition);
			byte byteValue = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition, 1).get();
			if (byteValue == '\n') {
				// found line break
				break;
			}

			realSize--;
			currentPosition--;
		}
		return realSize;
	}

	private static List<Chunk> readChunks(long nbChunks) {
		try (RandomAccessFile file = new RandomAccessFile(FILE, "rw")) {
			List<Chunk> res = new ArrayList<>();
			FileChannel channel = file.getChannel();
			long bytesCount = channel.size();
			long bytesPerChunk = bytesCount / nbChunks;

			// Memory map the file in read-only mode
			// TODO: Optimize using threads
			long currentPosition = 0;
			for (int i = 0; i < nbChunks; i++) {
				int startSize = (int) bytesPerChunk;
				int realSize = startSize;

				if (i == nbChunks - 1) {
					realSize = (int) (bytesCount - currentPosition);
					MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
							realSize);

					res.add(new Chunk(i, currentPosition, realSize, mappedByteBuffer));
					break;
				}

				// Adjust size so that it ends on a newline
				realSize = reduceSizeToFitLineBreak(channel, currentPosition, startSize);

				MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
						realSize);

				res.add(new Chunk(i, currentPosition, realSize, mappedByteBuffer));
				currentPosition += realSize;
			}

			channel.close();
			file.close();

			return res;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return List.of();
	}

	private static class ChunkResult {
		private Map<String, Long> count;
		private Map<String, Double> sum, min, max;

		public ChunkResult() {
			sum = new HashMap<>();
			min = new HashMap<>();
			max = new HashMap<>();
			count = new HashMap<>();
		}

		public Map<String, Double> getSum() {
			return sum;
		}

		public Map<String, Double> getMin() {
			return min;
		}

		public Map<String, Double> getMax() {
			return max;
		}

		public Map<String, Long> getCount() {
			return this.count;
		}

		private double round(double value) {
			return Math.round(value * 10.0) / 10.0;
		}

		public void print() {
			var stationNames = new ArrayList<String>(this.getSum().keySet());
			Collections.sort(stationNames);
			System.out.print("{");
			for (int i = 0; i < stationNames.size() - 1; i++) {
				var name = stationNames.get(i);
				System.out.printf("%s=%.1f/%.1f/%.1f, ", name, round(this.getMin().get(name)),
						round(this.getSum().get(name) / this.getCount().get(name)),
						round(this.getMax().get(name)));
			}
			var name = stationNames.get(stationNames.size() - 1);
			System.out.printf("%s=%.1f/%.1f/%.1f", name, round(this.getMin().get(name)),
					round(this.getSum().get(name) / this.getCount().get(name)),
					round(this.getMax().get(name)));
			System.out.print("}");
		}

		public void mergeWith(ChunkResult other) {
			// Increment the count
			mergeMap(this.count, other.count, Long::sum);

			// Merge sum values
			mergeMap(this.sum, other.sum, Double::sum);

			// Merge min values
			mergeMap(this.min, other.min, Double::min);

			// Merge max values
			mergeMap(this.max, other.max, Double::max);
		}

		private <T> void mergeMap(Map<String, T> target, Map<String, T> source,
				BinaryOperator<T> mergeFunction) {
			for (Map.Entry<String, T> entry : source.entrySet()) {
				String key = entry.getKey();
				T sourceValue = entry.getValue();
				target.merge(key, sourceValue, mergeFunction);
			}
		}
	}

	private static ChunkResult processChunk(Chunk chunk) {
		ChunkResult result = new ChunkResult();

		// Perform processing on the chunk data
		byte[] data = new byte[chunk.bytesCount()];
		chunk.mappedByteBuffer().get(data);

		// Process each line
		for (int offset = 0; offset < chunk.bytesCount(); offset++) {
			int eol;
			for (eol = offset; eol < chunk.bytesCount() && data[eol] != '\n'; eol++) {
			}
			byte[] lineBytes = new byte[eol - offset];
			for (int i = offset; i < eol; i++) {
				lineBytes[i - offset] = data[i];
			}
			String line = new String(lineBytes, StandardCharsets.UTF_8);

			Measurement measurement = new Measurement(line.split(";"));

			// Init & count
			if (!result.getSum().containsKey(measurement.station())) {
				result.getCount().put(measurement.station(), 0L);
				result.getSum().put(measurement.station(), 0.0);
				result.getMin().put(measurement.station(), Double.MAX_VALUE);
				result.getMax().put(measurement.station(), -Double.MAX_VALUE);
			}
			// Sum
			var currentSum = result.getSum().get(measurement.station());
			result.getSum().put(measurement.station(), currentSum + measurement.value());
			// Count
			var currentCount = result.getCount().get(measurement.station());
			result.getCount().put(measurement.station(), currentCount + 1);
			// Min
			var currentMin = result.getMin().get(measurement.station());
			result.getMin().put(measurement.station(),
					currentMin > measurement.value() ? measurement.value() : currentMin);
			// Max
			var currentMax = result.getMax().get(measurement.station());
			result.getMax().put(measurement.station(),
					currentMax < measurement.value() ? measurement.value() : currentMax);

			offset = eol;
		}

		return result;
	}

	private static ChunkResult processAllChunks(List<Chunk> chunks) {
		var globalRes = new ChunkResult();
		for (var chunk : chunks) {
			var chunkRes = processChunk(chunk);
			globalRes.mergeWith(chunkRes);
		}
		return globalRes;
	}

	public static void main(String[] args) throws IOException {
		var chunks = readChunks(NB_CHUNKS);
		var result = processAllChunks(chunks);
		result.print();
	}
}
