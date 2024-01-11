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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BinaryOperator;

public class CalculateAverage_gnabyl {

	private static final String FILE = "./measurements.txt";

	private static final int NB_CHUNKS = 8;

	private static record Chunk(long start, int bytesCount, MappedByteBuffer mappedByteBuffer) {
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

					res.add(new Chunk(currentPosition, realSize, mappedByteBuffer));
					break;
				}

				// Adjust size so that it ends on a newline
				realSize = reduceSizeToFitLineBreak(channel, currentPosition, startSize);

				MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPosition,
						realSize);

				res.add(new Chunk(currentPosition, realSize, mappedByteBuffer));
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
			System.out.println("}");
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
		byte[] lineBytes;
		String stationName;
		Double value;
		int iSplit, iEol;
		for (int offset = 0; offset < chunk.bytesCount(); offset++) {
			// Find station name
			for (iSplit = offset; iSplit < chunk.bytesCount() && data[iSplit] != ';'; iSplit++) {
			}
			lineBytes = new byte[iSplit - offset];
			for (int i = offset; i < iSplit; i++) {
				lineBytes[i - offset] = data[i];
			}
			stationName = new String(lineBytes, StandardCharsets.UTF_8);

			// Find value
			iSplit++;
			for (iEol = iSplit; iEol < chunk.bytesCount() && data[iEol] != '\n'; iEol++) {
			}
			lineBytes = new byte[iEol - iSplit];
			for (int i = iSplit; i < iEol; i++) {
				lineBytes[i - iSplit] = data[i];
			}
			value = Double.parseDouble(new String(lineBytes, StandardCharsets.UTF_8));

			// Init & count
			if (!result.getSum().containsKey(stationName)) {
				result.getCount().put(stationName, 1L);
				result.getSum().put(stationName, value);
				result.getMin().put(stationName, value);
				result.getMax().put(stationName, value);
			} else {
				// Sum
				var currentSum = result.getSum().get(stationName);
				result.getSum().put(stationName, currentSum + value);
				// Count
				var currentCount = result.getCount().get(stationName);
				result.getCount().put(stationName, currentCount + 1);
				// Min
				var currentMin = result.getMin().get(stationName);
				result.getMin().put(stationName,
						currentMin > value ? value : currentMin);
				// Max
				var currentMax = result.getMax().get(stationName);
				result.getMax().put(stationName,
						currentMax < value ? value : currentMax);
			}

			offset = iEol;
		}

		return result;
	}

	private static ChunkResult processAllChunks(List<Chunk> chunks) {
		// var globalRes = new ChunkResult();
		// for (var chunk : chunks) {
		// var chunkRes = processChunk(chunk);
		// globalRes.mergeWith(chunkRes);
		// }
		// return globalRes;

		List<CompletableFuture<ChunkResult>> computeTasks = new ArrayList<>();

		for (Chunk chunk : chunks) {
			computeTasks.add(CompletableFuture.supplyAsync(() -> processChunk(chunk)));
		}

		ChunkResult globalRes = new ChunkResult();

		for (CompletableFuture<ChunkResult> completedTask : computeTasks) {
			try {
				ChunkResult chunkRes = completedTask.get();
				globalRes.mergeWith(chunkRes);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace(); // Handle exceptions if needed
			}
		}

		return globalRes;
	}

	public static void main(String[] args) throws IOException {
		var chunks = readChunks(NB_CHUNKS);
		var result = processAllChunks(chunks);
		result.print();
	}
}
