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

	private static class StationData {
		private double sum, min, max;
		private long count;

		public StationData(double value) {
			this.count = 1;
			this.sum = value;
			this.min = value;
			this.max = value;
		}

		public void update(double value) {
			this.count++;
			this.sum += value;
			this.min = Math.min(this.min, value);
			this.max = Math.max(this.max, value);
		}

		public double getMean() {
			return sum / count;
		}

		public double getMin() {
			return min;
		}

		public double getMax() {
			return max;
		}

		public void mergeWith(StationData other) {
			this.sum += other.sum;
			this.count += other.count;
			this.min = Math.min(this.min, other.min);
			this.max = Math.max(this.max, other.max);
		}

	}

	private static class ChunkResult {
		private Map<String, StationData> data;

		public ChunkResult() {
			data = new HashMap<>();
		}

		public StationData getData(String name) {
			return data.get(name);
		}

		public void addStation(String name, double value) {
			this.data.put(name, new StationData(value));
		}

		private double round(double value) {
			return Math.round(value * 10.0) / 10.0;
		}

		public void print() {
			var stationNames = new ArrayList<String>(this.data.keySet());
			Collections.sort(stationNames);
			System.out.print("{");
			for (int i = 0; i < stationNames.size() - 1; i++) {
				var name = stationNames.get(i);
				var stationData = data.get(name);
				System.out.printf("%s=%.1f/%.1f/%.1f, ", name, round(stationData.getMin()),
						round(stationData.getMean()),
						round(stationData.getMax()));
			}
			var name = stationNames.get(stationNames.size() - 1);
			var stationData = data.get(name);
			System.out.printf("%s=%.1f/%.1f/%.1f", name, round(stationData.getMin()),
					round(stationData.getMean()),
					round(stationData.getMax()));
			System.out.println("}");
		}

		public void mergeWith(ChunkResult other) {
			for (Map.Entry<String, StationData> entry : other.data.entrySet()) {
				String stationName = entry.getKey();
				StationData otherStationData = entry.getValue();
				StationData thisStationData = this.data.get(stationName);

				if (thisStationData == null) {
					this.data.put(stationName, otherStationData);
				} else {
					thisStationData.mergeWith(otherStationData);
				}
			}
		}
	}

	private static ChunkResult processChunk(Chunk chunk) {
		ChunkResult result = new ChunkResult();

		// Perform processing on the chunk data
		byte[] data = new byte[chunk.bytesCount()];
		chunk.mappedByteBuffer().get(data);

		// Process each line
		String stationName;
		Double value;
		int iSplit, iEol;
		StationData stationData;
		for (int offset = 0; offset < data.length; offset++) {
			// Find station name
			for (iSplit = offset; iSplit < data.length && data[iSplit] != ';'; iSplit++) {
			}
			stationName = new String(data, offset, iSplit - offset, StandardCharsets.UTF_8);

			// Find value
			iSplit++;
			for (iEol = iSplit; iEol < data.length && data[iEol] != '\n'; iEol++) {
			}
			value = Double.parseDouble(new String(data, iSplit, iEol - iSplit, StandardCharsets.UTF_8));

			// Init & count
			stationData = result.getData(stationName);

			if (stationData == null) {
				result.addStation(stationName, value);
			} else {
				stationData.update(value);
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
