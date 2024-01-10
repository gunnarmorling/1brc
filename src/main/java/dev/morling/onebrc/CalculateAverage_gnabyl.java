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
import java.util.ArrayList;
import java.util.List;

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

	private static record ResultRow(double min, double mean, double max) {
		public String toString() {
			return round(min) + "/" + round(mean) + "/" + round(max);
		}

		private double round(double value) {
			return Math.round(value * 10.0) / 10.0;
		}
	};

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

	private static void processChunk(Chunk chunk) {
		System.out.println("Processing Chunk " + chunk.index() + " in Thread " + Thread.currentThread().getName());

		// Perform processing on the chunk data
		byte[] data = new byte[chunk.bytesCount()];
		chunk.mappedByteBuffer().get(data);
	}

	private static void processAllChunks(List<Chunk> chunks) {
		for (var chunk : chunks) {
			processChunk(chunk);
		}
	}

	public static void main(String[] args) throws IOException {
		var chunks = readChunks(NB_CHUNKS);
		processAllChunks(chunks);
	}
}
