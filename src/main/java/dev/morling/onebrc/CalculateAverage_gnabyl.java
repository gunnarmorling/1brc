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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_gnabyl {

	private static final String FILE = "./measurements-saved.txt";

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

	private static List<Chunk> readChunks(long nbChunks) {
		try (RandomAccessFile file = new RandomAccessFile(FILE, "rw")) {
			List<Chunk> res = new ArrayList<>();
			FileChannel channel = file.getChannel();
			long bytesCount = channel.size();
			long bytesPerChunk = bytesCount / nbChunks;

			// Memory map the file in read-only mode
			// TODO: Optimize using threads
			for (int i = 0; i < nbChunks; i++) {
				long position = i * bytesPerChunk;
				int size = (int) ((i == nbChunks - 1) ? (bytesCount - position) : bytesPerChunk);

				MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, position, size);

				res.add(new Chunk(i, position, size, mappedByteBuffer));
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
		var chunkBuffer = chunk.mappedByteBuffer().get(data);
	}

	private static void processChunksInParallel(List<Chunk> chunks) {
		ExecutorService executorService = Executors.newFixedThreadPool(chunks.size());

		try {
			// Submit tasks to process each chunk
			for (var chunk : chunks) {
				executorService.submit(() -> processChunk(chunk));
			}

			// Shutdown the executor and wait for all tasks to complete
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		var chunks = readChunks(NB_CHUNKS);
		processChunksInParallel(chunks);
	}
}
