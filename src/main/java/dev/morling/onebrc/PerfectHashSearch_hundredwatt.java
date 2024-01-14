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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

/** Offline script used to find the perfect hash seed for CalculateAverage_hundredwatt. */
public class PerfectHashSearch_hundredwatt {
    public static final int DESIRED_SLOTS = 5003;
    public static final int N_THREADS = Runtime.getRuntime().availableProcessors() - 1;

    public static void main(String[] args) throws IOException, InterruptedException {
        AtomicLong magicSeed = new AtomicLong(0);
        AtomicLong totalAttempts = new AtomicLong(0);
        AtomicLong maxCardinality = new AtomicLong(0);

        long start = System.currentTimeMillis();

        System.out.println("Searching for perfect hash seed for " + DESIRED_SLOTS + " slots");

        // Figure out encoding for all possible temperature values (1999 total)
        Map<Long, Short> decodeTemperatureMap = new HashMap<>();
        for (short i = -999; i <= 999; i++) {
            long word = 0;
            int shift = 0;
            if (i < 0) {
                word |= ((long) '-') << shift;
                shift += 8;
            }
            if (Math.abs(i) >= 100) {
                int hh = Math.abs(i) / 100;
                int tt = (Math.abs(i) - hh * 100) / 10;

                word |= ((long) (hh + '0')) << shift;
                shift += 8;
                word |= ((long) (tt + '0')) << shift;
            }
            else {
                int tt = Math.abs(i) / 10;
                // convert to ascii
                word |= ((long) (tt + '0')) << shift;
            }
            shift += 8;
            word |= ((long) '.') << shift;
            shift += 8;
            int uu = Math.abs(i) % 10;
            word |= ((long) (uu + '0')) << shift;

            // 31302e3000000000
            decodeTemperatureMap.put(word, i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);

        RandomGeneratorFactory factory = RandomGeneratorFactory.of("L64X256MixRandom");

        Runnable search = () -> {
            // Brute force to find seed:
            // generate a cryptographically secure random seed
            RandomGenerator rand;
            try {
                byte[] seed = new byte[16];
                SecureRandom.getInstanceStrong().nextBytes(seed);
                rand = factory.create(ByteBuffer.wrap(seed).getLong());
                System.out.println(Thread.currentThread().getName() + " | Using seed: " + rand.nextLong());
            }
            catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            int max = 0;
            int attempts = 0;
            while (true) {
                BitSet bs = new BitSet(DESIRED_SLOTS);
                var seed = rand.nextLong();
                seed |= 0b1; // make sure it's odd
                for (var word : decodeTemperatureMap.keySet()) {
                    var h = (word * seed) & ~(1L << 63);
                    var pos = (int) (h % DESIRED_SLOTS);
                    bs.set(pos);
                }
                var c = bs.cardinality();
                if (c == decodeTemperatureMap.size()) {
                    System.out.println("FOUND seed: " + seed + " cardinality: " + c + " max cardinality: " + max);
                    magicSeed.set(seed);
                    return;
                }
                max = Math.max(max, c);
                if (attempts % 100_000 == 0) {
                    if (magicSeed.get() != 0)
                        return;
                    int finalMax = max;
                    long currentMaxCardinality = maxCardinality.updateAndGet(currentMax -> Math.max(currentMax, finalMax));
                    long currentTotalAttempts = totalAttempts.addAndGet(100_000);

                    if (Thread.currentThread().getName().endsWith("-1"))
                        System.out.println(Thread.currentThread().getName() + " | max cardinality: " + currentMaxCardinality + " attempts: "
                                + String.format("%,d", currentTotalAttempts));
                }
                attempts++;
            }
        };

        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            executor.submit(search);
        }

        // Wait for the search to complete
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

        short[] TEMPERATURES = new short[DESIRED_SLOTS];
        long seed = magicSeed.get();

        decodeTemperatureMap.entrySet().stream().forEach(e -> {
            var word = e.getKey();
            var h = (word * seed) & ~(1L << 63);
            var pos = (int) (h % DESIRED_SLOTS);
            if (TEMPERATURES[pos] != 0)
                throw new RuntimeException("collision at " + pos);
            TEMPERATURES[pos] = e.getValue();
        });
        System.out.println("SUCCESS seed: " + seed + " total attempts: " + totalAttempts.get());

        try {
            File file = new File("seeds.txt");
            file.delete();
            file.createNewFile();

            // Write the seed to seeds.txt
            FileWriter myWriter = new FileWriter("seeds.txt");
            myWriter.write(Long.toString(seed));
            myWriter.write("\n");
            myWriter.close();

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Search took " + ((System.currentTimeMillis() - start) / 1000) + "s");
    }
}
