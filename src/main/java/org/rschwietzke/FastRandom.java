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
package org.rschwietzke;

/**
 * Ultra-fast pseudo random generator that is not synchronized!
 * Don't use anything from Random by inheritance, this will inherit
 * a volatile! Not my idea, copyied in parts some demo random
 * generator lessons.
 *
 * @author rschwietzke
 *
 */
public class FastRandom {
    private long seed;

    public FastRandom() {
        this.seed = System.currentTimeMillis();
    }

    public FastRandom(long seed) {
        this.seed = seed;
    }

    protected int next(int nbits) {
        // N.B. Not thread-safe!
        long x = this.seed;
        x ^= (x << 21);
        x ^= (x >>> 35);
        x ^= (x << 4);
        this.seed = x;

        x &= ((1L << nbits) - 1);

        return (int) x;
    }

    /**
     * Borrowed from the JDK
     *
     * @param bound
     * @return
     */
    public int nextInt(int bound) {
        int r = next(31);
        int m = bound - 1;
        if ((bound & m) == 0) // i.e., bound is a power of 2
            r = (int) ((bound * (long) r) >> 31);
        else {
            for (int u = r; u - (r = u % bound) + m < 0; u = next(31))
                ;
        }
        return r;
    }

    /**
     * Borrowed from the JDK
     * @return
     */
    public int nextInt() {
        return next(32);
    }
}
