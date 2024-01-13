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

import org.rschwietzke.FastRandom;
import org.rschwietzke.CheaperCharBuffer;

public class WeatherStation {
    final static char[] NUMBERS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    public final String id;
    public final char[] idChars;
    public final double meanTemperature;

    private final FastRandom fastRandom;

    public WeatherStation(long seed, String id, double meanTemperature) {
        this.id = id;
        this.meanTemperature = meanTemperature;
        this.fastRandom = new FastRandom(((long) id.hashCode()) ^ seed);
        this.idChars = id.toCharArray();
    }

    private int nextWholePart() {
        // fake -10.9 to +10.9 variance without double operations and rounding
        // gives us -10 to +10
        return (int) this.meanTemperature + (this.fastRandom.nextInt(21) - 10);
    }

    private int nextDecimalPart() {
        return this.fastRandom.nextInt(10);
    }

    public double measurement() {
        double whole = (double) this.nextWholePart();
        double decimal = (double) this.nextDecimalPart();

        if (whole < 0) {
            return whole - (decimal / 10.0);
        }
        else {
            return whole + (decimal / 10.0);
        }
    }

    /**
     * We write out data into the buffer to avoid string conversion
     * We also no longer use double and gaussian, because for our
     * purpose, the fake numbers here will do it. Less
     *
     * @param buffer the buffer to append to
     */
    public void measurement(final CheaperCharBuffer buffer) {
        int m = this.nextWholePart();
        // gives us a decimal digit 0 to 9 as char
        char d = NUMBERS[this.nextDecimalPart()];

        // just append, only one number has to be converted and we can do
        // better... if we watn
        buffer.append(String.valueOf(m)).append('.').append(d);
    }
}