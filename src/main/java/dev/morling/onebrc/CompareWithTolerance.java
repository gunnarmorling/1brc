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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class CompareWithTolerance {
    private static final Pattern numRegex = Pattern.compile("-?\\d\\d?\\.\\d");

    record NameAndStats(String name, int[] stats) {
    }

    public static void main(String[] args) throws IOException {
        var fname1 = args[0];
        var fname2 = args[1];
        var entries1 = entries(fname1);
        var entries2 = entries(fname2);
        if (entries1.length != entries2.length) {
            System.out.printf("Files don't match in the number of entries: %,d vs. %,d%n", entries1.length, entries2.length);
            System.exit(1);
        }
        for (int i = 0; i < entries1.length; i++) {
            var nameAndStats1 = parseEntry(entries1[i], i, fname1);
            var nameAndStats2 = parseEntry(entries2[i], i, fname2);
            if (IntStream.range(0, 3)
                    .mapToObj(n -> new double[]{ nameAndStats1.stats[n], nameAndStats2.stats[n] })
                    .anyMatch(pair -> Math.abs((pair[0] - pair[1])) > 1)) {
                System.out.printf("Values don't match in entry #%,d: \"%s\" vs. \"%s\"%n", i, entries1[i], entries2[i]);
                System.exit(1);
            }
        }
    }

    private static String[] entries(String fname) throws IOException {
        var content = new BufferedReader(new FileReader(fname)).readLine();
        if (!content.startsWith("{") || !content.endsWith("}")) {
            System.out.printf("%s isn't in the format \"{entries}\"%n", fname);
            System.exit(1);
        }
        content = content.substring(1, content.length() - 1);
        return content.split(",");
    }

    private static NameAndStats parseEntry(String entry, int indexOfEntry, String fname) {
        var nameAndStats = entry.split("=");
        var stats = nameAndStats[1].split("/");
        if (nameAndStats.length != 2 || stats.length != 3) {
            System.out.printf("Parsing error in %s, entry #%,d: \"%s\"%n", fname, indexOfEntry, entry);
            System.exit(1);
        }
        if (Arrays.stream(stats).anyMatch(val -> !numRegex.matcher(val).matches())) {
            System.out.printf("Parsing error in %s, entry #%,d: \"%s\"%n", fname, indexOfEntry, entry);
            System.exit(1);
        }
        var values = Arrays.stream(stats).mapToInt(i -> parseTemperature(i)).toArray();
        return new NameAndStats(nameAndStats[0], values);
    }

    private static int parseTemperature(String str) {
        final byte minus = (byte) '-';
        final byte zero = (byte) '0';
        final byte dot = (byte) '.';

        var i = 0;
        var ch = str.charAt(i);
        int absValue;
        int sign;
        if (ch == minus) {
            sign = -1;
            ch = str.charAt(++i);
        }
        else {
            sign = 1;
        }
        absValue = ch - zero;
        ch = str.charAt(++i);
        if (ch == dot) {
            ch = str.charAt(++i);
        }
        else {
            absValue = 10 * absValue + (ch - zero);
            ch = str.charAt(i + 2);
        }
        absValue = 10 * absValue + (ch - zero);
        return sign * absValue;
    }
}
