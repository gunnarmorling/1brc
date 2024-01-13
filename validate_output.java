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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class validate_output {
    private static final Pattern numRegex = Pattern.compile("-?\\d\\d?\\.\\d");

    record NameAndStats(String name, int[] stats) {
    }

    public static void main(String[] args) throws IOException {
        boolean lenient;
        int argIndex;
        if (args[0].equals("--lenient")) {
            lenient = true;
            argIndex = 1;
        } else {
            lenient = false;
            argIndex = 0;
        }
        var fnameExpected = args[argIndex];
        var fnameActual = args[argIndex + 1];
        var entriesExpected = entries(fnameExpected);
        var entriesActual = entries(fnameActual);
        if (entriesExpected.length != entriesActual.length) {
            System.out.printf("Files don't match in the number of entries: expected %,d, actual %,d%n", entriesExpected.length, entriesActual.length);
            System.exit(1);
        }
        for (int i = 0; i < entriesExpected.length; i++) {
            var nameAndStatsExpected = parseEntry(entriesExpected[i], i, fnameExpected);
            var nameAndStatsActual = parseEntry(entriesActual[i], i, fnameActual);
            int[] statsExpected = nameAndStatsExpected.stats;
            int[] statsActual = nameAndStatsActual.stats;
            var mismatch = statsExpected[0] != statsActual[0] || statsExpected[2] != statsActual[2];
            if (!mismatch) {
                var avgExpected = statsExpected[1];
                var avgActual = statsActual[1];
                mismatch = avgExpected != avgActual;
                if (lenient) {
                    mismatch &= avgExpected - avgActual != 1;
                }
            }
            if (mismatch) {
                System.out.printf("Values don't match in entry #%,d: expected \"%s\", actual \"%s\"%n", i, entriesExpected[i], entriesActual[i]);
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
