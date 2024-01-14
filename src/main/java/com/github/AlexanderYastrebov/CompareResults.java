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
package com.github.AlexanderYastrebov;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Pattern;

public class CompareResults {
    public static void main(String[] args) throws IOException {
        require(args.length == 2 || args.length == 3, """
                Usage: java CompareResults.java <expected> <actual> [<tolerance>]

                Examples:
                    java CompareResults.java expected.out actual.out
                    java CompareResults.java expected.out actual.out 0.1""");

        var expected = args[0];
        var actual = args[1];
        BigDecimal tolerance = BigDecimal.ZERO;

        if (args.length == 3) {
            tolerance = new BigDecimal(args[2]);
            require(tolerance.signum() >= 0, "tolerance must be positive or zero");
        }

        var expectedLines = Files.readAllLines(Paths.get(expected));
        require(expectedLines.size() == 1, "File %s has %d lines but must have 1", expected, expectedLines.size());

        var actualLines = Files.readAllLines(Paths.get(actual));
        require(actualLines.size() == 1, "File %s has %d lines but must have 1", actual, actualLines.size());

        var expectedLine = expectedLines.get(0);
        var actualLine = actualLines.get(0);

        require(expectedLine.startsWith("{"), "File %s does not start with {", expected);
        require(expectedLine.endsWith("}"), "File %s does not end with }", expected);
        require(actualLine.startsWith("{"), "File %s does not start with {", actual);
        require(actualLine.endsWith("}"), "File %s does not end with }", actual);

        expectedLine = expectedLine.substring(1, expectedLine.length() - 1);
        actualLine = actualLine.substring(1, actualLine.length() - 1);

        // id may contain comma, e.g. "Washington, D.C.=-15.1/14.8/44.8, Wau=-2.1/27.4/53.4"
        // so split at ", " only if it is preceded by a digit
        var expectedEntries = Arrays.stream(expectedLine.split("(?<=[0-9]), ")).map(Entry::parse).toList();
        var actualEntries = Arrays.stream(actualLine.split("(?<=[0-9]), ")).map(Entry::parse).toList();

        require(expectedEntries.stream().sorted().toList().equals(expectedEntries), "File %s entries are not sorted", expected);
        require(actualEntries.stream().sorted().toList().equals(actualEntries), "File %s entries are not sorted", actual);

        int i = 0, j = 0;
        while (i < expectedEntries.size()) {
            require(j < actualEntries.size(), "expected has more entries than actual %d>%d", expectedEntries.size(), actualEntries.size());

            var expectedEntry = expectedEntries.get(i);
            var actualEntry = actualEntries.get(j);

            var c = expectedEntry.id.compareTo(actualEntry.id);
            if (c < 0) {
                error("missing %s", expectedEntry.value);
                i++;
                continue;
            }
            else if (c > 0) {
                error("unexpected %s", actualEntry.value);
                j++;
                continue;
            }

            checkTolerance(expectedEntry.min, actualEntry.min, tolerance, "%s != %s (min)", expectedEntry.value, actualEntry.value);
            checkTolerance(expectedEntry.avg, actualEntry.avg, tolerance, "%s != %s (avg)", expectedEntry.value, actualEntry.value);
            checkTolerance(expectedEntry.max, actualEntry.max, tolerance, "%s != %s (max)", expectedEntry.value, actualEntry.value);
            i++;
            j++;
        }
        require(expectedEntries.size() == actualEntries.size(), "expected has less entries than actual %d<%d", expectedEntries.size(), actualEntries.size());
    }

    private static void require(boolean condition, String format, Object... args) {
        if (!condition) {
            fatal(format, args);
        }
    }

    private static void checkTolerance(BigDecimal expected, BigDecimal actual, BigDecimal tolerance, String format, Object... args) {
        if (expected.subtract(actual).abs().compareTo(tolerance) == 1) {
            error(format, args);
        }
    }

    private static void fatal(String format, Object... args) {
        System.err.format(format, args);
        System.err.println();
        System.exit(1);
    }

    private static void error(String format, Object... args) {
        System.err.format(format, args);
        System.err.println();
    }

    record Entry(String value, String id, BigDecimal min, BigDecimal avg, BigDecimal max) implements Comparable<Entry> {

        static final String number = "(-?[0-9]{1,2}[.][0-9])";
        static final Pattern p = Pattern.compile("^([^=]+)=" + number + "/" + number + "/" + number + "$");

        static Entry parse(String s) {
            var m = p.matcher(s);
            require(m.matches() && m.groupCount() == 4, "%s has wrong format", s);
            return new Entry(s,
                    m.group(1),
                    new BigDecimal(m.group(2)),
                    new BigDecimal(m.group(3)),
                    new BigDecimal(m.group(4)));
        }

        @Override
        public int compareTo(Entry o) {
            return id.compareTo(o.id);
        }
    }
}
