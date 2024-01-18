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
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidateResult {

    private static HashMap<String, String> parseOutputStringToMap(String s) {
        HashMap<String, String> map = new HashMap<>();

        String regex = "\\b([\\p{L}.'\\-() ,]*)=([\\d./-]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(s);
        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String value = matcher.group(2).trim();

            if (key.startsWith(", ")) {
                key = key.substring(2);
            }
            map.put(key, value);
        }

        return map;
    }

    public static void main(String[] args) throws IOException {
        HashMap<String, String> expected = parseOutputStringToMap(args[0]);
        HashMap<String, String> actual = parseOutputStringToMap(args[1]);

        AtomicLong count = new AtomicLong();
        if (!expected.equals(actual)) {
            expected.forEach((key, expectedValue) -> {
                if (actual.containsKey(key)) {
                    String actualValue = actual.get(key);
                    if (!expectedValue.equals(actualValue)) {
                        System.out.println(key + ", Expected: " + expectedValue + ", Actual: " + actualValue);
                        count.getAndIncrement();
                    }
                }
                else {
                    count.getAndIncrement();
                    System.out.println("Key: " + key + "does not exist in actual.");
                }
            });
        }
        System.out.println("Correct Stations: " + (expected.keySet().size() - count.get()) + "/" + expected.keySet().size());
        System.out.println("Solution Correctness: " + String.format("%.2f", (expected.keySet().size() - count.get()) * 100.0 / expected.keySet().size()) + "%.");
    }
}
