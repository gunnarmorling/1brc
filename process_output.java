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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class process_output {

    public static void main(String... args) throws Exception {
        String expectedFile = args[0];
        String actualFile = args[1];

        String expected = new String(Files.readAllBytes(Paths.get(expectedFile)));
        List<String> times = new ArrayList<>();

        var outputLines = Files.lines(Paths.get(actualFile))
                .collect(Collectors.toList());

        int matched = 0;

        for (String line : outputLines) {
            if (line.contains("Hamburg")) {
                if (!line.equals(expected)) {
                    System.err.println("FAILURE Unexpected output");
                    System.err.println(line);
                }
                else {
                    matched++;
                }
            }
            else if (line.startsWith("real")) {
                times.add(line);
            }
        }

        if (matched == 5) {
            System.out.println("OK Output matched");
        }
        else {
            System.err.println("FAILURE Output didn't match");
        }

        System.out.println();
        System.out.println(actualFile);

        System.out.println(times.stream()
            .map(t -> t.substring(5))
            .map(t -> t.replace("s", "").replace("m", ":"))
            .collect(Collectors.joining(System.lineSeparator())));
    }
}
