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
package org.radughiorma;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class Arguments {
    private static final String FILE = "./measurements.txt";

    private Arguments() {
    }

    public static Path measurmentsPath(String[] args) {
        String filePath = measurmentsFilename(args);

        return Paths.get(filePath);
    }

    public static String measurmentsFilename(String[] args) {
        String filePath;
        if(args.length == 0){
            System.out.println("Usage: java CalculateAverage_* <input-file>");
            System.out.println(STR."Defaulting to: \{FILE}");
            filePath = FILE;
        }else{
            filePath = args[0];
        }
        return filePath;
    }
}
