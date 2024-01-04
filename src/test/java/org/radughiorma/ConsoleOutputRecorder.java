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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class ConsoleOutputRecorder implements BeforeEachCallback, AfterEachCallback {

    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        System.setOut(originalOut);
    }

    public String getCapturedOutput() {
        return capturedOut.toString();
    }
}
