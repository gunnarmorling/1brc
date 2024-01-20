#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -XX:+UnlockDiagnosticVMOptions -XX:PrintAssemblyOptions=intel -XX:CompileCommand=print,*.CalculateAverage_mtopolnik::recordMeasurementAndAdvanceCursor"
# -XX:InlineSmallCode=10000 -XX:-TieredCompilation -XX:CICompilerCount=2 -XX:CompileThreshold=1000\
java -XX:+UseParallelGC  --enable-preview \
  --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_jerrinot
