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

JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms1g -Xmx1g -XX:-AlwaysPreTouch -XX:+UseTransparentHugePages
-XX:-TieredCompilation -XX:-UseCountedLoopSafepoints -XX:+TrustFinalNonStaticFields -XX:CompileThreshold=2048
--add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
-XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=quiet
-XX:CompileCommand=dontinline,dev.morling.onebrc.CalculateAverage_shipilev\$ParsingTask::seqCompute
-XX:CompileCommand=dontinline,dev.morling.onebrc.CalculateAverage_shipilev\$MeasurementsMap::updateSlow
-XX:CompileCommand=inline,dev.morling.onebrc.CalculateAverage_shipilev\$Bucket::matches"
java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_shipilev
