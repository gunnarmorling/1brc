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

JAVA_OPTS="--enable-preview --add-modules jdk.incubator.vector -Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -XX:+AlwaysPreTouch"
# These flags are mostly copied from the shipilev's branch. They don't really give a predictable benefit, but they don't hurt either.
JAVA_OPTS="$JAVA_OPTS -XX:-TieredCompilation -XX:CICompilerCount=1 -XX:CompileThreshold=2048 -XX:-UseCountedLoopSafepoints -XX:+TrustFinalNonStaticFields"

if [[ "$(uname -s)" == "Linux" ]]; then
    JAVA_OPTS="$JAVA_OPTS -XX:+UseTransparentHugePages"
fi

# https://stackoverflow.com/a/23378780/7221823
logicalCpuCount=$([ $(uname) = 'Darwin' ] &&
                       sysctl -n hw.logicalcpu_max ||
                       lscpu -p | egrep -v '^#' | wc -l)
# The required heap is proportional to the number of cores.
# There's roughly 3.5MB heap per thread required for the 10k problem.
requiredMemory=$(echo "(l(15 + 3.5 * $logicalCpuCount)/l(2))" | bc -l)
heapSize=$(echo "scale=0; 2^(($requiredMemory+1)/1)" | bc)

JAVA_OPTS="$JAVA_OPTS -Xms${heapSize}m -Xmx${heapSize}m"
java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_melgenek
