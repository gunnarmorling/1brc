#!/bin/sh
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

# runs with -Xmx24m on my machine, playing it safe with a larger heap
JAVA_OPTS="-Xmx64m --enable-preview"
# to use some black magic options
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions"
# no GC, not needed
JAVA_OPTS="$JAVA_OPTS -XX:+UseEpsilonGC -XX:+AlwaysPreTouch"
# my finals are really final
JAVA_OPTS="$JAVA_OPTS -XX:+TrustFinalNonStaticFields"
# to get CalculateAverage_obourgain$OpenAddressingMap::getOrCreate to inline. A compile command wasn't enough, it was still hitting 'already compiled into a big method'
JAVA_OPTS="$JAVA_OPTS -XX:InlineSmallCode=10000"
# seems to be a bit faster
JAVA_OPTS="$JAVA_OPTS -XX:-TieredCompilation -XX:CICompilerCount=2 -XX:CompileThreshold=1000"

time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_obourgain
