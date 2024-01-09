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

JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS --enable-preview"
JAVA_OPTS="$JAVA_OPTS -XX:+UseParallelGC"
JAVA_OPTS="$JAVA_OPTS -XX:+AlwaysCompileLoopMethods"
#JAVA_OPTS="$JAVA_OPTS -XX:C1MaxInlineSize=500"
#JAVA_OPTS="$JAVA_OPTS -XX:FreqInlineSize=500"

#JAVA_OPTS="$JAVA_OPTS -XX:+PrintCompilation"
#JAVA_OPTS="$JAVA_OPTS -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining"
#JAVA_OPTS="$JAVA_OPTS -Xlog:async"

#JAVA_OPTS="$JAVA_OPTS -XX:CompileThreshold=20 "
#JAVA_OPTS="$JAVA_OPTS -XX:-TieredCompilation -XX:TieredStopAtLevel=4"
#JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xlog:all=off"
#JAVA_OPTS="$JAVA_OPTS -Xlog:gc*=debug:file=/tmp/gc.log"

source "$HOME/.sdkman/bin/sdkman-init.sh"
#sdk use java 21.0.1-open 1>&2
sdk use java 21.0.1-graal 1>&2
#sdk use java 21.0.1-graalce 1>&2
time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_vemana "$@"
