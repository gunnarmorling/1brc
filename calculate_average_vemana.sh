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

# Basics
JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS --enable-preview"
JAVA_OPTS="$JAVA_OPTS --add-exports java.base/jdk.internal.ref=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens java.base/java.nio=ALL-UNNAMED"
#JAVA_OPTS="$JAVA_OPTS --add-modules jdk.incubator.vector"
#JAVA_OPTS="$JAVA_OPTS -XX:+UnlockDiagnosticVMOptions"

# JIT parameters
#JAVA_OPTS="$JAVA_OPTS -Xlog:class+load=info"
#JAVA_OPTS="$JAVA_OPTS -XX:+LogCompilation"
JAVA_OPTS="$JAVA_OPTS -XX:+AlwaysCompileLoopMethods"
#JAVA_OPTS="$JAVA_OPTS -XX:TieredStopAtLevel=1"
#JAVA_OPTS="$JAVA_OPTS -XX:TieredStopAtLevel=1"
#JAVA_OPTS="$JAVA_OPTS -XX:CompileCommand=inline,*State.processLine()"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintAssembly"
#JAVA_OPTS="$JAVA_OPTS -XX:LogFile=../hotspot.log"
#JAVA_OPTS="$JAVA_OPTS -XX:+DebugNonSafepoints"
#JAVA_OPTS="$JAVA_OPTS -XX:C1MaxInlineSize=150"
#JAVA_OPTS="$JAVA_OPTS -XX:C1InlineStackLimit=40"

#JAVA_OPTS="$JAVA_OPTS -XX:FreqInlineSize=500"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintCompilation"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintInlining"
#JAVA_OPTS="$JAVA_OPTS -XX:CompileThreshold=20 "
#JAVA_OPTS="$JAVA_OPTS -Xlog:async"

# GC parameters
JAVA_OPTS="$JAVA_OPTS -XX:+UseParallelGC"

#JAVA_OPTS="$JAVA_OPTS -Xlog:gc*=debug:file=/tmp/gc.log"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseEpsilonGC -Xlog:all=off"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGC -XX:+PrintGCDetails"

java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_vemana "$@"
