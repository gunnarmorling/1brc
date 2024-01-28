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

PROFILER_OPTS="-agentpath:/Users/guruprasad.sridharan/async-profiler-2.9-macos/build/libasyncProfiler.dylib=start,event=cpu,file=target/profile.html,interval=999000"
JAVA_OPTS="$PROFILER_OPTS --enable-preview --add-modules jdk.incubator.vector -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UseParallelGC -Xms2g -Xmx2g -Xlog:gc:stdout -XX:MaxDirectMemorySize=4g -Dthreads=16 -Djava.util.concurrent.ForkJoinPool.common.parallelism=16"
java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_godofwharf
