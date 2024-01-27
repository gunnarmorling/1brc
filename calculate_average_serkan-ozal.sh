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

JAVA_OPTS="--enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector"
JAVA_OPTS="$JAVA_OPTS -XX:-TieredCompilation -XX:MaxInlineSize=10000 -XX:InlineSmallCode=10000 -XX:FreqInlineSize=10000"
JAVA_OPTS="$JAVA_OPTS -Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms256m -Xmx256m -XX:+AlwaysPreTouch"
if [[ ! "$(uname -s)" = "Darwin" ]]; then
  JAVA_OPTS="$JAVA_OPTS -XX:+UseTransparentHugePages"
fi

#echo "Process started at $(date +%s%N | cut -b1-13)"
eval "exec 3< <({ CLOSE_STDOUT_ON_RESULT=true USE_SHARED_ARENA=true java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_serkan_ozal; })"
read <&3 result
echo -e "$result"
#echo "Process finished at $(date +%s%N | cut -b1-13)"
