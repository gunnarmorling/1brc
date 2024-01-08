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

# also tried: 23.ea.3-open, 21.0.1-graalce, 21.0.1-graal, 21.0.1.crac-librca (tried CRaC API to see if it preserves JIT state)
. "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.1-tem

# --enable-preview to use the new memory mapped segments
# We don't allocate much, so just give it 1G heap and turn off GC; the AlwaysPreTouch was suggested by the ergonomics
# Experimenting on the target VM config with various memory tweaks showed that UseTransparentHugePages gives us 10% boost
JAVA_OPTS="--enable-preview -da -dsa -Xms1g -Xmx1g -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC  -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages"
time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_ddimtirov
