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

# ParallelGC test - Time (measured by evaluate2.sh): 00:33.130
# JAVA_OPTS="--enable-preview -XX:+UseParallelGC -XX:+UseTransparentHugePages"

# G1GC test - Time (measured by evaluate2.sh):  00:26.447
# JAVA_OPTS="--enable-preview -XX:+UseG1GC -XX:+UseTransparentHugePages"

# ZGC test - Time (measured by evaluate2.sh): 00:22.813
JAVA_OPTS="--enable-preview -XX:+UseZGC -XX:+UseTransparentHugePages"

# EpsilonGC test - for now doesnt work because heap space gets exhausted
#JAVA_OPTS="--enable-preview -XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -XX:+AlwaysPreTouch"

java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_felix19350
