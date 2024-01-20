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

JAVA_OPTS="--enable-preview -XX:+UseTransparentHugePages"

# epsilon GC needs enough memory or it makes things worse
# see https://stackoverflow.com/questions/58087596/why-are-repeated-memory-allocations-observed-to-be-slower-using-epsilon-vs-g1
# 2GB seems to be the sweet spot
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:-EnableJVMCI -XX:+UseEpsilonGC -Xmx2G -Xms2G -XX:+AlwaysPreTouch"

java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_roman_r_m
