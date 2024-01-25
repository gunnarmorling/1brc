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

if [ -f target/CalculateAverage_jonathan-aotearoa_image ]; then
    echo "Using native image 'target/CalculateAverage_jonathan-aotearoa_image'. Delete this file to select JVM mode." 1>&2
    target/CalculateAverage_jonathan-aotearoa_image
else
    JAVA_OPTS="--enable-preview -XX:+UnlockExperimentalVMOptions -XX:+TrustFinalNonStaticFields -dsa -XX:+UseNUMA"
    JAVA_OPTS="$JAVA_OPTS -XX:+UseTransparentHugePages"
    echo "Running in JVM mode as no native image was found. Run 'prepare_jonathan-aotearoa.sh' to generate a native image." 1>&2
    java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_jonathanaotearoa
fi

