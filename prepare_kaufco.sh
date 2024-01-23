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

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.1-graal 1>&2

if [ ! -f target/CalculateAverage_kaufco_image ]; then

    native-image --enable-preview --pgo-instrument \
    --gc=epsilon -march=native \
    --class-path target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_kaufco_instrumented_image \
    dev.morling.onebrc.CalculateAverage_kaufco

    ./target/CalculateAverage_kaufco_instrumented_image -XX:ProfilesDumpFile=./target/CalculateAverage_kaufco.iprof

    native-image --enable-preview --gc=epsilon -O7 -march=native --static --link-at-build-time \
    --pgo=./target/CalculateAverage_kaufco.iprof --class-path ./target/classes dev.morling.onebrc.CalculateAverage_kaufco \
    -o target/CalculateAverage_kaufco_image

fi
