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
sdk use java 21.0.2-graal 1>&2

if [ ! -f target/CalculateAverage_ebarlas_image ]; then
    NATIVE_IMAGE_OPTS="-H:+UnlockExperimentalVMOptions --initialize-at-build-time=dev.morling.onebrc.CalculateAverage_ebarlas --gc=epsilon -O3 -march=native -R:MaxHeapSize=128m -H:-GenLoopSafepoints --enable-preview"
    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_ebarlas_image dev.morling.onebrc.CalculateAverage_ebarlas
fi
