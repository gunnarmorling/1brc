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

if [ ! -f target/CalculateAverage_jonathan-aotearoa_image ]; then
    # Enable preview features and disable system assertions.
    JAVA_OPTS="--enable-preview -dsa"
    # Use the no-op GC.
    # Enable CPU features (-march=native) and level-3 optimisations (-O3)
    NATIVE_IMAGE_OPTS="--initialize-at-build-time=dev.morling.onebrc.CalculateAverage_jonathanaotearoa --gc=epsilon -O3 -march=native --strict-image-heap $JAVA_OPTS"
    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_jonathan-aotearoa_image dev.morling.onebrc.CalculateAverage_jonathanaotearoa
fi