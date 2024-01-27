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

if [ ! -f target/CalculateAverage_justplainlaake_image ]; then
    #disable assertions
    #optimize code for best performance
    #native march gives best performance for machine image is built on
    #strict image heap allows all classes ot be used at build time
    #native image info prints the trace of the build
    #enable preview allows for preview features of current release
    #epsilon garbage collector is a gc that doesn't gc... haha
    native-image -dsa -O3 -march=native --strict-image-heap --native-image-info --enable-preview --gc=epsilon -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_justplainlaake_image dev.morling.onebrc.CalculateAverage_justplainlaake
fi
