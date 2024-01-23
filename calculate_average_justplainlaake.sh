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

if [ -f target/CalculateAverage_justplainlaake_image ]; then #if there is a native image, then lets run it. Else fallback to standard java execution
    target/CalculateAverage_justplainlaake_image
else
    java -XX:+UseG1GC --enable-preview -XX:+UnlockExperimentalVMOptions -XX:+TrustFinalNonStaticFields -dsa -XX:+UseNUMA --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_justplainlaake
fi

