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

if [ -f target/average_qbos07 ]; then
    echo "Picking up existing native image, delete the file to select JVM mode." 1>&2
    time target/average_qbos07
else
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    sdk use java 21.0.1-graal 1>&2
    JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xms100G -Xmx100G"
    echo "Chosing to run the app in JVM mode as no native image was found, use prepare_qbos07.sh to generate." 1>&2
    time java $JAVA_OPTS -cp target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_royvanrijn
fi