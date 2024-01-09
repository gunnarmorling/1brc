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

if [ -f ./image_calculateaverage_mtopolnik ]; then
    echo "Using Graal native image: image_calculateaverage_mtopolnik; delete to run with JVM instead." 1>&2
    time ./image_calculateaverage_mtopolnik
else
  echo "Graal native image not found, using the JVM. Run additional_build_step_mtopolnik.sh to create." 1>&2
  source "$HOME/.sdkman/bin/sdkman-init.sh"
  sdk use java 21.0.1-graal 1>&2
  time java -Xmx256m --enable-preview -XX:InlineSmallCode=10000\
    --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_mtopolnik
fi
