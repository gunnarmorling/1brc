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

JAVA_OPTS="-Xms20480m -Xmx40960m "

if [ -f target/CalculateAverage_karthikeyan97_image ]; then
    #echo "Picking up existing native image 'target/CalculateAverage_karthikeyan97_image', delete the file to select JVM mode." 1>&2
    target/CalculateAverage_karthikeyan97_image -Xms20480m -Xmx32768m
else
    JAVA_OPTS="--enable-preview"
    #echo "Chosing to run the app in JVM mode as no native image was found, use prepare_karthikeyan97.sh to generate." 1>&2
    java -Xms20480m -Xmx32768m --enable-preview --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_karthikeyan97
fi


