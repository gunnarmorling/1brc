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

sdk use java 21.0.1-graal
echo "Script executed from: ${PWD}"
NATIVE_IMAGE_OPTS="--gc=epsilon"
JAVA_OPTS=""

if [ -z "${JVM_MODE}" ]; then
    echo "Chosing native image mode, set JVM_MODE variable to select JVM mode."
    if [ -f ./image_calculateaverage_thomaswue ]; then
        echo 'Picking up existing native image.'
    else
        if [ -f ./profile_thomaswue.iprof ]; then
            echo 'Picking up profiling information from thomaswue.iprof.'
        else
            echo 'Could not find profiling information, therefore it will be now regenerated.'
            native-image --pgo-instrument -cp target/average-1.0.0-SNAPSHOT.jar -o instrumented_calculateaverage_thomaswue dev.morling.onebrc.CalculateAverage_thomaswue
            echo 'Running example with instrumented image for 20 seconds.'
            timeout 20 ./instrumented_calculateaverage_thomaswue
            mv default.iprof profile_thomaswue.iprof
        fi
        echo 'Could not find a native image, building a new one now.'
        native-image $NATIVE_IMAGE_OPTS --pgo=profile_thomaswue.iprof -cp target/average-1.0.0-SNAPSHOT.jar -o image_calculateaverage_thomaswue dev.morling.onebrc.CalculateAverage_thomaswue
    fi
    
    time ./image_calculateaverage_thomaswue
else
    echo "Chosing JVM mode, unset JVM_MODE variable to select native image mode."
    time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_thomaswue
fi

