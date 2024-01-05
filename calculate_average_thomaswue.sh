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

NATIVE_IMAGE_OPTS="--gc=epsilon -O3 -march=native --enable-preview"
JAVA_OPTS="--enable-preview"

if [ -z "${JVM_MODE}" ]; then
    # Chosing native image mode, set JVM_MODE variable to select JVM mode.
    if [ -z "${PGO_MODE}" ]; then
        if [ -f ./image_calculateaverage_thomaswue ]; then
            : # Picking up existing native image.
        else
            echo 'Using native image without PGO data, set PGO_MODE to use profile-guided optimizations instead.'
            sdk use java 21.0.1-graal
            native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o image_calculateaverage_thomaswue dev.morling.onebrc.CalculateAverage_thomaswue
        fi
        time ./image_calculateaverage_thomaswue
    else
        if [ -f ./image_calculateaverage_thomaswue_pgo ]; then
            : # Picking up existing native image with PGO.
	else
            if [ -f ./profile_thomaswue.iprof ]; then
                echo 'Picking up profiling information from profile_thomaswue.iprof.'
            else
                echo 'Could not find profiling information, therefore it will be now regenerated.'
                sdk use java 21.0.1-graal
                native-image --pgo-instrument -cp target/average-1.0.0-SNAPSHOT.jar -o instrumented_calculateaverage_thomaswue dev.morling.onebrc.CalculateAverage_thomaswue
                echo 'Running example with instrumented image for 20 seconds.'
                timeout 20 ./instrumented_calculateaverage_thomaswue
                mv default.iprof profile_thomaswue.iprof
            fi
            echo 'Could not find a native image, building a new one now.'
            sdk use java 21.0.1-graal
            native-image $NATIVE_IMAGE_OPTS --pgo=profile_thomaswue.iprof -cp target/average-1.0.0-SNAPSHOT.jar -o image_calculateaverage_thomaswue_pgo dev.morling.onebrc.CalculateAverage_thomaswue
	fi
        time ./image_calculateaverage_thomaswue_pgo
    fi
else
    # Chosing JVM mode, unset JVM_MODE variable to select native image mode.
    sdk use java 21.0.1-graal
    time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_thomaswue
fi

