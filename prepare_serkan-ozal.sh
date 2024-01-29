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
sdk use java 21.0.1-open 1>&2

JAVA_OPTS="--enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector "
JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions"
JAVA_OPTS="$JAVA_OPTS -XX:-TieredCompilation -XX:MaxInlineSize=10000 -XX:InlineSmallCode=10000 -XX:FreqInlineSize=10000"
JAVA_OPTS="$JAVA_OPTS -XX:-UseCountedLoopSafepoints -XX:GuaranteedSafepointInterval=0"
JAVA_OPTS="$JAVA_OPTS -XX:+TrustFinalNonStaticFields -da -dsa -XX:+UseNUMA -XX:-EnableJVMCI"
JAVA_OPTS="$JAVA_OPTS -Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
JAVA_OPTS="${JAVA_OPTS} -Dfile.path=src/test/resources/samples/measurements-10000-unique-keys.txt"
if [[ ! "$(uname -s)" = "Darwin" ]]; then
  JAVA_OPTS="$JAVA_OPTS -XX:+UseTransparentHugePages"
fi

# Set configs
export USE_SHARED_ARENA=true
export USE_SHARED_REGION=true
export CLOSE_STDOUT_ON_RESULT=true

CLASS_NAME="CalculateAverage_serkan_ozal"

# Create CDS archive
java ${JAVA_OPTS} -Xshare:off -XX:DumpLoadedClassList=target/${CLASS_NAME}.classlist --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.${CLASS_NAME}
java ${JAVA_OPTS} -Xshare:dump -XX:SharedClassListFile=target/${CLASS_NAME}.classlist -XX:SharedArchiveFile=target/${CLASS_NAME}.jsa --class-path target/average-1.0.0-SNAPSHOT.jar
java ${JAVA_OPTS} -Xshare:on -XX:SharedArchiveFile=target/${CLASS_NAME}.jsa -XX:ArchiveClassesAtExit=target/${CLASS_NAME}_cds.jsa --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.${CLASS_NAME}
