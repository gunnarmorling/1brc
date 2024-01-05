#! /bin/bash
#
#  Copyright 2024 The original authors
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

# sdk use 21-zulu

# $script build -> build on 21-zulu before running
# $script native -> use native-image and build before running

function run() {
  JAVA_OPTS="--enable-preview"
  java $JAVA_OPTS --class-path target/classes dev.morling.onebrc.CalculateAverage_rmannibucau
}

if [ -n "$1" ]; then
  source "$HOME/.sdkman/bin/sdkman-init.sh"
  case "$1" in
    build)
      sdk use java 21-zulu
      mvn package
      time run
      ;;

    # not really faster than jvm mode as of today so no need to test it
    native)
      # note: this is not great to native image all mains at once but no need to break the challenge proposal for that yet
      sdk use java 21.0.1-graal
      mvn compile
      native-image --gc=G1 -classpath target/classes dev.morling.onebrc.CalculateAverage_rmannibucau ./target/CalculateAverage_rmannibucau
      time ./target/CalculateAverage_rmannibucau
      ;;

    *)
      echo "[SEVERE] Unknown parameter $1" 1>&2
      exit 1
      ;;
  esac
else
  time run
fi
