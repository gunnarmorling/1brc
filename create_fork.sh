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

set -eo pipefail

SOURCE_FORK="baseline"

usage() {
  echo "Usage: create_fork.sh [-s <source fork>] <fork name>"
  echo "  -s <source fork>  The name of the fork to copy from (default: baseline)"
  echo "  <fork name>       The name of the fork to create"
  exit 1
}

# Parse
while getopts ":s:" opt; do
  case ${opt} in
    s )
      SOURCE_FORK=$OPTARG
      ;;
    \? )
      usage
      exit 1
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" 1>&2
      exit 1
      ;;
  esac
done

FORK=${@:$OPTIND:1}
if [ -z "$FORK" ]
  then
    usage
    exit 1
fi

# validate the fork name has only [a-zA-Z0-9_] and then error otherwise to let the user fix
if [[ ! "$FORK" =~ ^[a-zA-Z0-9_]+$ ]]; then
  echo "Fork name must only contain characters result in a valid Java class name  [a-zA-Z0-9_]"
  exit 1
fi


# helper function
function substitute_in_file {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/$1/$2/g" $3
  else
    sed -i "s/$1/$2/g" $3
  fi
}

set -x

# create new fork

cp -i prepare_$SOURCE_FORK.sh prepare_$FORK.sh

cp -i calculate_average_$SOURCE_FORK.sh calculate_average_$FORK.sh
substitute_in_file $SOURCE_FORK $FORK calculate_average_$FORK.sh

if [ $SOURCE_FORK == "baseline" ]; then
  cp -i src/main/java/dev/morling/onebrc/CalculateAverage_baseline.java src/main/java/dev/morling/onebrc/CalculateAverage_$FORK.java
  substitute_in_file CalculateAverage_baseline CalculateAverage_$FORK src/main/java/dev/morling/onebrc/CalculateAverage_$FORK.java
else
  cp -i src/main/java/dev/morling/onebrc/CalculateAverage_$SOURCE_FORK.java src/main/java/dev/morling/onebrc/CalculateAverage_$FORK.java
  substitute_in_file $SOURCE_FORK $FORK src/main/java/dev/morling/onebrc/CalculateAverage_$FORK.java
fi

