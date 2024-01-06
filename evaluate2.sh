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

if [ -z "$1" ]
  then
    echo "Usage: evaluate2.sh <fork name> (<fork name 2> ...)"
    exit 1
fi

function check_command_installed {
  if ! [ -x "$(command -v $1)" ]; then
    echo "Error: $1 is not installed." >&2
    exit 1
  fi
}
check_command_installed java
check_command_installed mvn
check_command_installed hyperfine
check_command_installed jq

set -o xtrace

java --version

mvn --quiet clean verify

rm -f measurements.txt
ln -s measurements_1B.txt measurements.txt

set +o xtrace

echo ""
TEMP_FILE=$(mktemp)
HYPERFINE_OPTS="--warmup 1 --runs 5 --export-json $TEMP_FILE"
# For debugging:
# HYPERFINE_OPTS="$HYPERFINE_OPTS --show-output"

# Prepare commands for running benchmarks for each of the forks
forks=()
for fork in "$@"; do
    forks+=("./calculate_average_$fork.sh 2>&1")
done

# Use hyperfine to run the benchmarks for each fork
hyperfine $HYPERFINE_OPTS "${forks[@]}"

# The slowest and the fastest runs are discarded
# The mean value of the remaining three runs is the result for that contender
echo ""
echo "command,trimmed_mean"
jq -r '.results[] | [ .command, ((.times | sort) | .[1:-1] | add / length) ] | join(",")' $TEMP_FILE \
  | perl -pe 's/^[.]\/calculate_average_(\w+).*,/$1,/'

# Output the raw times for each command
echo ""
echo "command,raw_times"
jq -r '.results[] | [.command, (.times | join(","))] | join(",")' $TEMP_FILE \
  | perl -pe 's/^[.]\/calculate_average_(\w+).*?,/$1,/'

rm $TEMP_FILE
