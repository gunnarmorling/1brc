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
    echo " for each fork, there must be a 'prepare_<fork name>.sh' script and a 'calculate_average_<fork name>.sh' script"
    echo " there may be an 'additional_build_steps_<fork name>.sh' script too"
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

# Prepare commands for running benchmarks for each of the forks
filetimestamp=$(date  +"%Y%m%d%H%M%S") # same for all fork.out files from this run
forks=()
for fork in "$@"; do
  # Use prepare script to invoke SDKMAN
  if [ ! -f "./prepare_$fork.sh" ]; then
    echo "Error: prepare script for $fork not found, expecting file ./prepare_$fork.sh"
    exit 1
  fi
  source "./prepare_$fork.sh"

  # Optional additional build steps
  if [ -f "./additional_build_steps_$fork.sh" ]; then
    ./additional_build_steps_$fork.sh
  fi

  # Use hyperfine to run the benchmarks for each fork
  HYPERFINE_OPTS="--warmup 1 --runs 5 --export-json $fork-$filetimestamp.out"
  # For debugging:
  # HYPERFINE_OPTS="$HYPERFINE_OPTS --show-output"
  hyperfine $HYPERFINE_OPTS "./calculate_average_$fork.sh 2>&1"
done

# Print the 'Summary' in bold and white
BOLD_WHITE='\033[1;37m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
PURPLE='\033[0;35m'
RESET='\033[0m' # No Color

echo -e "${BOLD_WHITE}Summary${RESET}"

forks=()
for fork in "$@"; do
  # Trimmed mean = The slowest and the fastest runs are discarded, the
  # mean value of the remaining three runs is the result for that contender
  trimmed_mean=$(jq -r '.results[0].times | .[1:-1] | add / length' $fork-$filetimestamp.out)
  raw_times=$(jq -r '.results[0].times | join(",")' $fork-$filetimestamp.out)

  if [ "$fork" == "$1" ]; then
    color=$CYAN
  elif [ "$fork" == "$2" ]; then
    color=$GREEN
  else
    color=$PURPLE
  fi

  echo -e "  ${color}$fork${RESET}: trimmed mean ${BOLD_WHITE}$trimmed_mean${RESET}, raw times ${BOLD_WHITE}$raw_times${RESET}"
done

echo ""
echo "Raw results saved to file(s):"
for fork in "$@"; do
  echo "  $fork-$filetimestamp.out"
done
