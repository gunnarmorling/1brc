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

if [ -z "$1" ]
  then
    echo "Usage: evaluate2.sh <fork name> (<fork name 2> ...)"
    echo " for each fork, there must be a 'prepare_<fork name>.sh' script and a 'calculate_average_<fork name>.sh' script"
    echo " there may be an 'additional_build_steps_<fork name>.sh' script too"
    exit 1
fi

BOLD_WHITE='\033[1;37m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
PURPLE='\033[0;35m'
BOLD_RED='\033[1;31m'
RED='\033[0;31m'
BOLD_YELLOW='\033[1;33m'
RESET='\033[0m' # No Color

function check_command_installed {
  if ! [ -x "$(command -v $1)" ]; then
    echo "Error: $1 is not installed." >&2
    exit 1
  fi
}

check_command_installed java
check_command_installed hyperfine
check_command_installed jq

# Check if SMT is enabled (we want it disabled)
if [ -f "/sys/devices/system/cpu/smt/active" ]; then
  if [ "$(cat /sys/devices/system/cpu/smt/active)" != "0" ]; then
    echo -e "${BOLD_YELLOW}WARNING${RESET} SMT is enabled"
  fi
fi

# Check if Turbo Boost is enabled (we want it disabled)
if [ -f "/sys/devices/system/cpu/cpufreq/boost" ]; then
  if [ "$(cat /sys/devices/system/cpu/cpufreq/boost)" != "0" ]; then
    echo -e "${BOLD_YELLOW}WARNING${RESET} Turbo Boost is enabled"
  fi
fi

set -o xtrace

java --version

./mvnw --quiet clean verify

rm -f measurements.txt
ln -s measurements_1B.txt measurements.txt

set +o xtrace

echo ""

# check if out_expected.txt exists
if [ ! -f "out_expected.txt" ]; then
  echo "Error: out_expected.txt does not exist." >&2
  echo "Please create it with:"
  echo "  ./calculate_average_baseline.sh > out_expected.txt"
  exit 1
fi

# Prepare commands for running benchmarks for each of the forks
filetimestamp=$(date  +"%Y%m%d%H%M%S") # same for all fork.out files from this run
failed=()
for fork in "$@"; do
  # Use prepare script to invoke SDKMAN
  if [ -f "./prepare_$fork.sh" ]; then
    echo "+ source ./prepare_$fork.sh"
    source "./prepare_$fork.sh"
  fi

  # Optional additional build steps
  if [ -f "./additional_build_steps_$fork.sh" ]; then
    echo "+ ./additional_build_steps_$fork.sh"
    ./additional_build_steps_$fork.sh
  fi

  # Use hyperfine to run the benchmarks for each fork
  HYPERFINE_OPTS="--warmup 1 --runs 5 --export-json $fork-$filetimestamp-timing.json --output ./$fork-$filetimestamp.out"

  set +e # we don't want hyperfine or diff failing on 1 fork to exit the script early

  # check if this script is running on a Linux box
  if [ "$(uname -s)" == "Linux" ]; then
    check_command_installed numactl

    # Linux platform
    # prepend this with numactl --physcpubind=0-7 for running it only with 8 cores
    numactl --physcpubind=0-7 hyperfine $HYPERFINE_OPTS "./calculate_average_$fork.sh 2>&1"
  else
    hyperfine $HYPERFINE_OPTS "./calculate_average_$fork.sh 2>&1"
  fi

  # Verify output
  diff <(grep Hamburg $fork-$filetimestamp.out) <(grep Hamburg out_expected.txt) > /dev/null
  if [ $? -ne 0 ]; then
    echo ""
    echo -e "${BOLD_RED}FAILURE${RESET}: output of ${BOLD_WHITE}$fork-$filetimestamp.out${RESET} does not match ${BOLD_WHITE}out_expected.txt${RESET}"
    echo ""

    # add $fork to $failed array
    failed+=("$fork")
  fi
  set -e
done

# Summary
echo -e "${BOLD_WHITE}Summary${RESET}"
for fork in "$@"; do
  # skip reporting results for failed forks
  if [[ " ${failed[@]} " =~ " ${fork} " ]]; then
    echo -e "  ${RED}$fork${RESET}: output did not match"
    continue
  fi

  # Trimmed mean = The slowest and the fastest runs are discarded, the
  # mean value of the remaining three runs is the result for that contender
  trimmed_mean=$(jq -r '.results[0].times | .[1:-1] | add / length' $fork-$filetimestamp-timing.json)
  raw_times=$(jq -r '.results[0].times | join(",")' $fork-$filetimestamp-timing.json)

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

# Leaderboard
echo -e "${BOLD_WHITE}Leaderboard${RESET}"
for fork in "$@"; do
  # skip reporting results for failed forks
  if [[ " ${failed[@]} " =~ " ${fork} " ]]; then
    continue
  fi

  trimmed_mean=$(jq -r '.results[0].times | .[1:-1] | add / length' $fork-$filetimestamp-timing.json)

  # Read java version from prepare_$fork.sh if it exists
  java_version="unknown"
  if [ -f "./prepare_$fork.sh" ]; then
    java_version=$(grep "sdk use java" ./prepare_$fork.sh | cut -d' ' -f4)
  fi

  # trimmed_mean is in seconds
  # Format trimmed_mean as MM::SS.mmm
  # using bc
  trimmed_mean_minutes=$(echo "$trimmed_mean / 60" | bc)
  trimmed_mean_seconds=$(echo "$trimmed_mean % 60 / 1" | bc)
  trimmed_mean_ms=$(echo "($trimmed_mean - $trimmed_mean_minutes * 60 - $trimmed_mean_seconds) * 1000 / 1" | bc)
  trimmed_mean_formatted=$(printf "%02d:%02d.%03d" $trimmed_mean_minutes $trimmed_mean_seconds $trimmed_mean_ms)

  # var result = String.format("%02d:%02d.%.0f", mean.toMinutesPart(), mean.toSecondsPart(), (double) mean.toNanosPart() / 1_000_000);
  # var author = actualFile.replace(".out", "")
  # System.out.println(String.format("\n|   |        %s| [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_%s.java)| 21.0.1-open | [%s](https://github.com/%s)|", result, author, author, author));

  echo "|   |        $trimmed_mean_formatted| [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_$fork.java)| $java_version | [$fork](https://github.com/$fork)|"
done
echo ""

# Finalize .out files
echo "Raw results saved to file(s):"
for fork in "$@"; do
  # Append $fork-$filetimestamp-timing.json to $fork-$filetimestamp.out and rm $fork-$filetimestamp-timing.json
  cat $fork-$filetimestamp-timing.json >> $fork-$filetimestamp.out
  rm $fork-$filetimestamp-timing.json

  echo "  $fork-$filetimestamp.out"
done
