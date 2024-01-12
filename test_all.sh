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

set -euo pipefail

INPUT=${1:-""}

if [ "$INPUT" = "-h" ] || [ "$#" -gt 1 ]; then
  echo "Usage: ./test_all.sh [input file pattern]"
  echo
  echo "For each available fork run ./test.sh <fork name> [input file pattern]."
  echo "Note that optional <input file pattern> should be quoted if contains wild cards."
  echo
  echo "Examples:"
  echo "./test_all.sh"
  echo "./test_all.sh 2>/dev/null"
  echo "./test_all.sh src/test/resources/samples/measurements-1.txt"
  echo "./test_all.sh 'src/test/resources/samples/measurements-*.txt'"
  exit 1
fi

if [ -t 1 ]; then
  GREEN='\033[0;32m'
  RED='\033[0;31m'
  RESET='\033[0m'
else
  GREEN=""
  RED=""
  RESET=""
fi

WITH_TIMEOUT=""
if [ -x "$(command -v timeout)" ]; then
  WITH_TIMEOUT="timeout -s KILL 5s"
elif [ -x "$(command -v gtimeout)" ]; then # MacOS from `brew install coreutils`
  WITH_TIMEOUT="gtimeout -s KILL 5s"
else
  echo "$0: timeout command not available, tests may run indefinitely long." 1>&2
fi

for impl in $(ls calculate_average_*.sh | sort); do
  noext="${impl%%.sh}"
  fork=${noext##calculate_average_}

  # ./test.sh calls ./prepare_$fork.sh e.g. to build native image
  # which may take some time.
  # Here we run it upfront, assuming that prepare result is cached
  # to avoid timeout due to long preparation.
  if [ -f "./prepare_$fork.sh" ]; then
    if ! output=$("./prepare_$fork.sh" 2>&1); then
      echo "$output" 1>&2
      echo "FAIL $fork"
      continue
    fi
  fi

  if output=$($WITH_TIMEOUT ./test.sh "$fork" "$INPUT" 2>&1); then
    echo -e "${GREEN}PASS${RESET} $fork"
  elif [ $? -eq 137 ]; then
    echo -e "${RED}TIME${RESET} $fork"
  else
    echo "$output" 1>&2
    echo -e "${RED}FAIL${RESET} $fork"
  fi
done
