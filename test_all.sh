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

for impl in $(ls calculate_average_*.sh | sort); do
  noext="${impl%%.sh}"
  fork=${noext##calculate_average_}

  if output=$(./test.sh "$fork" "$INPUT" 2>&1); then
    echo "PASS $fork"
  else
    echo "FAIL $fork"
    echo "$output" 1>&2
  fi
done
