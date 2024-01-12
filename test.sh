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

DEFAULT_INPUT="src/test/resources/samples/*.txt"
FORK=${1:-""}
INPUT=${2:-$DEFAULT_INPUT}

if [ "$#" -eq 0 ] || [ "$#" -gt 2 ] || [ "$FORK" = "-h" ]; then
  echo "Usage: ./test.sh <fork name> [input file pattern]"
  echo
  echo "For each test sample matching <input file pattern> (default '$DEFAULT_INPUT')"
  echo "runs <fork name> implementation and diffs the result with the expected output."
  echo "Note that optional <input file pattern> should be quoted if contains wild cards."
  echo
  echo "Examples:"
  echo "./test.sh baseline"
  echo "./test.sh baseline src/test/resources/samples/measurements-1.txt"
  echo "./test.sh baseline 'src/test/resources/samples/measurements-*.txt'"
  exit 1
fi

if [ -f "./prepare_$FORK.sh" ]; then
  "./prepare_$FORK.sh"
fi

for sample in $(ls $INPUT); do
  echo "Validating calculate_average_$FORK.sh -- $sample"

  rm -f measurements.txt
  ln -s $sample measurements.txt

  if command -v git; then
    # git version must be 2.43 or newer to use \`git diff\` with process substitution
    # Source: https://stackoverflow.com/questions/22706714/why-does-git-diff-not-work-with-process-substitution
    # In the future, we can do this instead of a temp file:
    #   git diff --no-index --word-diff <("./calculate_average_$FORK.sh") ${sample%.txt}
    temp_file=$(mktemp)
    "./calculate_average_$FORK.sh" > $temp_file
    git diff --no-index --word-diff $temp_file ${sample%.txt}.out
    rm $temp_file
  else
    diff <("./calculate_average_$FORK.sh") ${sample%.txt}.out
  fi
done

rm measurements.txt
