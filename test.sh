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

if [ "$#" -eq 0 ] || [ "$#" -gt 3 ] || [ "$1" == "-h" ]; then
  echo "Usage: ./test.sh <fork name> [input file pattern]"
  echo
  echo "For each test sample matching <input file pattern> (default '$DEFAULT_INPUT')"
  echo "runs <fork name> implementation and diffs the result with the expected output."
  echo "Note that optional <input file pattern> should be quoted if contains wild cards."
  echo "Use optional flag --quiet to suppress output."
  echo
  echo "Examples:"
  echo "./test.sh baseline"
  echo "./test.sh baseline src/test/resources/samples/measurements-1.txt"
  echo "./test.sh baseline 'src/test/resources/samples/measurements-*.txt'"
  echo "./test.sh --quiet baseline"
  exit 1
fi

QUIET=false
if [ "$1" == "--quiet" ]; then
  QUIET=true
  shift
fi

FORK=${1:-""}
INPUT=${2:-$DEFAULT_INPUT}

if [ -f "./prepare_$FORK.sh" ]; then
  if [ $QUIET == true ]; then
    "./prepare_$FORK.sh"
  else
    "./prepare_$FORK.sh" > /dev/null
  fi
fi

for sample in $(ls $INPUT); do
  if [ $QUIET != true ]; then
    echo "Validating calculate_average_$FORK.sh -- $sample"
  fi

  rm -f measurements.txt
  ln -s $sample measurements.txt

  diff <("./calculate_average_$FORK.sh" | ./tocsv.sh) <(./tocsv.sh < ${sample%.txt}.out)
done

rm measurements.txt
