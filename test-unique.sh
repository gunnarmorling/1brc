#!/bin/bash

set -euo pipefail

: "${UNIQUE:=100000}"

if [ $# -ne 1 ]; then
  echo "usage: $0 <gh-user>" >&2
  exit 1
fi
user="${1}"

exe="calculate_average_${1}.sh"

if [ ! -x "${exe}" ]; then
  echo "Start script '${exe}' does not exist" >&2
  exit 2
fi

testdata="measurements-unique-${UNIQUE}.txt"
if [ ! -r "${testdata}" ]; then
  ./create_measurements_unique.sh "${UNIQUE}"
fi

ln -sfn "${testdata}" "measurements.txt"

mkdir -p results

baseline="results/result-unique-${UNIQUE}-baseline.txt"
if [ ! -r "${baseline}" ]; then
  echo "Baseline result '${baseline}' does not yet exists, we create it first!" >&2
  ./prepare_baseline.sh
  ./calculate_average_baseline.sh > "${baseline}"
fi

prep="prepare_${user}.sh"
test -r "${prep}" && bash ./${prep}

./reset-sdk.sh

result="results/result-unique-${UNIQUE}-${user}.txt"
./${exe} > "${result}"

if ! diff -q "${result}" "${baseline}"; then
  ls -l "${result}" "${baseline}"
fi