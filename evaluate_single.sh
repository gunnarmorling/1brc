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

if [ -z "$1" ]; then
    echo "Usage: time_fork.sh <username>"
    exit 1
fi

BOLD_RED='\033[1;31m'
BOLD_WHITE='\033[1;37m'
RESET='\033[0m'

USERNAME=$1

if [ ! -f "./calculate_average_$USERNAME.sh" ]; then
    echo -e "${BOLD_RED}ERROR${RESET}: ./calculate_average_$USERNAME.sh does not exist." >&2
    exit 1
fi

if [ -f "./prepare_$USERNAME.sh" ]; then
    PREPARE_START=$(date +%s.%N)
    source "./prepare_$USERNAME.sh" > /dev/null 2>&1
    PREPARE_END=$(date +%s.%N)
    PREPARE_TIME=$(echo "$PREPARE_END - $PREPARE_START" | bc)
fi

CALC_START=$(date +%s.%N)
./calculate_average_$USERNAME.sh > /dev/null 2>&1
CALC_END=$(date +%s.%N)
CALC_TIME=$(echo "$CALC_END - $CALC_START" | bc)

if [ -n "$PREPARE_TIME" ]; then
    echo -e "${BOLD_WHITE}$USERNAME${RESET}: prepare ${PREPARE_TIME}s, calculate ${CALC_TIME}s"
else
    echo -e "${BOLD_WHITE}$USERNAME${RESET}: ${CALC_TIME}s"
fi