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
    echo "Usage: test_ci.sh <fork name> (<fork name 2> ...)"
    echo " for each fork, there must be a 'calculate_average_<fork name>.sh' script and an optional 'prepare_<fork name>.sh'."
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

MEASUREMENTS_FILE="measurements_10M.txt"
RUNS=5
DEFAULT_JAVA_VERSION="21.0.1-open"
RUN_TIME_LIMIT=300 # seconds

TIMEOUT=""
if [ "$(uname -s)" == "Linux" ]; then
  TIMEOUT="timeout -v $RUN_TIME_LIMIT"
else # MacOs
  if [ -x "$(command -v gtimeout)" ]; then
    TIMEOUT="gtimeout -v $RUN_TIME_LIMIT" # from `brew install coreutils`
  else
    echo -e "${BOLD_YELLOW}WARNING${RESET} gtimeout not available, benchmark runs may take indefinitely long."
  fi
fi

function check_command_installed {
  if ! [ -x "$(command -v $1)" ]; then
    echo "Error: $1 is not installed." >&2
    exit 1
  fi
}

function print_and_execute() {
  echo "+ $@" >&2
  "$@"
}

check_command_installed java

# Validate that ./calculate_average_<fork>.sh exists for each fork
for fork in "$@"; do
  if [ ! -f "./calculate_average_$fork.sh" ]; then
    echo -e "${BOLD_RED}ERROR${RESET}: ./calculate_average_$fork.sh does not exist." >&2
    exit 1
  fi
done

## SDKMAN Setup
# 1. Custom check for sdkman installed; not sure why check_command_installed doesn't detect it properly
if [ ! -f "$HOME/.sdkman/bin/sdkman-init.sh" ]; then
     echo -e "${BOLD_RED}ERROR${RESET}: sdkman is not installed." >&2
    exit 1
fi

# 2. Init sdkman in this script
source "$HOME/.sdkman/bin/sdkman-init.sh"

# 3. make sure the default java version is installed
if [ ! -d "$HOME/.sdkman/candidates/java/$DEFAULT_JAVA_VERSION" ]; then
  print_and_execute sdk install java $DEFAULT_JAVA_VERSION
fi

# 4. Install missing SDK java versions in any of the prepare_*.sh scripts for the provided forks
for fork in "$@"; do
  if [ -f "./prepare_$fork.sh" ]; then
    grep -h "^sdk use" "./prepare_$fork.sh" | cut -d' ' -f4 | while read -r version; do
      if [ ! -d "$HOME/.sdkman/candidates/java/$version" ]; then
        print_and_execute sdk install java $version
      fi
    done || true # grep returns exit code 1 when no match, `|| true` prevents the script from exiting early
  fi
done
## END - SDKMAN Setup

# Run tests and benchmark for each fork
filetimestamp=$(date  +"%Y%m%d%H%M%S") # same for all fork.out files from this run
failed=()
for fork in "$@"; do
  set +e # we don't want prepare.sh, test.sh or hyperfine failing on 1 fork to exit the script early

  # Run prepare script
  if [ -f "./prepare_$fork.sh" ]; then
    print_and_execute source "./prepare_$fork.sh"
  else
    print_and_execute sdk use java $DEFAULT_JAVA_VERSION
  fi

  # Run the test suite
  print_and_execute $TIMEOUT ./test.sh $fork
  if [ $? -ne 0 ]; then
    failed+=("$fork")
    echo ""
    echo -e "${BOLD_RED}FAILURE${RESET}: ./test.sh $fork failed"

    exit 1
  fi
  echo ""
done
