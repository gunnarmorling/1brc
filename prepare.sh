#!/bin/sh
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

set -x

if [ -z "$1" ]
  then
    echo "Usage: prepare.sh <fork name>:<branch name>"
    exit 1
fi

parts=(${1//:/ })
echo "  User: ${parts[0]}"
echo "Branch: ${parts[1]}"

git branch -D ${parts[0]} &>/dev/null

git checkout -b ${parts[0]}
git fetch https://github.com/${parts[0]}/1brc.git ${parts[1]}
# git fetch git@github.com:${parts[0]}/1brc.git ${parts[1]}
git reset --hard FETCH_HEAD
git rebase main
