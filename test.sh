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
    echo "Usage: test.sh <fork name>"
    exit 1
fi

java --version

mvn clean verify

for sample in $(ls src/test/resources/samples/*.txt)
do
  echo "Validating calculate_average_$1.sh -- $sample"
  rm -f measurements.txt
  ln -s $sample measurements.txt

  diff <("./calculate_average_$1.sh") ${sample%.txt}.out
done
rm measurements.txt
