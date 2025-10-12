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


#!/bin/bash

java --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CreateMeasurements 10000000
./calculate_average_baseline.sh > measurements_10M.out
mv measurements.txt measurements_10M.txt

java --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CreateMeasurements 100000000
./calculate_average_baseline.sh > measurements_100M.out
mv measurements.txt measurements_100M.txt

java --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CreateMeasurements 1000000000
./calculate_average_baseline.sh > measurements_1000M.out
mv measurements.txt measurements_1000M.txt

ln -s measurements_1000M.txt measurements_1B.txt
ln -s measurements_1B.txt measurements.txt
ln -s measurements_1000M.out measurements_1B.out

