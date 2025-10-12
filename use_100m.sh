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


# Remove symlinks if present
rm -f measurements.txt
rm -f measurements_1B.txt
rm -f measurements_1B.out

# Create symlinks
ln -s measurements_100M.txt measurements_1B.txt
ln -s measurements_100M.txt measurements.txt
ln -s measurements_100M.out measurements_1B.out