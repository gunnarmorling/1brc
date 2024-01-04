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

exec sed '
#
# Transform calculate_average*.sh output into semicolon-separated values, one per line.
#

# 1. remove "{" and "}"
s/[{}]//g;

# 2. replace "=" and "/" with semicolon
s/[=/]/;/g;

# 3. id may contain comma, e.g. "Washington, D.C.;-15.1;14.8;44.8, Wau;-2.1;27.4;53.4"
# so replace ", " with a newline only if it is preceded by a digit
s/\([0-9]\), /\1\n/g
'
