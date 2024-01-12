echo off
REM !/bin/bash
REM 
REM   Copyright 2023 The original authors
REM 
REM   Licensed under the Apache License, Version 2.0 (the "License");
REM   you may not use this file except in compliance with the License.
REM   You may obtain a copy of the License at
REM 
REM       http://www.apache.org/licenses/LICENSE-2.0
REM 
REM   Unless required by applicable law or agreed to in writing, software
REM   distributed under the License is distributed on an "AS IS" BASIS,
REM   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM   See the License for the specific language governing permissions and
REM   limitations under the License.
REM 

REM set -euo pipefail
SETLOCAL EnableDelayedExpansion

if [%1] == [] (
  echo "Usage: test.cmd <fork name>"
  exit 1
)

for /f "delims=" %%G in ('dir /b src\test\resources\samples\*.txt') do (
  echo Validating calculate_average_%1 -- %%G

  del -f measurements.txt
  mklink measurements.txt src\test\resources\samples\%%G

  set testdata=%%~nG
  echo src\test\resources\samples\%%~nG.out
  call calculate_average_%1.cmd > measurements.out
  fc measurements.out src\test\resources\samples\%%~nG.out || (Exit /b 1)
  del measurements.out
)
del measurements.txt
