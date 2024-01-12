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

# sdk use java 21.0.1-tem

JAVA_OPTS=""
java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_agoncal


# v1 - 73603 ms
# You are entering the The One Billion Row Challenge (1BRC) which is an exploration of how far modern Java can be pushed for aggregating one billion rows from a text file. Grab all the (virtual) threads, reach out to SIMD, optimize the GC, or pull any other trick, and create the fastest implementation for solving this task!
# The text file contains temperature values for a range of weather stations. Each row is one measurement in the format <string: station name>;<double: measurement>, with the measurement value having exactly one fractional digit. The following shows ten rows as an example:
# Hamburg;12.0
# Bulawayo;8.9
# Palembang;38.8
# St. John's;15.2
# Cracow;12.6
# Bridgetown;26.9
# Istanbul;6.2
# Roseau;34.4
# Conakry;31.2
# Istanbul;23.0
# You have to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit):
# {Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
# You must use Java 21.
# Create an algorithm in any way you see fit including parallelizing the computation, using the (incubating) Vector API, memory-mapping different sections of the file concurrently, using AppCDS, GraalVM, CRaC, etc. for speeding up the application start-up, choosing and tuning the garbage collector, and much more.
# No external library dependencies may be used.

# v2 - 71831 ms
# Being written in Java 21, please use records instead of classes for Measurement.

# v3 - 69333 ms
# If the temperatures are small numbers, why use double? Can't you use another datatype ?
#
# The profiler mentions that this line of code has very bad performance. Can you refactor it so it has better performance:
# ---
# String[] parts = line.split(";")
# ---
#
# There is a maximum of 10000 unique station names. Can you optimize the code taking this into account?
