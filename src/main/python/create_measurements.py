#!/usr/bin/env python
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

# Based on https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CreateMeasurements.java

import os
import sys
import random
import time


def check_args(file_args):
    """
    Sanity checks out input and prints out usage if input is not a positive integer
    """
    try:
        if len(file_args) != 2 or int(file_args[1]) <= 0:
            raise Exception()
    except:
        print("Usage:  create_measurements.sh <positive integer number of records to create>")
        print("        You can use underscore notation for large number of records.")
        print("        For example:  1_000_000_000 for one billion")
        exit()


def build_weather_station_name_list():
    """
    Grabs the weather station names from example data provided in repo and dedups
    """
    station_names = []
    with open('../../../data/weather_stations.csv', 'r') as file:
        file_contents = file.read()
    for station in file_contents.splitlines():
        if "#" in station:
            next
        else:
            station_names.append(station.split(';')[0])
    return list(set(station_names))


def convert_bytes(num):
    """
    Convert bytes to a human-readable format (e.g., KiB, MiB, GiB)
    """
    for x in ['bytes', 'KiB', 'MiB', 'GiB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds):
    """
    Format elapsed time in a human-readable format
    """
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


def estimate_file_size(weather_station_names, num_rows_to_create):
    """
    Tries to estimate how large a file the test data will be
    """
    max_string = float('-inf')
    min_string = float('inf')
    per_record_size = 0
    record_size_unit = "bytes"

    for station in weather_station_names:
        if len(station) > max_string:
            max_string = len(station)
        if len(station) < min_string:
            min_string = len(station)
        per_record_size = ((max_string + min_string * 2) + len(",-123.4")) / 2

    total_file_size = num_rows_to_create * per_record_size
    human_file_size = convert_bytes(total_file_size)

    return f"Estimated max file size is:  {human_file_size}.\nTrue size is probably much smaller (around half)."


def build_test_data(weather_station_names, num_rows_to_create):
    """
    Generates and writes to file the requested length of test data
    """
    start_time = time.time()
    coldest_temp = -99.9
    hottest_temp = 99.9
    station_names_10k_max = random.choices(weather_station_names, k=10_000)
    batch_size = 10000 # instead of writing line by line to file, process a batch of stations and put it to disk
    progress_step = max(1, (num_rows_to_create // batch_size) // 100)
    print('Building test data...')

    try:
        with open("../../../data/measurements.txt", 'w') as file:
            for s in range(0,num_rows_to_create // batch_size):
                
                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = '\n'.join([f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}" for station in batch]) # :.1f should quicker than round on a large scale, because round utilizes mathematical operation
                file.write(prepped_deviated_batch + '\n')
                
                # Update progress bar every 1%
                if s % progress_step == 0 or s == num_rows_to_create - 1:
                    sys.stdout.write('\r')
                    sys.stdout.write("[%-50s] %d%%" % ('=' * int((s + 1) / num_rows_to_create * 50), (s + 1) / num_rows_to_create * 100))
                    sys.stdout.flush()
        sys.stdout.write('\n')
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        exit()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize("../../../data/measurements.txt")
    human_file_size = convert_bytes(file_size)
 
    print("Test data successfully written to 1brc/data/measurements.txt")
    print(f"Actual file size:  {human_file_size}")
    print(f"Elapsed time: {format_elapsed_time(elapsed_time)}")


def main():
    """
    main program function
    """
    check_args(sys.argv)
    num_rows_to_create = int(sys.argv[1])
    weather_station_names = []
    weather_station_names = build_weather_station_name_list()
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(weather_station_names, num_rows_to_create)
    print("Test data build complete.")


if __name__ == "__main__":
    main()
exit()
