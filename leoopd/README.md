# 1brc/leoopd@V0.0.1

# Focus

Prove of concept, minimal setup

# Flow

1) Open measurements-file from hardcoded filepath
2) Use a bufio.Scanner to read lines
3) Split lines at ";"
4) Create a map map[string]Entry to hold data for each city
5) Output map to stdOut using fmt.Println() using the correct output format

# Tests

Basic ChatGPT unit test to confirm results

# Runs

16-core Intel Core i7-13700KF, NVIDIA GeForce RTX 4070 Ti, 32 GiB DDR5 4800 MT/s

opening: 13.462µs, scan: 1m25.267554272s, output: 10.89357ms, all: 1m25.278461513
opening: 14.238µs, scan: 1m24.909007679s, output: 11.269563ms, all: 1m24.920291657
opening: 8.846µs, scan: 1m25.083524445s, output: 10.781262ms, all: 1m25.094314832
opening: 14.959µs, scan: 1m25.130260341s, output: 11.093358ms, all: 1m25.141368864
opening: 16.787µs, scan: 1m25.029442598s, output: 10.7964ms, all: 1m25.040255985
