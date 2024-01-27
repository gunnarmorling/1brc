# 1brc in go

It uses Docker with BuildKit plugin to build and [export binary](https://docs.docker.com/engine/reference/commandline/build/#output) binary,
see [prepare_AlexanderYastrebov.sh](../../../../prepare_AlexanderYastrebov.sh)
and [calculate_average_AlexanderYastrebov.sh](../../../../calculate_average_AlexanderYastrebov.sh).

Demo:
```sh
$ ./test.sh AlexanderYastrebov
[+] Building 0.2s (9/9) FINISHED
 => [internal] load .dockerignore                                                                                                             0.0s
 => => transferring context: 2B                                                                                                               0.0s
 => [internal] load build definition from Dockerfile                                                                                          0.0s
 => => transferring dockerfile: 172B                                                                                                          0.0s
 => [internal] load metadata for docker.io/library/golang:latest                                                                              0.0s
 => [internal] load build context                                                                                                             0.0s
 => => transferring context: 145B                                                                                                             0.0s
 => [build-stage 1/3] FROM docker.io/library/golang                                                                                           0.0s
 => CACHED [build-stage 2/3] COPY . src/                                                                                                      0.0s
 => CACHED [build-stage 3/3] RUN cd src && go build .                                                                                         0.0s
 => CACHED [export-stage 1/1] COPY --from=build-stage /go/src/1brc /                                                                          0.0s
 => exporting to client directory                                                                                                             0.1s
 => => copying files 2.03MB                                                                                                                   0.0s
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-10000-unique-keys.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-10.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-1.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-20.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-2.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-3.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-boundaries.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-complex-utf8.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-dot.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-shortest.txt
Validating calculate_average_AlexanderYastrebov.sh -- src/test/resources/samples/measurements-short.txt

# Run once to setup the benchmark
# ./create_measurements.sh 1000000000
# mv measurements.txt measurements_1B.txt
# ln -s measurements_1B.txt measurements.txt
# ./calculate_average_baseline.sh > out_expected.txt

$ wc -l measurements_1B.txt
1000000000 measurements_1B.txt

$ ./evaluate2.sh AlexanderYastrebov royvanrijn
...                                                                                                                                         0.0s
Benchmark 1: ./calculate_average_AlexanderYastrebov.sh 2>&1
  Time (mean ± σ):     16.786 s ±  0.545 s    [User: 56.030 s, System: 10.068 s]
  Range (min … max):   15.918 s … 17.309 s    5 runs
...
Benchmark 1: ./calculate_average_royvanrijn.sh 2>&1
  Time (mean ± σ):     16.731 s ±  0.190 s    [User: 56.485 s, System: 10.279 s]
  Range (min … max):   16.490 s … 16.951 s    5 runs

Summary
  AlexanderYastrebov: trimmed mean 16.901712789513336, raw times 16.69836470718,17.30911065018,16.83413600418,15.91787706218,17.17263765718
  royvanrijn: trimmed mean 16.738037123633333, raw times 16.4900939703,16.9513459953,16.5794539913,16.8297746273,16.8048827523
```
