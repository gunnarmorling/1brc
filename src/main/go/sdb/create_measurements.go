//
//   Copyright 2023 The original authors
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

// # Based on https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CreateMeasurements.java and https://github.com/gunnarmorling/1brc/blob/main/src/main/python/create_measurements.py

package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type FileOpener interface {
	Open(name string) (*os.File, error)
}

type RealFileOpener struct{}

func (r RealFileOpener) Open(name string) (*os.File, error) {
	return os.Open(name)
}

type FileWriter interface {
	Create(name string) (*os.File, error)
	NewWriter(file *os.File) *bufio.Writer
}

type RealFileWriter struct{}

func (RealFileWriter) Create(name string) (*os.File, error) {
	return os.Create(name)
}

func (RealFileWriter) NewWriter(file *os.File) *bufio.Writer {
	return bufio.NewWriter(file)
}

type Random interface {
	Float64() float64
	Intn(n int) int
}

type StdRandom struct{}

func (StdRandom) Float64() float64 {
	return rand.Float64()
}

func (StdRandom) Intn(n int) int {
	return rand.Intn(n)
}

func checkArgs(args []string) (int, error) {
	if len(args) != 2 {
		return 0, fmt.Errorf("incorrect number of arguments - see example usage, go run create_measurements.go 1000")
	}
	numRows, err := strconv.Atoi(args[1])
	if err != nil || numRows <= 0 {
		return 0, fmt.Errorf("argument must be a positive integer -  - see example usage, go run create_measurements.go 1000")
	}
	return numRows, nil
}

func buildWeatherStationNameList(opener FileOpener) ([]string, error) {
	var stationNames []string

	file, err := opener.Open("../../../../data/weather_stations.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "#") {
			continue
		}
		station := strings.Split(line, ";")[0]
		stationNames = append(stationNames, station)
	}
	return stationNames, nil
}

func estimateFileSize(weatherStationNames []string, numRowsToCreate int) string {
	totalNameBytes := 0
	for _, name := range weatherStationNames {
		totalNameBytes += len(name)
	}
	avgNameBytes := totalNameBytes / len(weatherStationNames)
	avgTempBytes := 4.400200100050025
	avgLineLength := avgNameBytes + int(avgTempBytes) + 2
	fileSize := numRowsToCreate * avgLineLength
	return fmt.Sprintf("Estimated max file size is: %s.", convertBytes(fileSize))
}

func convertBytes(num int) string {
	units := []string{"bytes", "KiB", "MiB", "GiB"}
	var i int
	for num >= 1024 && i < len(units)-1 {
		num /= 1024
		i++
	}
	return fmt.Sprintf("%d %s", num, units[i])
}

func buildTestData(weatherStationNames []string, numRowsToCreate int, fileWriter FileWriter, random Random) error {
	startTime := time.Now()

	coldestTemp := -99.9
	hottestTemp := 99.9

	// Adjust the batchSize based on numRowsToCreate if less than 10,000
	batchSize := 10000
	if numRowsToCreate < batchSize {
		batchSize = numRowsToCreate
	}

	fmt.Println("Building test data...")

	// Initialize the file and writer
	file, err := fileWriter.Create("../../../../data/measurements.txt")
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	writer := fileWriter.NewWriter(file)
	defer writer.Flush()

	// Generate and write data in batches
	for i := 0; i < numRowsToCreate; i += batchSize {
		end := i + batchSize
		if end > numRowsToCreate {
			end = numRowsToCreate
		}

		for j := i; j < end; j++ {
			stationName := weatherStationNames[random.Intn(len(weatherStationNames))]
			temp := random.Float64()*(hottestTemp-coldestTemp) + coldestTemp
			line := fmt.Sprintf("%s;%.1f\n", stationName, temp)
			if _, err := writer.WriteString(line); err != nil {
				return fmt.Errorf("error writing string: %w", err)
			}
		}
	}

	fmt.Println("\nTest data successfully written.")
	fmt.Printf("Elapsed time: %s\n", time.Since(startTime))
	return nil
}

func main() {
	args := os.Args
	numRowsToCreate, err := checkArgs(args)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	opener := RealFileOpener{}
	weatherStationNames, err := buildWeatherStationNameList(opener)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(estimateFileSize(weatherStationNames, numRowsToCreate))

	fileWriter := RealFileWriter{}
	random := StdRandom{}

	// Call buildTestData with the concrete implementations.
	err = buildTestData(weatherStationNames, numRowsToCreate, fileWriter, random)
	if err != nil {
		fmt.Printf("Failed to build test data: %v\n", err)
		return
	}

	fmt.Println("Test data build complete.")
}
