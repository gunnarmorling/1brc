package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	startRoot := time.Now()
	file, err := os.Open("measurements_1b.txt")
	if err != nil {
		log.Fatal("unable to open measurements, err: ", err)
	}
	defer file.Close()
	durationOpen := time.Since(startRoot)

	entries := make(map[string]*Entry)
	scanner := bufio.NewScanner(file)

	startScan := time.Now()
	for scanner.Scan() {
		lineElements := strings.Split(scanner.Text(), ";")
		cityName := lineElements[0]
		measurement, err := strconv.ParseFloat(lineElements[1], 64)
		if err != nil {
			log.Panic("unable to parse measurement, err:", err)
		}

		if entry, ok := entries[cityName]; ok {
			entry.housekeep(measurement)
		} else {
			entries[cityName] = NewEntry(measurement)
		}
	}
	durationScan := time.Since(startScan)
	startOutput := time.Now()

	for city, entry := range entries {
		fmt.Printf("%s;%.1f;%.1f;%.1f\n", city, entry.Min, entry.Sum/entry.Count, entry.Max) // min, mean, max
	}

	durationOutput := time.Since(startOutput)
	durationAll := time.Since(startRoot)

	fmt.Printf("opening: %v, scan: %v, output: %v, all: %v\n", durationOpen, durationScan,durationOutput, durationAll) 

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

type Entry struct {
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func NewEntry(measurement float64) *Entry {
	return &Entry{
		Min:   measurement,
		Max:   measurement,
		Sum:   measurement,
		Count: 1,
	}
}

func (e *Entry) housekeep(measurement float64) {
	e.Count++
	e.Sum += measurement

	if measurement < e.Min {
		e.Min = measurement
	}

	if measurement > e.Max {
		e.Max = measurement
	}
}
