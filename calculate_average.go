package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type cityData struct {
	min, total, max float64
	count           int
}

// open file 'measurements.txt'
// for each line in file
//
//	split line into fields separated by semicolon
//	convert field 2 to float64
//	add field 2 to sum
//	increment count
//
// close file
// calculate average
// print average
func main() {
	name := "measurements.txt"
	if os.Args[1] != "" {
		name = os.Args[1]
	}
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}

	cities := make(map[string]cityData)
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		city := parts[0]
		tempAsFloat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			panic(err)
		}
		if previous, ok := cities[city]; !ok {
			cities[city] = cityData{tempAsFloat, tempAsFloat, tempAsFloat, 1}
		} else {
			var newMin, newMax float64
			if tempAsFloat < previous.min {
				newMin = tempAsFloat
			} else {
				newMin = previous.min
			}
			if tempAsFloat > previous.max {
				newMax = tempAsFloat
			} else {
				newMax = previous.max
			}
			cities[city] = cityData{newMin, previous.total + tempAsFloat, newMax, previous.count + 1}
		}
	}
	fmt.Print("{")
	for city, data := range cities {
		average := data.total / float64(data.count)
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", city, data.min, average, data.max)
	}
	fmt.Print("}")
}
