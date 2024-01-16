package main

import (
	"bufio"
	"bytes"
	"fmt"
	mmap "github.com/edsrzf/mmap-go"
	"math"
	"os"
	"slices"
	"strconv"
)

type cityData struct {
	min, total, max float64
	count           int
}

func splitSemicolonOrNewline(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i := 0; i < len(data); i++ {
		if data[i] == ';' || data[i] == '\n' {
			return i + 1, data[:i], nil
		}
	}
	if !atEOF {
		return 0, nil, nil
	}
	// There is one final token to be delivered, which may be the empty string.
	// Returning bufio.ErrFinalToken here tells Scan there are no more tokens after this
	// but does not trigger an error to be returned from Scan itself.
	return 0, data, bufio.ErrFinalToken
}

func main() {
	name := "measurements.txt"
	if len(os.Args) == 2 {
		name = os.Args[1]
	}
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}

	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}

	cities := make(map[string]cityData)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Split(splitSemicolonOrNewline)

	for scanner.Scan() {
		city := scanner.Text()
		if !scanner.Scan() {
			break
		}
		tempAsFloat, err := strconv.ParseFloat(scanner.Text(), 64)
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
	keys := make([]string, 0)
	for k, _ := range cities {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, city := range keys {
		data := cities[city]
		average := data.total / float64(data.count)
		average = math.Round(average*10) / 10
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", city, data.min, average, data.max)
	}
	fmt.Print("}")
}
