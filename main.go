package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type CityData struct {
	min   float64
	max   float64
	sum   float64
	count int
}

const filename = "measurements-1b.txt"

func main() {
	fmt.Println("Let's process a billion rows!")

	cities := map[string]CityData{}
	fmt.Println(cities)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		city := parts[0]
		temp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Fatal(err)
		}

		data, present := cities[city]

		if !present {
			cities[city] = CityData{min: temp, max: temp, sum: temp, count: 1}
		} else {
			data.count++
			data.sum += temp

			if temp < data.min {
				data.min = temp
			}
			if temp > data.max {
				data.max = temp
			}

			cities[city] = data
		}
	}

	for city, data := range cities {
		mean := data.sum / float64(data.count)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", city, data.min, mean, data.max)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
