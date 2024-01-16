package main

import (
	"flag"
	"fmt"
	mmap "github.com/edsrzf/mmap-go"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
)

type cityData struct {
	min, total, max int
	count           int
}

type results map[string]cityData

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	name := dataFileName()
	data := readData(name)
	if len(data) > 1_00_000 {
		numCpu := runtime.NumCPU()
		_, _ = fmt.Fprintf(os.Stderr, "parallel %d\n", numCpu)
		c := make(chan results)

		runInParallel(numCpu, data, c)
		cities := collectResults(c, numCpu)
		printCities(cities)
	} else {
		cities := parseData1(data)
		printCities(cities)
	}
}

func collectResults(c chan results, numCpu int) results {
	cities := <-c
	for i := 1; i < numCpu; i++ {
		cities = merge(cities, <-c)
	}
	return cities
}

func runInParallel(numCpu int, data []byte, c chan results) {
	// calculate the places to split the work
	increments := make([]int, numCpu+1)
	for i := 0; i < numCpu; i++ {
		increments[i] = i * len(data) / numCpu
		// adjust the increments so that they start on the beginning of a city
		for i > 0 && data[increments[i]-1] != '\n' {
			increments[i]--
		}
	}
	increments[numCpu] = len(data)

	for i := 0; i < numCpu; i++ {
		from := increments[i]
		to := increments[i+1]
		go func() {
			c <- parseData1(data[from:to])
		}()
	}
}

func merge(m0, m1 results) results {
	for k, data0 := range m0 {
		if data1, ok := m1[k]; ok {
			m1[k] = cityData{
				min(data0.min, data1.min),
				data0.total + data1.total,
				max(data0.max, data1.max),
				data0.count + data1.count}
		} else {
			m1[k] = data0
		}
	}
	return m1
}

func dataFileName() string {
	name := "measurements.txt"
	if len(os.Args) == 2 {
		name = os.Args[1]
	}
	return name
}

func readData(name string) []byte {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}

	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}
	return data
}

type state struct {
	name string
}

var (
	parsingCityName    = state{"parsingCityName"}
	skippingSemicolon  = state{"skippingSemicolon"}
	parsingTemperature = state{"parsingTemperature"}
)

func parseData1(data []byte) results {
	cities := make(results)
	state := parsingCityName
	var cityStartOffset, cityEndOffset int
	var temp, sign int

	cityStartOffset = 0
	for i, currentChar := range data {
		if state == parsingCityName && currentChar == ';' {
			state = skippingSemicolon
			cityEndOffset = i
		} else if state == parsingCityName {
			// do nothing
		} else if state == skippingSemicolon && currentChar == '-' {
			state = parsingTemperature
			temp = 0
			sign = -1
		} else if state == skippingSemicolon && currentChar >= '0' && currentChar <= '9' {
			state = parsingTemperature
			temp = int(currentChar - '0')
			sign = 1
		} else if state == parsingTemperature && currentChar >= '0' && currentChar <= '9' {
			temp = temp*10 + int(currentChar-'0')
		} else if state == parsingTemperature && currentChar == '.' {
			// do nothing
		} else if state == parsingTemperature && currentChar == '\n' {
			cityName := string(data[cityStartOffset:cityEndOffset])
			accumulate(cities, cityName, temp*sign)
			state = parsingCityName
			cityStartOffset = i + 1
		} else {
			panic(fmt.Sprintf("Unexpected: %s, %c", state, currentChar))
		}
	}
	return cities
}

func accumulate(cities results, city string, tempAsInt int) {
	if previous, ok := cities[city]; !ok {
		cities[city] = cityData{tempAsInt, tempAsInt, tempAsInt, 1}
	} else {
		newMin := min(previous.min, tempAsInt)
		newMax := max(previous.max, tempAsInt)
		newTotal := previous.total + tempAsInt
		cities[city] = cityData{newMin, newTotal, newMax, previous.count + 1}
	}
}

func printCities(cities results) {
	fmt.Print("{")
	keys := make([]string, 0)
	for k, _ := range cities {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, city := range keys {
		data := cities[city]
		min := float64(data.min) / 10.0
		average := formatTemperature(data.total / data.count)
		max := float64(data.max) / 10.0
		fmt.Printf(
			"%s=%.1f/%s/%.1f, ",
			city,
			min,
			average,
			max)
	}
	fmt.Print("}")
}

func formatTemperature(tempTimesTen int) string {
	if tempTimesTen >= 0 {
		return fmt.Sprintf("%d.%d", tempTimesTen/10, tempTimesTen%10)
	} else {
		return fmt.Sprintf("-%d.%d", -tempTimesTen/10, -tempTimesTen%10)
	}
}
