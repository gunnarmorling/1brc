package main

import (
	"flag"
	"fmt"
	mmap "github.com/edsrzf/mmap-go"
	"log"
	"os"
	"runtime/pprof"
	"slices"
)

type cityData struct {
	min, total, max int
	count           int
}

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
	cities := parseData1(data)
	printCities(cities)
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

func parseData1(data []byte) map[string]cityData {
	cities := make(map[string]cityData)
	state := parsingCityName
	var cityStartOffset, cityEndOffset int
	var temp, sign int

	cityStartOffset = 0
	for i, currentChar := range data {
		//fmt.Printf("%02d %c\n", i, currentChar)
		if state == parsingCityName && currentChar == ';' {
			state = skippingSemicolon
			cityEndOffset = i
		} else if state == parsingCityName {
			// do nothing
			//} else if state == parsingCityName && (currentChar&0x80 == 0) {
			//	// do nothing
			//} else if state == parsingCityName && (currentChar&0xE0 == 0xC0) {
			//	i++ // 2-byte utf8 char
			//} else if state == parsingCityName && (currentChar&0xF0 == 0xE0) {
			//	i += 2 // 3-byte utf8 char
			//} else if state == parsingCityName && (currentChar&0xF8 == 0xF0) {
			//	i += 3 // 4-byte utf8 char
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

func accumulate(cities map[string]cityData, city string, tempAsInt int) {
	if previous, ok := cities[city]; !ok {
		cities[city] = cityData{tempAsInt, tempAsInt, tempAsInt, 1}
	} else {
		var newMin, newMax int
		if tempAsInt < previous.min {
			newMin = tempAsInt
		} else {
			newMin = previous.min
		}
		if tempAsInt > previous.max {
			newMax = tempAsInt
		} else {
			newMax = previous.max
		}
		cities[city] = cityData{newMin, previous.total + tempAsInt, newMax, previous.count + 1}
	}
}

func printCities(cities map[string]cityData) {
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
