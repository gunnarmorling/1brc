package main

import (
	"bufio"
	"bytes"
	"fmt"
	mmap "github.com/edsrzf/mmap-go"
	"os"
	"slices"
	"strconv"
)

type cityData struct {
	min, total, max int
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
	for i := 0; i < len(data); i++ {
		currentChar := data[i]
		//fmt.Printf("%02d %c\n", i, currentChar)
		if state == parsingCityName && currentChar == ';' {
			state = skippingSemicolon
			cityEndOffset = i
			//} else if state == parsingCityName {
			//	// do nothing
		} else if state == parsingCityName && (currentChar&0x80 == 0) {
			// do nothing
		} else if state == parsingCityName && (currentChar&0xE0 == 0xC0) {
			i++ // 2-byte utf8 char
		} else if state == parsingCityName && (currentChar&0xF0 == 0xE0) {
			i += 2 // 3-byte utf8 char
		} else if state == parsingCityName && (currentChar&0xF8 == 0xF0) {
			i += 3 // 4-byte utf8 char
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

func parseData(data []byte) map[string]cityData {
	cities := make(map[string]cityData)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Split(splitSemicolonOrNewline)

	for scanner.Scan() {
		city := scanner.Text()
		if !scanner.Scan() {
			break
		}
		tempAsFloat, err := strconv.ParseFloat(scanner.Text(), 64)
		tempAsInt := int(tempAsFloat * 10)
		if err != nil {
			panic(err)
		}
		accumulate(cities, city, tempAsInt)
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
		average := float64(data.total/data.count) / 10.0
		max := float64(data.max) / 10.0
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", city, min, average, max)
	}
	fmt.Print("}")
}
