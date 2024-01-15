package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// elh's 1BRC solution in Go
// See https://github.com/elh/1brc-go for README and development context.
//
// go run main.go [measurements_file]

var (
	// Optional env vars
	shouldProfile = os.Getenv("PROFILE") == "true"

	defaultMeasurementsPath = "measurements.txt"
	// others: "heap", "threadcreate", "block", "mutex"
	profileTypes = []string{"goroutine", "allocs"}
)

const (
	maxNameLen = 100
	maxNameNum = 10000

	// Tune these for performance
	minNumParsers  = 12 // currently configured to the number of runtime.NumCPU()
	parseChunkSize = 256 * 1024 * 1024
)

type Stats struct {
	Min, Max, Sum float64
	Count         int
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}

// parseFloatFast is a high performance float parser using the assumption that
// the byte slice will always have a single decimal digit.
func parseFloatFast(bs []byte) float64 {
	var intStartIdx int // is negative?
	if bs[0] == '-' {
		intStartIdx = 1
	}

	v := float64(bs[len(bs)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(bs) - 3; i >= intStartIdx; i-- { // integer part
		v += float64(bs[i]-'0') * place
		place *= 10
	}

	if intStartIdx == 1 {
		v *= -1
	}
	return v
}

// size is the intended number of bytes to parse. buffer should be longer than size
// because we need to continue reading until the end of the line in order to
// properly segment the entire file and not miss any data.
func parseAt(f *os.File, buf []byte, offset int64, size int) map[string]*Stats {
	stats := make(map[string]*Stats, maxNameNum)
	n, err := f.ReadAt(buf, offset) // load the buffer
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	lastName := make([]byte, maxNameLen) // last name parsed
	var lastNameLen int
	isScanningName := true // currently scanning name or value?

	// if offset is non-zero, skip to the first new line
	var idx, start int
	if offset != 0 {
		for idx < n {
			if buf[idx] == '\n' {
				idx++
				start = idx
				break
			}
			idx++
		}
	}
	// tick tock between parsing names and values while accummulating stats
	for {
		if isScanningName {
			for idx < n {
				if buf[idx] == ';' {
					nameBs := buf[start:idx]
					lastNameLen = copy(lastName, nameBs)

					idx++
					start = idx
					isScanningName = false
					break
				}
				idx++
			}
		} else {
			for idx < n {
				if buf[idx] == '\n' {
					valueBs := buf[start:idx]
					value := parseFloatFast(valueBs)

					nameUnsafe := unsafe.String(&lastName[0], lastNameLen)
					if s, ok := stats[nameUnsafe]; !ok {
						name := string(lastName[:lastNameLen]) // actually allocate string
						stats[name] = &Stats{Min: value, Max: value, Sum: value, Count: 1}
					} else {
						if value < s.Min {
							s.Min = value
						}
						if value > s.Max {
							s.Max = value
						}
						s.Sum += value
						s.Count++
					}

					idx++
					start = idx
					isScanningName = true
					break
				}
				idx++
			}
		}
		// terminate when we hit the first newline after the intended size OR
		// when we hit the end of the file
		if (isScanningName && idx >= size) || idx >= n {
			break
		}
	}

	return stats
}

func printResults(stats map[string]*Stats) { // doesn't help
	// sorted alphabetically for output
	names := make([]string, 0, len(stats))
	for name := range stats {
		names = append(names, name)
	}
	sort.Strings(names)

	var builder strings.Builder
	for i, name := range names {
		s := stats[name]
		// gotcha: first round the sum to to remove float precision errors!
		avg := round(round(s.Sum) / float64(s.Count))
		builder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, s.Min, avg, s.Max))
		if i < len(names)-1 {
			builder.WriteString(", ")
		}
	}

	writer := bufio.NewWriter(os.Stdout)
	fmt.Fprintf(writer, "{%s}\n", builder.String())
	writer.Flush()
}

// Read file in chunks and parse concurrently. N parsers work off of a chunk
// offset chan and send results on an output chan. The results are merged into a
// single map of stats and printed.
func main() {
	measurementsPath := defaultMeasurementsPath
	if len(os.Args) > 1 {
		measurementsPath = os.Args[1]
	}

	if shouldProfile {
		nowUnix := time.Now().Unix()
		os.MkdirAll(fmt.Sprintf("profiles/%d", nowUnix), 0755)
		for _, profileType := range profileTypes {
			file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.%s.pprof",
				nowUnix, filepath.Base(measurementsPath), profileType))
			defer file.Close()
			defer pprof.Lookup(profileType).WriteTo(file, 0)
		}

		file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.cpu.pprof",
			nowUnix, filepath.Base(measurementsPath)))
		defer file.Close()
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

	// read file
	f, err := os.Open(measurementsPath)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to open %s file: %w", measurementsPath, err))
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to read %s file: %w", measurementsPath, err))
	}

	// kick off "parser" workers
	numParsers := runtime.NumCPU()
	if minNumParsers > numParsers {
		numParsers = minNumParsers
	}

	wg := sync.WaitGroup{}
	wg.Add(numParsers)

	// buffered to not block on merging
	chunkOffsetCh := make(chan int64, numParsers)
	chunkStatsCh := make(chan map[string]*Stats, numParsers)

	go func() {
		for i := 0; i < int(info.Size()); i++ {
			chunkOffsetCh <- int64(i)
			i += parseChunkSize
		}
		close(chunkOffsetCh)
	}()

	for i := 0; i < numParsers; i++ {
		// WARN: w/ extra padding for line overflow. Each chunk should be read past
		// the intended size to the next new line. 128 bytes should be enough for
		// a max 100 byte name + the float value.
		buf := make([]byte, parseChunkSize+128)
		go func() {
			for chunkOffset := range chunkOffsetCh {
				chunkStatsCh <- parseAt(f, buf, chunkOffset, parseChunkSize)
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(chunkStatsCh)
	}()

	mergedStats := make(map[string]*Stats, maxNameNum)
	for chunkStats := range chunkStatsCh {
		for name, s := range chunkStats {
			if ms, ok := mergedStats[name]; !ok {
				mergedStats[name] = s
			} else {
				if s.Min < ms.Min {
					ms.Min = s.Min
				}
				if s.Max > ms.Max {
					ms.Max = s.Max
				}
				ms.Sum += s.Sum
				ms.Count += s.Count
			}
		}
	}

	printResults(mergedStats)
}
