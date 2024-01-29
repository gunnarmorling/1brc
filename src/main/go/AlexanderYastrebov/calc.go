package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
)

type measurement struct {
	min, max, sum, count int64
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Missing measurements filename")
	}

	measurements := processFile(os.Args[1])

	ids := make([]string, 0, len(measurements))
	for id := range measurements {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Print("{")
	for i, id := range ids {
		if i > 0 {
			fmt.Print(", ")
		}
		m := measurements[id]
		fmt.Printf("%s=%.1f/%.1f/%.1f", id, round(float64(m.min)/10.0), round(float64(m.sum)/10.0/float64(m.count)), round(float64(m.max)/10.0))
	}
	fmt.Println("}")
}

func processFile(filename string) map[string]*measurement {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("Stat: %v", err)
	}

	size := fi.Size()
	if size <= 0 || size != int64(int(size)) {
		log.Fatalf("Invalid file size: %d", size)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	defer func() {
		if err := syscall.Munmap(data); err != nil {
			log.Fatalf("Munmap: %v", err)
		}
	}()

	return process(data)
}

func process(data []byte) map[string]*measurement {
	nChunks := runtime.NumCPU()

	chunkSize := len(data) / nChunks
	if chunkSize == 0 {
		chunkSize = len(data)
	}

	chunks := make([]int, 0, nChunks)
	offset := 0
	for offset < len(data) {
		offset += chunkSize
		if offset >= len(data) {
			chunks = append(chunks, len(data))
			break
		}

		nlPos := bytes.IndexByte(data[offset:], '\n')
		if nlPos == -1 {
			chunks = append(chunks, len(data))
			break
		} else {
			offset += nlPos + 1
			chunks = append(chunks, offset)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	results := make([]map[string]*measurement, len(chunks))
	start := 0
	for i, chunk := range chunks {
		go func(data []byte, i int) {
			results[i] = processChunk(data)
			wg.Done()
		}(data[start:chunk], i)
		start = chunk
	}
	wg.Wait()

	measurements := make(map[string]*measurement)
	for _, r := range results {
		for id, rm := range r {
			m := measurements[id]
			if m == nil {
				measurements[id] = rm
			} else {
				m.min = min(m.min, rm.min)
				m.max = max(m.max, rm.max)
				m.sum += rm.sum
				m.count += rm.count
			}
		}
	}
	return measurements
}

func processChunk(data []byte) map[string]*measurement {
	// Use fixed size linear probe lookup table
	const (
		// use power of 2 for fast modulo calculation,
		// should be larger than max number of keys which is 10_000
		entriesSize = 1 << 14

		// use FNV-1a hash
		fnv1aOffset64 = 14695981039346656037
		fnv1aPrime64  = 1099511628211
	)

	type entry struct {
		m     measurement
		hash  uint64
		vlen  int
		value [128]byte // use power of 2 > 100 for alignment
	}
	entries := make([]entry, entriesSize)
	entriesCount := 0

	// keep short and inlinable
	getMeasurement := func(hash uint64, value []byte) *measurement {
		i := hash & uint64(entriesSize-1)
		entry := &entries[i]

		// bytes.Equal could be commented to speedup assuming no hash collisions
		for entry.vlen > 0 && !(entry.hash == hash && bytes.Equal(entry.value[:entry.vlen], value)) {
			i = (i + 1) & uint64(entriesSize-1)
			entry = &entries[i]
		}

		if entry.vlen == 0 {
			entry.hash = hash
			entry.vlen = copy(entry.value[:], value)
			entriesCount++
		}
		return &entry.m
	}

	// assume valid input
	for len(data) > 0 {

		idHash := uint64(fnv1aOffset64)
		semiPos := 0
		for i, b := range data {
			if b == ';' {
				semiPos = i
				break
			}

			// calculate FNV-1a hash
			idHash ^= uint64(b)
			idHash *= fnv1aPrime64
		}

		idData := data[:semiPos]

		data = data[semiPos+1:]

		var temp int64
		// parseNumber
		{
			negative := data[0] == '-'
			if negative {
				data = data[1:]
			}

			_ = data[3]
			if data[1] == '.' {
				// 1.2\n
				temp = int64(data[0])*10 + int64(data[2]) - '0'*(10+1)
				data = data[4:]
				// 12.3\n
			} else {
				_ = data[4]
				temp = int64(data[0])*100 + int64(data[1])*10 + int64(data[3]) - '0'*(100+10+1)
				data = data[5:]
			}

			if negative {
				temp = -temp
			}
		}

		m := getMeasurement(idHash, idData)
		if m.count == 0 {
			m.min = temp
			m.max = temp
			m.sum = temp
			m.count = 1
		} else {
			m.min = min(m.min, temp)
			m.max = max(m.max, temp)
			m.sum += temp
			m.count++
		}
	}

	result := make(map[string]*measurement, entriesCount)
	for i := range entries {
		entry := &entries[i]
		if entry.m.count > 0 {
			result[string(entry.value[:entry.vlen])] = &entry.m
		}
	}
	return result
}

func round(x float64) float64 {
	return roundJava(x*10.0) / 10.0
}

// roundJava returns the closest integer to the argument, with ties
// rounding to positive infinity, see java's Math.round
func roundJava(x float64) float64 {
	t := math.Trunc(x)
	if x < 0.0 && t-x == 0.5 {
		//return t
	} else if math.Abs(x-t) >= 0.5 {
		t += math.Copysign(1, x)
	}

	if t == 0 { // check -0
		return 0.0
	}
	return t
}

// parseNumber reads decimal number that matches "^-?[0-9]{1,2}[.][0-9]" pattern,
// e.g.: -12.3, -3.4, 5.6, 78.9 and return the value*10, i.e. -123, -34, 56, 789.
func parseNumber(data []byte) int64 {
	negative := data[0] == '-'
	if negative {
		data = data[1:]
	}

	var result int64
	switch len(data) {
	// 1.2
	case 3:
		result = int64(data[0])*10 + int64(data[2]) - '0'*(10+1)
	// 12.3
	case 4:
		result = int64(data[0])*100 + int64(data[1])*10 + int64(data[3]) - '0'*(100+10+1)
	}

	if negative {
		return -result
	}
	return result
}
