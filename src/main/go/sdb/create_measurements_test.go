package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"testing"
)

type MockFileOpener struct{}

func (m MockFileOpener) Open(name string) (*os.File, error) {
	// Simulate file content with a reader
	content := "Station1\n#Comment\nStation2;Data\n"
	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tmpfile.Write([]byte(content)); err != nil {
		log.Fatal(err)
	}
	if _, err := tmpfile.Seek(0, 0); err != nil {
		log.Fatal(err)
	}

	return tmpfile, nil
}

// MockFileWriter simulates file operations for testing.
type MockFileWriter struct {
	Buffer *bytes.Buffer
}

func (m *MockFileWriter) Create(name string) (*os.File, error) {
	// For testing, we do not actually create a file.
	return &os.File{}, nil
}

func (m *MockFileWriter) NewWriter(file *os.File) *bufio.Writer {
	return bufio.NewWriter(m.Buffer)
}

// MockRandom simulates random number generation for testing.
type MockRandom struct {
	Floats   []float64
	index    int // current index in Floats
	Ints     []int
	intIndex int
}

func (m *MockRandom) Float64() float64 {
	if m.index >= len(m.Floats) {
		m.index = 0 // Reset or loop around if out of predefined floats
	}
	result := m.Floats[m.index]
	m.index++
	return result
}

func (m *MockRandom) Intn(n int) int {
	if m.intIndex >= len(m.Ints) {
		m.intIndex = 0 // Reset or loop around if out of predefined ints
	}
	result := m.Ints[m.intIndex]
	m.intIndex++
	return result
}

func TestBuildWeatherStationNameList(t *testing.T) {
	mockOpener := MockFileOpener{}
	stationNames, err := buildWeatherStationNameList(mockOpener)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(stationNames) != 2 || stationNames[0] != "Station1" || stationNames[1] != "Station2" {
		t.Errorf("Unexpected station names: %v", stationNames)
	}
}

func TestCheckArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantNum int
		wantErr bool
	}{
		{
			name:    "valid arguments",
			args:    []string{"cmd", "100"},
			wantNum: 100,
			wantErr: false,
		},
		{
			name:    "incorrect number of arguments - too few",
			args:    []string{"cmd"},
			wantNum: 0,
			wantErr: true,
		},
		{
			name:    "incorrect number of arguments - too many",
			args:    []string{"cmd", "100", "extra"},
			wantNum: 0,
			wantErr: true,
		},
		{
			name:    "non-integer argument",
			args:    []string{"cmd", "not-an-int"},
			wantNum: 0,
			wantErr: true,
		},
		{
			name:    "negative integer argument",
			args:    []string{"cmd", "-100"},
			wantNum: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNum, err := checkArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotNum != tt.wantNum {
				t.Errorf("checkArgs() gotNum = %v, want %v", gotNum, tt.wantNum)
			}
		})
	}
}

func TestConvertBytes(t *testing.T) {
	tests := []struct {
		input int
		want  string
	}{
		{1023, "1023 bytes"},
		{1024, "1 KiB"},
		{2048, "2 KiB"},
		{1048576, "1 MiB"},
	}

	for _, tt := range tests {
		got := convertBytes(tt.input)
		if got != tt.want {
			t.Errorf("convertBytes(%d) = %s; want %s", tt.input, got, tt.want)
		}
	}
}

func TestEstimateFileSize(t *testing.T) {

	tests := []struct {
		name                string
		weatherStationNames []string
		numRowsToCreate     int
		want                string // The expected result format
	}{
		{
			name:                "single short station name",
			weatherStationNames: []string{"StationA"},
			numRowsToCreate:     1,
			want:                "Estimated max file size is: 14 bytes.", // Adjust based on your calculation
		},
		{
			name:                "multiple station names",
			weatherStationNames: []string{"StationA", "LongerStationName"},
			numRowsToCreate:     2,
			want:                "Estimated max file size is: 36 bytes.", // Adjust based on your calculation
		},
		{
			name:                "large number of rows",
			weatherStationNames: []string{"StationA", "StationB"},
			numRowsToCreate:     10000,
			want:                "Estimated max file size is: 136 KiB.", // Adjust based on your calculation and rounding
		},
		{
			name:                "zero rows",
			weatherStationNames: []string{"StationA"},
			numRowsToCreate:     0,
			want:                "Estimated max file size is: 0 bytes.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateFileSize(tt.weatherStationNames, tt.numRowsToCreate)
			if got != tt.want {
				t.Errorf("estimateFileSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TODO - implement test for buildTestData
// func TestBuildTestData(t *testing.T) {

// }
