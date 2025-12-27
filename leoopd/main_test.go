package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_main(t *testing.T) {
	// Prepare temporary working directory
	tmpDir := t.TempDir()

	// Create measurements.txt expected by main()
	input := `
Berlin;12.3
Paris;8.1
Rome;15.4
Madrid;20.2
Vienna;6.9
London;9.8
Berlin;11.7
Paris;7.5
Rome;16.1
Madrid;21.0
Vienna;7.2
London;10.3
Berlin;13.0
Paris;9.0
Rome;14.8
Madrid;19.6
Vienna;6.4
London;8.9
Berlin;10.9
Paris;8.7
Rome;15.9
Madrid;22.1
Vienna;7.8
London;11.1
Berlin;14.2
Paris;6.8
Rome;17.3
Madrid;18.9
Vienna;5.9
London;9.5
Berlin;12.8
Paris;7.9
Rome;16.5
Madrid;20.7
Vienna;6.6
London;10.0
Berlin;11.3
Paris;8.4
Rome;15.1
Madrid;21.4
Vienna;7.1
London;10.6
Berlin;13.5
Paris;9.2
Rome;14.6
Madrid;19.8
Vienna;6.2
London;9.1
Berlin;12.0
Paris;8.0
`
	input = strings.TrimSpace(input)

	filePath := filepath.Join(tmpDir, "measurements_1b.txt")
	if err := os.WriteFile(filePath, []byte(input), 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	// Switch working directory so ./measurements.txt resolves
	wd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(wd) })
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}

	// Capture stdout
	var buf bytes.Buffer
	stdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run
	main()

	// Restore stdout
	w.Close()
	os.Stdout = stdout
	buf.ReadFrom(r)

	output := buf.String()

	// Assertions (order-independent)
	if !strings.Contains(output, "Berlin;10.9;12.4;14.2") {
		t.Fatalf("unexpected output for Berlin:\n%s", output)
	}

	if !strings.Contains(output, "Paris;6.8;8.2;9.2") {
		t.Fatalf("unexpected output for Paris:\n%s", output)
	}

	if !strings.Contains(output, "Rome;14.6;15.7;17.3") {
		t.Fatalf("unexpected output for Rome:\n%s", output)
	}

	if !strings.Contains(output, "Madrid;18.9;20.5;22.1") {
		t.Fatalf("unexpected output for Madrid:\n%s", output)
	}

	if !strings.Contains(output, "Vienna;5.9;6.8;7.8") {
		t.Fatalf("unexpected output for Vienna:\n%s", output)
	}

	if !strings.Contains(output, "Paris;6.8;8.2;9.2") {
		t.Fatalf("unexpected output for London:\n%s", output)
	}
}
