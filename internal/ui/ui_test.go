package ui

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{-10, "0 B"},
		{0, "0 B"},
		{500, "500 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, FormatBytes(tc.input))
		})
	}
}

func TestCalculateProgressAndFormatPercentage(t *testing.T) {
	// Zero or negative total
	assert.Equal(t, 100.0, CalculateProgress(0, 0))
	assert.Equal(t, 100.0, CalculateProgress(10, 0))
	assert.Equal(t, 0.0, CalculateProgress(-5, 0))
	assert.Equal(t, "100.0%", FormatPercentage(0, 0))

	// Zero or negative current
	assert.Equal(t, 0.0, CalculateProgress(0, 100))
	assert.Equal(t, 0.0, CalculateProgress(-10, 100))
	assert.Equal(t, "0.0%", FormatPercentage(0, 100))

	// Normal progress
	assert.Equal(t, 50.0, CalculateProgress(50, 100))
	assert.Equal(t, "50.0%", FormatPercentage(50, 100))
	assert.Equal(t, "33.3%", FormatPercentage(1, 3))

	// Overshoot clamped to 100%
	assert.Equal(t, 100.0, CalculateProgress(150, 100))
	assert.Equal(t, "100.0%", FormatPercentage(150, 100))
}

func TestCalculateSpeedAndFormatSpeed(t *testing.T) {
	// Zero bytes or zero/negative duration
	assert.Equal(t, 0.0, CalculateSpeed(0, time.Second))
	assert.Equal(t, 0.0, CalculateSpeed(1000, 0))
	assert.Equal(t, 0.0, CalculateSpeed(1000, -time.Second))
	assert.Equal(t, "0 B/s", FormatSpeed(0, time.Second))
	assert.Equal(t, "0 B/s", FormatSpeed(1000, 0))

	// Normal speed
	speed := CalculateSpeed(1024*1024, time.Second)
	assert.Equal(t, float64(1024*1024), speed)
	assert.Equal(t, "1.0 MB/s", FormatSpeed(1024*1024, time.Second))

	// Instant / fast duration
	instantSpeed := CalculateSpeed(500, 10*time.Millisecond)
	assert.Equal(t, 50000.0, instantSpeed)
}

func TestCalculateETAAndFormatETA(t *testing.T) {
	// Zero remaining or invalid speed
	assert.Equal(t, time.Duration(0), CalculateETA(0, 1000))
	assert.Equal(t, time.Duration(0), CalculateETA(-10, 1000))
	assert.Equal(t, time.Duration(0), CalculateETA(1000, 0))
	assert.Equal(t, time.Duration(0), CalculateETA(1000, -5))
	assert.Equal(t, "--:--", FormatETA(0, 1000))
	assert.Equal(t, "--:--", FormatETA(1000, 0))

	// MM:SS format (< 1 hour)
	assert.Equal(t, "01:05", FormatETA(6500, 100)) // 65 seconds
	assert.Equal(t, "00:15", FormatETA(1500, 100)) // 15 seconds

	// HH:MM:SS format (>= 1 hour)
	assert.Equal(t, "01:01:05", FormatETA(366500, 100)) // 3665 seconds = 1h 1m 5s

	// Extreme ETA capped to 1 year
	capped := CalculateETA(1e18, 1)
	assert.Equal(t, 365*24*time.Hour, capped)
}

func TestRenderProgressBar(t *testing.T) {
	// Default width fallback if width < 5
	barNarrow := RenderProgressBar(50, 100, 2)
	assert.Len(t, barNarrow, len("[")+10*len("█")+10*len("░")+len("]"))

	// 0% progress with width 10
	bar0 := RenderProgressBar(0, 100, 10)
	assert.Equal(t, "["+strings.Repeat("░", 10)+"]", bar0)

	// 50% progress with width 10
	bar50 := RenderProgressBar(50, 100, 10)
	assert.Equal(t, "["+strings.Repeat("█", 5)+strings.Repeat("░", 5)+"]", bar50)

	// 100% progress with width 10
	bar100 := RenderProgressBar(100, 100, 10)
	assert.Equal(t, "["+strings.Repeat("█", 10)+"]", bar100)

	// Boundary: total <= 0
	barZeroTotal := RenderProgressBar(0, 0, 10)
	assert.Equal(t, "["+strings.Repeat("█", 10)+"]", barZeroTotal)
}

func TestHalfBlockChar(t *testing.T) {
	assert.Equal(t, "█", HalfBlockChar(true, true))
	assert.Equal(t, "▀", HalfBlockChar(true, false))
	assert.Equal(t, "▄", HalfBlockChar(false, true))
	assert.Equal(t, " ", HalfBlockChar(false, false))
}

func TestRenderHalfBlockLines(t *testing.T) {
	assert.Nil(t, RenderHalfBlockLines(nil))
	assert.Nil(t, RenderHalfBlockLines([][]bool{}))

	bitmap := [][]bool{
		{true, false, true},
		{true, true, false},
		{false, true, false},
		{false, false, true},
	}
	lines := RenderHalfBlockLines(bitmap)
	assert.Len(t, lines, 2)
	// Row 0 & 1: (true, true)->█, (false, true)->▄, (true, false)->▀
	assert.Equal(t, "  █▄▀", lines[0])
	// Row 2 & 3: (false, false)->" ", (true, false)->▀, (false, true)->▄
	assert.Equal(t, "   ▀▄", lines[1])
}

func TestColorize_NO_COLOR(t *testing.T) {
	// When NO_COLOR is set, colorization must be completely stripped
	os.Setenv("NO_COLOR", "1")
	defer os.Unsetenv("NO_COLOR")

	result := Colorize(cyan, "hello world")
	assert.Equal(t, "hello world", result)
}

func TestDisplayRoutines_NoPanic(t *testing.T) {
	// Capture standard output to verify functions do not panic
	rescueStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	PrintBanner()
	PrintFileMeta("testfile.bin", 1024*1024)
	PrintDiscovery("http://192.168.1.5:8080", "beam.local")
	PrintReceiverURL("http://192.168.1.5:8080")
	PrintWaiting(true)
	PrintWaiting(false)
	PrintMDNSError(errors.New("multicast lookup failed"))
	PrintQR("http://192.168.1.5:8080")

	w.Close()
	var buf bytes.Buffer
	io.Copy(&buf, r)
	os.Stdout = rescueStdout

	output := buf.String()
	assert.Contains(t, output, "testfile.bin")
	assert.Contains(t, output, "1.0 MB")
	assert.Contains(t, output, "192.168.1.5:8080")
	assert.Contains(t, output, "beam.local")
	assert.Contains(t, output, "multicast lookup failed")
}
