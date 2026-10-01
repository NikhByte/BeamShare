package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/server"
)

func TestParseFlags(t *testing.T) {
	argsIn := []string{"--stun-server=stun:1", "--stun-server", "stun:2", "--turn-server=turn:1", "--turn-username", "user", "--turn-credential=pass", "--discovery-timeout=15", "send", "file.txt"}
	cleanArgs, iceServers, discoveryTimeout := parseFlags(argsIn)

	expectedArgs := []string{"send", "file.txt"}
	if !reflect.DeepEqual(cleanArgs, expectedArgs) {
		t.Fatalf("expected args %v, got %v", expectedArgs, cleanArgs)
	}

	if len(iceServers) != 2 {
		t.Fatalf("expected 2 ICE servers, got %d", len(iceServers))
	}

	if !reflect.DeepEqual(iceServers[0].URLs, []string{"stun:1", "stun:2"}) {
		t.Fatalf("expected stun URLs, got %v", iceServers[0].URLs)
	}

	if !reflect.DeepEqual(iceServers[1].URLs, []string{"turn:1"}) {
		t.Fatalf("expected turn URLs, got %v", iceServers[1].URLs)
	}

	if iceServers[1].Username != "user" || iceServers[1].Credential != "pass" {
		t.Fatalf("expected turn auth, got user=%v pass=%v", iceServers[1].Username, iceServers[1].Credential)
	}

	if discoveryTimeout != 15*1000*1000*1000 { // 15 seconds
		t.Fatalf("expected discovery timeout 15s, got %v", discoveryTimeout)
	}
}

func TestBufferSizeClamping(t *testing.T) {
	// Test excessively large buffer size gets clamped to 100MB
	argsExcessive := []string{"--buffer-size=1000000000", "send", "file.txt"}
	parseFlags(argsExcessive)
	if liveBufferSize != 100*1024*1024 {
		t.Fatalf("expected liveBufferSize clamped to 100MB, got %d", liveBufferSize)
	}

	// Test negative/sub-minimum buffer size gets clamped to 64KB
	argsSubMin := []string{"--buffer-size=100", "send", "file.txt"}
	parseFlags(argsSubMin)
	if liveBufferSize != 64*1024 {
		t.Fatalf("expected liveBufferSize clamped to 64KB, got %d", liveBufferSize)
	}
}

func TestDownloadFile_PlainHTTP(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		meta := server.FileMeta{
			Name: "test_download.txt",
			Size: 12,
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello World!"))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_download.txt")

	err := downloadFile(ts.URL)
	if err != nil {
		t.Fatalf("downloadFile failed: %v", err)
	}
}

func TestDownloadFile_Relay(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("s") != "session123" {
			t.Fatalf("expected session ID 'session123', got '%s'", r.URL.Query().Get("s"))
		}
		meta := server.FileMeta{
			Name: "test_download_relay.txt",
			Size: 15,
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("s") != "session123" {
			t.Fatalf("expected session ID 'session123', got '%s'", r.URL.Query().Get("s"))
		}
		w.Write([]byte("Relay Data 1234"))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_download_relay.txt")

	urlWithSession := "http://example.com/?backend=" + ts.URL + "&s=session123"

	err := downloadFile(urlWithSession)
	if err != nil {
		t.Fatalf("downloadFile failed: %v", err)
	}
}

type mockDataChannel struct {
	mu                  sync.Mutex
	sentChunks          [][]byte
	sentTexts           []string
	bufferedAmount      uint64
	bufferedThreshold   uint64
	onBufferedAmountLow func()
}

func newMockDataChannel() *mockDataChannel {
	return &mockDataChannel{
		sentChunks: make([][]byte, 0),
		sentTexts:  make([]string, 0),
	}
}

func (m *mockDataChannel) Send(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentChunks = append(m.sentChunks, data)
	return nil
}

func (m *mockDataChannel) SendText(text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentTexts = append(m.sentTexts, text)
	return nil
}

func (m *mockDataChannel) BufferedAmount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bufferedAmount
}

func (m *mockDataChannel) SetBufferedAmountLowThreshold(th uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bufferedThreshold = th
}

func (m *mockDataChannel) OnBufferedAmountLow(f func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onBufferedAmountLow = f
}

func (m *mockDataChannel) setBufferedAmount(amt uint64) {
	m.mu.Lock()
	m.bufferedAmount = amt
	cb := m.onBufferedAmountLow
	th := m.bufferedThreshold
	m.mu.Unlock()

	if amt <= th && cb != nil {
		cb()
	}
}

func TestStreamFile_IndependentBufferSlices(t *testing.T) {
	// Create a temporary file with 150KB of test data (spanning multiple 64KB chunks)
	tmpFile, err := os.CreateTemp("", "beam_test_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := make([]byte, 150*1024)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	mockDC := newMockDataChannel()
	ctx := context.Background()

	err = streamFile(ctx, mockDC, tmpFile.Name(), "test.bin", int64(len(content)), 0)
	if err != nil {
		t.Fatalf("streamFile failed: %v", err)
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()

	if len(mockDC.sentChunks) == 0 {
		t.Fatalf("expected sent chunks, got none")
	}

	// Verify each chunk slice points to distinct memory (independent buffer slices)
	pointers := make(map[*byte]bool)
	reconstructed := make([]byte, 0, len(content))
	for _, chunk := range mockDC.sentChunks {
		if len(chunk) == 0 {
			continue
		}
		ptr := &chunk[0]
		if pointers[ptr] {
			t.Fatalf("found duplicate slice pointer %p; chunk buffers were reused instead of independent allocations", ptr)
		}
		pointers[ptr] = true
		reconstructed = append(reconstructed, chunk...)
	}

	if !reflect.DeepEqual(reconstructed, content) {
		t.Fatalf("reconstructed content does not match original content")
	}

	if len(mockDC.sentTexts) == 0 || mockDC.sentTexts[len(mockDC.sentTexts)-1] != "EOF" {
		t.Fatalf("expected final text to be EOF, got %v", mockDC.sentTexts)
	}
}

func TestStreamFile_HybridBackpressure(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_bp_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := make([]byte, 200*1024)
	tmpFile.Write(content)
	tmpFile.Close()

	mockDC := newMockDataChannel()
	// Set initial buffered amount > 1MB (1.5MB)
	mockDC.setBufferedAmount(1500 * 1024)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doneChan := make(chan error, 1)
	go func() {
		doneChan <- streamFile(ctx, mockDC, tmpFile.Name(), "bp.bin", int64(len(content)), 0)
	}()

	// Verify that sender is paused while buffer > 1MB
	time.Sleep(100 * time.Millisecond)
	mockDC.mu.Lock()
	sentCountBefore := len(mockDC.sentChunks)
	mockDC.mu.Unlock()

	if sentCountBefore > 0 {
		t.Fatalf("expected 0 chunks sent while backpressure is active (>1MB), got %d", sentCountBefore)
	}

	// Drain buffer below 512KB (256KB) and then to 0
	mockDC.setBufferedAmount(256 * 1024)
	go func() {
		time.Sleep(50 * time.Millisecond)
		mockDC.setBufferedAmount(0)
	}()

	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("streamFile returned error after backpressure release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("streamFile timed out waiting for backpressure resume")
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()
	if len(mockDC.sentChunks) == 0 {
		t.Fatalf("expected chunks to be sent after buffer drained")
	}
}

func TestStreamFile_ContextCancellationOnDuplicateOffset(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_cancel_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := make([]byte, 200*1024)
	tmpFile.Write(content)
	tmpFile.Close()

	mockDC := newMockDataChannel()
	// Pause stream using high buffer amount
	mockDC.setBufferedAmount(2 * 1024 * 1024)

	ctx1, cancel1 := context.WithCancel(context.Background())

	errChan1 := make(chan error, 1)
	go func() {
		errChan1 <- streamFile(ctx1, mockDC, tmpFile.Name(), "cancel.bin", int64(len(content)), 0)
	}()

	time.Sleep(50 * time.Millisecond)
	// Duplicate OFFSET signal arrives: cancel routine 1
	cancel1()

	select {
	case err1 := <-errChan1:
		if err1 == nil {
			t.Fatalf("expected canceled context error from routine 1, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("routine 1 failed to terminate after context cancellation")
	}

	// Routine 2 starts after duplicate OFFSET
	mockDC.setBufferedAmount(0)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	err2 := streamFile(ctx2, mockDC, tmpFile.Name(), "cancel.bin", int64(len(content)), 0)
	if err2 != nil {
		t.Fatalf("routine 2 streamFile failed: %v", err2)
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()

	// Verify only 1 EOF token was sent across both routines
	eofCount := 0
	for _, text := range mockDC.sentTexts {
		if text == "EOF" {
			eofCount++
		}
	}
	if eofCount != 1 {
		t.Fatalf("expected exactly 1 EOF token sent, got %d", eofCount)
	}
}

func TestStreamFile_SafeEOFFlush_ZeroBuffer(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_eof_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := []byte("small file payload")
	tmpFile.Write(content)
	tmpFile.Close()

	mockDC := newMockDataChannel()
	mockDC.setBufferedAmount(0)

	start := time.Now()
	err = streamFile(context.Background(), mockDC, tmpFile.Name(), "eof.bin", int64(len(content)), 0)
	if err != nil {
		t.Fatalf("streamFile failed: %v", err)
	}

	if time.Since(start) > 2*time.Second {
		t.Fatalf("zero-buffer EOF flush took unexpectedly long")
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()

	if len(mockDC.sentTexts) == 0 || mockDC.sentTexts[len(mockDC.sentTexts)-1] != "EOF" {
		t.Fatalf("expected EOF sent, got %v", mockDC.sentTexts)
	}
}

func TestStreamFile_SafeEOFFlush_NonZeroBufferTimeout(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_eoftout_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := []byte("timeout test payload")
	tmpFile.Write(content)
	tmpFile.Close()

	mockDC := newMockDataChannel()
	// Keep buffered amount at 100 bytes continuously (never drains to 0)
	mockDC.setBufferedAmount(100)

	start := time.Now()
	err = streamFile(context.Background(), mockDC, tmpFile.Name(), "timeout.bin", int64(len(content)), 0)
	if err != nil {
		t.Fatalf("streamFile failed: %v", err)
	}

	elapsed := time.Since(start)
	// Bounded flush timeout is 5s, so it should take ~5s and not hang indefinitely
	if elapsed < 4500*time.Millisecond || elapsed > 8*time.Second {
		t.Fatalf("expected bounded timeout flush ~5s, took %v", elapsed)
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()

	if len(mockDC.sentTexts) == 0 || mockDC.sentTexts[len(mockDC.sentTexts)-1] != "EOF" {
		t.Fatalf("expected EOF sent after bounded timeout, got %v", mockDC.sentTexts)
	}
}

