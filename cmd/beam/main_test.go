package main

import (
	"bytes"
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

type mockDataChannelStreamer struct {
	mu                   sync.Mutex
	sentChunks           [][]byte
	sentTexts            []string
	bufferedAmountVal    uint64
	bufferedThresholdVal uint64
	onLowCb              func()
	onSendCallback       func()
}

func (m *mockDataChannelStreamer) Send(data []byte) error {
	m.mu.Lock()
	m.sentChunks = append(m.sentChunks, data)
	cb := m.onSendCallback
	m.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

func (m *mockDataChannelStreamer) SendText(text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentTexts = append(m.sentTexts, text)
	return nil
}

func (m *mockDataChannelStreamer) BufferedAmount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bufferedAmountVal
}

func (m *mockDataChannelStreamer) setBufferedAmount(val uint64) {
	m.mu.Lock()
	m.bufferedAmountVal = val
	cb := m.onLowCb
	thresh := m.bufferedThresholdVal
	m.mu.Unlock()

	if val <= thresh && cb != nil {
		cb()
	}
}

func (m *mockDataChannelStreamer) SetBufferedAmountLowThreshold(threshold uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bufferedThresholdVal = threshold
}

func (m *mockDataChannelStreamer) OnBufferedAmountLow(f func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onLowCb = f
}

func TestStreamFileP2P_MemoryIsolation(t *testing.T) {
	// Create a temporary file with 150KB of data (> 2 chunks of 64KB)
	tmpFile, err := os.CreateTemp("", "beam_test_mem_iso_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	content := bytes.Repeat([]byte("ABCDEFGH12345678"), 10000) // 160,000 bytes
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	mockDC := &mockDataChannelStreamer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamFileP2P(ctx, mockDC, tmpFile.Name(), "test.bin", int64(len(content)), 0)

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()

	if len(mockDC.sentChunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(mockDC.sentChunks))
	}

	// Verify pointer addresses of chunk backing arrays are distinct
	p1 := &mockDC.sentChunks[0][0]
	p2 := &mockDC.sentChunks[1][0]
	if p1 == p2 {
		t.Fatalf("expected isolated memory buffers for chunks, but both chunks share buffer address %p", p1)
	}

	// Verify transmitted data integrity
	reconstructed := bytes.Join(mockDC.sentChunks, nil)
	if !bytes.Equal(reconstructed, content) {
		t.Fatalf("reconstructed data mismatch! sent %d bytes, original %d bytes", len(reconstructed), len(content))
	}
}

func TestStreamFileP2P_ContextCancellationOnDuplicateOffset(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_test_dup_offset_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// 10MB temp file so stream 1 would take multiple chunks
	bigContent := bytes.Repeat([]byte("X"), 10*1024*1024)
	if _, err := tmpFile.Write(bigContent); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	mockDC1 := &mockDataChannelStreamer{}
	ctx1, cancel1 := context.WithCancel(context.Background())

	// Start stream 1 in goroutine
	done1 := make(chan struct{})
	go func() {
		streamFileP2P(ctx1, mockDC1, tmpFile.Name(), "big.bin", int64(len(bigContent)), 0)
		close(done1)
	}()

	// Wait briefly for stream 1 to start
	time.Sleep(10 * time.Millisecond)

	// Simulate duplicate OFFSET arrival by cancelling stream 1 and starting stream 2
	cancel1()

	select {
	case <-done1:
		// Stream 1 terminated immediately on cancellation
	case <-time.After(1 * time.Second):
		t.Fatalf("stream 1 did not exit promptly after context cancellation")
	}

	mockDC2 := &mockDataChannelStreamer{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	// Stream 2 runs to completion from offset 1024
	streamFileP2P(ctx2, mockDC2, tmpFile.Name(), "big.bin", int64(len(bigContent)), 1024)

	mockDC2.mu.Lock()
	defer mockDC2.mu.Unlock()

	reconstructed := bytes.Join(mockDC2.sentChunks, nil)
	if len(reconstructed) != len(bigContent)-1024 {
		t.Fatalf("expected stream 2 to send %d bytes, got %d bytes", len(bigContent)-1024, len(reconstructed))
	}
}

func TestStreamFileP2P_HybridBackpressure(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_test_backpressure_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	content := bytes.Repeat([]byte("Z"), 300*1024) // 300KB
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	mockDC := &mockDataChannelStreamer{}
	paused := make(chan struct{}, 1)

	mockDC.onSendCallback = func() {
		mockDC.mu.Lock()
		numSent := len(mockDC.sentChunks)
		mockDC.mu.Unlock()

		// Trigger high water mark backpressure after sending 1 chunk
		if numSent == 1 {
			mockDC.setBufferedAmount(2 * 1024 * 1024) // 2MB > 1MB
			select {
			case paused <- struct{}{}:
			default:
			}
		} else if numSent > 1 {
			// Clear buffer on subsequent sends so final EOF flush drains cleanly
			mockDC.setBufferedAmount(0)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		streamFileP2P(ctx, mockDC, tmpFile.Name(), "bp.bin", int64(len(content)), 0)
		close(done)
	}()

	// Wait for stream to enter backpressure pause
	select {
	case <-paused:
	case <-time.After(1 * time.Second):
		t.Fatalf("stream did not trigger backpressure callback")
	}

	time.Sleep(30 * time.Millisecond)

	// Verify that sending is paused
	mockDC.mu.Lock()
	chunksDuringPause := len(mockDC.sentChunks)
	mockDC.mu.Unlock()
	if chunksDuringPause > 1 {
		t.Fatalf("stream continued sending during high-water mark backpressure! chunks: %d", chunksDuringPause)
	}

	// Lower buffered amount to trigger resume (< 512KB)
	mockDC.setBufferedAmount(400 * 1024)

	select {
	case <-done:
		// Completed cleanly
	case <-time.After(1 * time.Second):
		t.Fatalf("stream did not resume after buffer cleared")
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()
	reconstructed := bytes.Join(mockDC.sentChunks, nil)
	if !bytes.Equal(reconstructed, content) {
		t.Fatalf("reconstructed data mismatch after backpressure resume")
	}
}

func TestStreamFileP2P_EOFFlush(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_test_eof_flush_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	content := []byte("Small file content for EOF test")
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	mockDC := &mockDataChannelStreamer{}

	// Set initial buffered amount > 0 when read completes
	mockDC.onSendCallback = func() {
		mockDC.setBufferedAmount(64 * 1024)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	flushStarted := time.Now()
	done := make(chan struct{})
	go func() {
		streamFileP2P(ctx, mockDC, tmpFile.Name(), "eof.bin", int64(len(content)), 0)
		close(done)
	}()

	// After 15ms, clear the buffer to 0
	time.Sleep(15 * time.Millisecond)
	mockDC.setBufferedAmount(0)

	select {
	case <-done:
		flushDuration := time.Since(flushStarted)
		if flushDuration > 500*time.Millisecond {
			t.Fatalf("EOF flush took too long: %v", flushDuration)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("stream hung waiting for EOF buffer flush")
	}

	mockDC.mu.Lock()
	defer mockDC.mu.Unlock()

	hasEOF := false
	for _, text := range mockDC.sentTexts {
		if text == "EOF" {
			hasEOF = true
			break
		}
	}
	if !hasEOF {
		t.Fatalf("expected 'EOF' text message to be sent after buffer flush")
	}
}
