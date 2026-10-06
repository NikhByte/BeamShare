package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/relay"
	"github.com/beamshare/beam/internal/server"
	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
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

	if !reflect.DeepEqual(parsedTurnServers, []string{"turn:1"}) {
		t.Fatalf("expected parsedTurnServers ['turn:1'], got %v", parsedTurnServers)
	}
	if parsedTurnUsername != "user" || parsedTurnCredential != "pass" {
		t.Fatalf("expected parsed turn auth user=user pass=pass, got user=%s pass=%s", parsedTurnUsername, parsedTurnCredential)
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

func TestDownloadFile_PathTraversalSanitization(t *testing.T) {
	traversalCases := []struct {
		rawMetaName      string
		expectedFileName string
	}{
		{"../../etc/passwd", "received_passwd"},
		{"..\\..\\evil.bat", "received_evil.bat"},
		{"\x00../malicious.sh", "received_malicious.sh"},
		{"....", "received_download.bin"},
	}

	for _, tc := range traversalCases {
		t.Run(tc.rawMetaName, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
				meta := server.FileMeta{
					Name: tc.rawMetaName,
					Size: 10,
				}
				json.NewEncoder(w).Encode(meta)
			})
			mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("0123456789"))
			})

			ts := httptest.NewServer(mux)
			defer ts.Close()

			defer os.Remove(tc.expectedFileName)

			err := downloadFile(ts.URL)
			if err != nil {
				t.Fatalf("downloadFile failed for %s: %v", tc.rawMetaName, err)
			}

			if _, err := os.Stat(tc.expectedFileName); os.IsNotExist(err) {
				t.Fatalf("expected file %s to exist, but was not found", tc.expectedFileName)
			}
		})
	}
}

func TestWebRTCSenderGoroutineDeduplicationAndBackpressure(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sender_test.bin")
	// Set test payload size to 2MB to ensure active stream in-flight when offset is changed
	fileSize := int64(2 * 1024 * 1024)
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte(i % 251)
	}
	if err := os.WriteFile(filePath, testPayload, 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	txDCReady := make(chan struct{})
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to create sender session: %v", err)
	}
	defer senderSession.Close()

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		close(txDCReady)

		var (
			senderCancel context.CancelFunc
			senderMu     sync.Mutex
		)

		dc.OnClose(func() {
			senderMu.Lock()
			if senderCancel != nil {
				senderCancel()
				senderCancel = nil
			}
			senderMu.Unlock()
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if !msg.IsString {
				return
			}
			dataStr := string(msg.Data)
			if strings.HasPrefix(dataStr, "OFFSET:") {
				parts := strings.SplitN(dataStr, ":", 2)
				var offset int64
				if len(parts) == 2 {
					offset, _ = strconv.ParseInt(parts[1], 10, 64)
				}

				senderMu.Lock()
				if senderCancel != nil {
					senderCancel()
				}
				var ctx context.Context
				ctx, senderCancel = context.WithCancel(mainCtx)
				senderMu.Unlock()

				go streamFileP2P(ctx, dc, filePath, filepath.Base(filePath), fileSize, offset)
			}
		})
	}

	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create rxPC: %v", err)
	}
	defer rxPC.Close()

	var mu sync.Mutex
	var rxCandidates []webrtc.ICECandidateInit
	rxPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			cand := c.ToJSON()
			mu.Lock()
			rxCandidates = append(rxCandidates, cand)
			mu.Unlock()
			_ = senderSession.AddICECandidate(cand)
		}
	})

	var rxDC *webrtc.DataChannel
	rxOpenCh := make(chan struct{})
	rxDCChan := make(chan *webrtc.DataChannel, 1)

	rxPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			select {
			case <-rxOpenCh:
			default:
				close(rxOpenCh)
			}
		})
		rxDCChan <- dc
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	if err != nil {
		t.Fatalf("CreateOffer failed: %v", err)
	}

	if err := rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  senderSession.RawOffer(),
	}); err != nil {
		t.Fatalf("SetRemoteDescription failed: %v", err)
	}

	for _, cand := range senderSession.GetCandidates() {
		_ = rxPC.AddICECandidate(cand)
	}

	senderSession.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = rxPC.AddICECandidate(c.ToJSON())
		}
	})

	answer, err := rxPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("CreateAnswer failed: %v", err)
	}
	if err := rxPC.SetLocalDescription(answer); err != nil {
		t.Fatalf("SetLocalDescription failed: %v", err)
	}

	answerBytes, _ := json.Marshal(answer)
	if err := senderSession.ProvideAnswer(string(answerBytes)); err != nil {
		t.Fatalf("ProvideAnswer failed: %v", err)
	}

	mu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	mu.Unlock()

	select {
	case rxDC = <-rxDCChan:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rxDC")
	}

	select {
	case <-rxOpenCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rxOpenCh")
	}

	select {
	case <-txDCReady:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for txDCReady")
	}

	var receivedBuf bytes.Buffer
	eofChan := make(chan struct{})
	firstChunkReceived := make(chan struct{}, 1)

	rxDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()
		if msg.IsString {
			str := string(msg.Data)
			if strings.HasPrefix(str, "META:") {
				receivedBuf.Reset()
			} else if str == "EOF" {
				select {
				case <-eofChan:
				default:
					close(eofChan)
				}
			}
		} else {
			receivedBuf.Write(msg.Data)
			select {
			case firstChunkReceived <- struct{}{}:
			default:
			}
		}
	})

	// Send initial OFFSET:0 to start streaming
	if err := rxDC.SendText("OFFSET:0"); err != nil {
		t.Fatalf("SendText OFFSET:0 failed: %v", err)
	}

	// Wait until at least 1 chunk is received from the first stream
	select {
	case <-firstChunkReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first chunk")
	}

	// Reset buffer and send second OFFSET:32768 while the first stream is actively running
	offsetSecond := int64(32768)
	mu.Lock()
	receivedBuf.Reset()
	mu.Unlock()

	if err := rxDC.SendText(fmt.Sprintf("OFFSET:%d", offsetSecond)); err != nil {
		t.Fatalf("SendText OFFSET second failed: %v", err)
	}

	select {
	case <-eofChan:
		mu.Lock()
		receivedBytes := receivedBuf.Bytes()
		mu.Unlock()

		expectedBytes := testPayload[offsetSecond:]
		if !bytes.Equal(receivedBytes, expectedBytes) {
			t.Fatalf("payload mismatch! expected length %d, got %d", len(expectedBytes), len(receivedBytes))
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for EOF after deduplication stream")
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

func TestDownloadFile_InvalidKey(t *testing.T) {
	requestsCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsCount++
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer ts.Close()

	// 1. Invalid base64 string
	invalidB64URL := "http://example.com/?backend=" + ts.URL + "#k=invalid!base64!key!"
	err := downloadFile(invalidB64URL)
	if err == nil {
		t.Fatal("expected error for malformed base64 key, got nil")
	}
	if !strings.Contains(err.Error(), "invalid base64 key encoding") {
		t.Fatalf("expected base64 encoding error, got: %v", err)
	}

	// 2. Non-32-byte decoded key (16 bytes)
	shortKey := make([]byte, 16)
	shortKeyB64 := base64.URLEncoding.EncodeToString(shortKey)
	shortKeyURL := "http://example.com/?backend=" + ts.URL + "#k=" + shortKeyB64
	err = downloadFile(shortKeyURL)
	if err == nil {
		t.Fatal("expected error for 16-byte key, got nil")
	}
	if !strings.Contains(err.Error(), "invalid encryption key length: expected 32 bytes, got 16") {
		t.Fatalf("expected 32-byte length error, got: %v", err)
	}

	if requestsCount != 0 {
		t.Fatalf("expected 0 HTTP requests when key is invalid, got %d", requestsCount)
	}
}

func TestDownloadFile_ValidEncrypted(t *testing.T) {
	validKey := make([]byte, 32)
	for i := range validKey {
		validKey[i] = byte(i + 1)
	}
	validKeyB64 := base64.URLEncoding.EncodeToString(validKey)

	plainContent := []byte("Encrypted Relay Stream Payload Test!")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		meta := server.FileMeta{
			Name: "test_encrypted.txt",
			Size: int64(len(plainContent)),
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		encReader, err := relay.NewEncryptingReader(bytes.NewReader(plainContent), validKey)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		io.Copy(w, encReader)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_encrypted.txt")

	encryptedURL := "http://example.com/?backend=" + ts.URL + "#k=" + validKeyB64
	err := downloadFile(encryptedURL)
	if err != nil {
		t.Fatalf("downloadFile failed for valid key: %v", err)
	}

	data, err := os.ReadFile("received_test_encrypted.txt")
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}
	if !bytes.Equal(data, plainContent) {
		t.Fatalf("expected decrypted content '%s', got '%s'", string(plainContent), string(data))
	}
}
