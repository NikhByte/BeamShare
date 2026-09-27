package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
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

func TestBackpressureSelectLoop(t *testing.T) {
	bufferedAmountLowChan := make(chan struct{}, 1)

	var currentBuffer uint64 = 1500 * 1024 // 1.5MB > 1MB threshold

	getBufferedAmount := func() uint64 {
		return currentBuffer
	}

	waitBackpressure := func(target uint64) {
		for getBufferedAmount() > target {
			select {
			case <-bufferedAmountLowChan:
			case <-time.After(30 * time.Millisecond):
			}
		}
	}

	// Case 1: Timeout fallback resolves when buffer drops without explicit signal
	doneChan := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		currentBuffer = 400 * 1024
	}()

	go func() {
		waitBackpressure(1024 * 1024)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waitBackpressure deadlocked on timeout fallback")
	}

	// Case 2: Signal unblocks loop
	currentBuffer = 1500 * 1024
	doneChan2 := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		currentBuffer = 200 * 1024
		bufferedAmountLowChan <- struct{}{}
	}()

	go func() {
		waitBackpressure(1024 * 1024)
		close(doneChan2)
	}()

	select {
	case <-doneChan2:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waitBackpressure deadlocked on signal unblock")
	}

	// Case 3: EOF flush with target threshold 0
	currentBuffer = 100 * 1024
	doneChan3 := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		currentBuffer = 0
	}()

	go func() {
		waitBackpressure(0)
		close(doneChan3)
	}()

	select {
	case <-doneChan3:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waitBackpressure deadlocked on EOF flush")
	}
}
