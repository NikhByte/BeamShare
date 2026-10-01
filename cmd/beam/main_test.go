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

func TestWaitDataChannelBuffer(t *testing.T) {
	ch := make(chan struct{}, 1)

	// Test 1: nil data channel returns error
	err := waitDataChannelBuffer(nil, ch, 100)
	if err == nil {
		t.Fatalf("expected error for nil data channel, got nil")
	}

	// Test 2: connected pair where buffer is 0 <= 1000 returns immediately
	api := webrtc.NewAPI()
	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create pc1: %v", err)
	}
	defer pc1.Close()

	pc2, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create pc2: %v", err)
	}
	defer pc2.Close()

	dc, err := pc1.CreateDataChannel("test", nil)
	if err != nil {
		t.Fatalf("failed to create dc: %v", err)
	}

	dcOpen := make(chan struct{})
	dc.OnOpen(func() {
		close(dcOpen)
	})

	// Wire ICE candidates between pc1 and pc2
	pc1.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pc2.AddICECandidate(c.ToJSON())
		}
	})
	pc2.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pc1.AddICECandidate(c.ToJSON())
		}
	})

	// Perform ICE handshake
	offer, err := pc1.CreateOffer(nil)
	if err != nil {
		t.Fatalf("failed offer: %v", err)
	}
	pc1.SetLocalDescription(offer)
	pc2.SetRemoteDescription(offer)

	answer, err := pc2.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("failed answer: %v", err)
	}
	pc2.SetLocalDescription(answer)
	pc1.SetRemoteDescription(answer)

	select {
	case <-dcOpen:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for data channel to open")
	}

	// Immediate return when BufferedAmount() <= target (0 <= 100)
	err = waitDataChannelBuffer(dc, ch, 100)
	if err != nil {
		t.Fatalf("expected nil for buffer <= target, got %v", err)
	}

	// Test channel / ticker signal unblock
	go func() {
		time.Sleep(30 * time.Millisecond)
		ch <- struct{}{}
	}()
	err = waitDataChannelBuffer(dc, ch, 100)
	if err != nil {
		t.Fatalf("expected nil on signal unblock, got %v", err)
	}

	// Test channel close during wait or beforehand
	pc1.Close()
	time.Sleep(50 * time.Millisecond)
	err = waitDataChannelBuffer(dc, ch, 100)
	if err == nil {
		t.Fatalf("expected error for closed data channel, got nil")
	}
}
