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
	"github.com/stretchr/testify/require"
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

func TestActiveChannelsPruningOnClose(t *testing.T) {
	channelsMu.Lock()
	activeChannels = nil
	channelsMu.Unlock()
	defer func() {
		channelsMu.Lock()
		activeChannels = nil
		channelsMu.Unlock()
	}()

	api := webrtc.NewAPI()
	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc1.Close()

	pc2, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc2.Close()

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

	dc1Open := make(chan struct{})
	dc2Open := make(chan struct{})

	dc1, err := pc1.CreateDataChannel("live1", nil)
	require.NoError(t, err)
	dc1.OnOpen(func() {
		channelsMu.Lock()
		dc1.OnClose(func() {
			channelsMu.Lock()
			defer channelsMu.Unlock()
			var updated []*webrtc.DataChannel
			for _, ch := range activeChannels {
				if ch != dc1 {
					updated = append(updated, ch)
				}
			}
			activeChannels = updated
		})
		activeChannels = append(activeChannels, dc1)
		channelsMu.Unlock()
		close(dc1Open)
	})

	dc2, err := pc1.CreateDataChannel("live2", nil)
	require.NoError(t, err)
	dc2.OnOpen(func() {
		channelsMu.Lock()
		dc2.OnClose(func() {
			channelsMu.Lock()
			defer channelsMu.Unlock()
			var updated []*webrtc.DataChannel
			for _, ch := range activeChannels {
				if ch != dc2 {
					updated = append(updated, ch)
				}
			}
			activeChannels = updated
		})
		activeChannels = append(activeChannels, dc2)
		channelsMu.Unlock()
		close(dc2Open)
	})

	offer, err := pc1.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, pc1.SetLocalDescription(offer))
	require.NoError(t, pc2.SetRemoteDescription(offer))

	answer, err := pc2.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, pc2.SetLocalDescription(answer))
	require.NoError(t, pc1.SetRemoteDescription(answer))

	select {
	case <-dc1Open:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for dc1 open")
	}
	select {
	case <-dc2Open:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for dc2 open")
	}

	channelsMu.Lock()
	count := len(activeChannels)
	channelsMu.Unlock()
	require.Equal(t, 2, count)

	// Close dc1
	require.NoError(t, dc1.Close())

	require.Eventually(t, func() bool {
		channelsMu.Lock()
		defer channelsMu.Unlock()
		return len(activeChannels) == 1 && activeChannels[0] == dc2
	}, 5*time.Second, 50*time.Millisecond)

	// Close dc2
	require.NoError(t, dc2.Close())

	require.Eventually(t, func() bool {
		channelsMu.Lock()
		defer channelsMu.Unlock()
		return len(activeChannels) == 0
	}, 5*time.Second, 50*time.Millisecond)
}
