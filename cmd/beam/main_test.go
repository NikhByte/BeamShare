package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

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

func TestWaitBufferedAmount_BelowThreshold(t *testing.T) {
	api := webrtc.NewAPI()
	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create PeerConnection: %v", err)
	}
	defer pc1.Close()

	dc, err := pc1.CreateDataChannel("test", nil)
	if err != nil {
		t.Fatalf("failed to create DataChannel: %v", err)
	}

	lowChan := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		waitBufferedAmount(dc, lowChan, 1024*1024, 512*1024)
		close(done)
	}()

	select {
	case <-done:
		// Success: returned immediately without blocking
	case <-time.After(1 * time.Second):
		t.Fatal("waitBufferedAmount timed out on empty buffer")
	}
}

func TestWaitBufferedAmount_ClosedChannel(t *testing.T) {
	api := webrtc.NewAPI()
	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create PeerConnection: %v", err)
	}
	defer pc1.Close()

	dc, err := pc1.CreateDataChannel("test", nil)
	if err != nil {
		t.Fatalf("failed to create DataChannel: %v", err)
	}

	lowChan := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		waitBufferedAmount(dc, lowChan, 0, 0)
		close(done)
	}()

	select {
	case <-done:
		// Success: returned immediately for non-open channel
	case <-time.After(1 * time.Second):
		t.Fatal("waitBufferedAmount timed out on non-open DataChannel")
	}
}

func TestWaitBufferedAmount_SignalDrivenAndPollingFallback(t *testing.T) {
	txDCReady := make(chan struct{})
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to create sender session: %v", err)
	}
	defer senderSession.Close()

	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		close(txDCReady)
	}

	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create rxPC: %v", err)
	}
	defer rxPC.Close()

	rxDataChannelCh := make(chan *webrtc.DataChannel, 1)
	rxPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		rxDataChannelCh <- dc
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	if err != nil {
		t.Fatalf("failed to create offer: %v", err)
	}

	err = rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  senderSession.RawOffer(),
	})
	if err != nil {
		t.Fatalf("failed to set remote description: %v", err)
	}

	answer, err := rxPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("failed to create answer: %v", err)
	}

	err = rxPC.SetLocalDescription(answer)
	if err != nil {
		t.Fatalf("failed to set local description: %v", err)
	}

	answerBytes, _ := json.Marshal(answer)
	err = senderSession.ProvideAnswer(string(answerBytes))
	if err != nil {
		t.Fatalf("failed to provide answer: %v", err)
	}

	select {
	case <-txDCReady:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for sender DataChannel ready")
	}

	txDC := senderSession.DataChannel()
	if txDC == nil {
		t.Fatal("sender DataChannel is nil")
	}

	lowChan := make(chan struct{}, 1)
	txDC.OnBufferedAmountLow(func() {
		select {
		case lowChan <- struct{}{}:
		default:
		}
	})

	data := make([]byte, 100*1024)
	_ = txDC.Send(data)

	done := make(chan struct{})
	go func() {
		waitBufferedAmount(txDC, lowChan, 0, 0)
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("waitBufferedAmount timed out waiting for 0 buffer flush")
	}
}
