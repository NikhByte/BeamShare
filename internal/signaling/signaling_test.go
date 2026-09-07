package signaling

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
)

func TestSDPCompressionDecompression(t *testing.T) {
	rawSDP := "v=0\r\no=- 123456 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\na=candidate:1 1 UDP 2122260223 192.168.1.100 50000 typ host\r\na=candidate:2 1 UDP 2122260223 10.0.0.1 50001 typ host\r\n"

	// Mock outbound IP finder to return a static local IP
	oldFinder := outboundIPFinder
	outboundIPFinder = func() net.IP {
		return net.ParseIP("192.168.1.100")
	}
	defer func() { outboundIPFinder = oldFinder }()

	compressed, err := compressSDP(rawSDP)
	if err != nil {
		t.Fatalf("unexpected error compressing SDP: %v", err)
	}

	decompressed, err := DecompressSDP(compressed)
	if err != nil {
		t.Fatalf("unexpected error decompressing SDP: %v", err)
	}

	if !strings.Contains(decompressed, "192.168.1.100") {
		t.Fatalf("expected decompressed SDP to contain preferred host IP 192.168.1.100, got: %s", decompressed)
	}
}

func TestCheckNATWithProber(t *testing.T) {
	// Mock NAT prober returns true when behind NAT
	mockProber := func(stunURL, localIP string) bool {
		return localIP != "203.0.113.195"
	}

	if !CheckNATWithProber("stun:stun.l.google.com:19302", "192.168.1.50", mockProber) {
		t.Fatalf("expected CheckNATWithProber to return true for local IP 192.168.1.50")
	}

	if CheckNATWithProber("stun:stun.l.google.com:19302", "203.0.113.195", mockProber) {
		t.Fatalf("expected CheckNATWithProber to return false when public IP matches local IP")
	}
}

func TestSignalingHandlers(t *testing.T) {
	// Create signaling session with empty ICE servers to avoid external network calls during PC creation
	iceServers := []webrtc.ICEServer{}
	session, err := NewSession(iceServers, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer session.Close()

	// Create offer first so pc is in HaveLocalOffer state
	offer, err := session.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("failed to create offer: %v", err)
	}
	if err := session.pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("failed to set local description: %v", err)
	}

	session.rawOffer = offer.SDP
	session.offerSDP = "compressed-test-sdp"

	mux := http.NewServeMux()
	session.RegisterHandlers(mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Test GET /api/signal/offer
	res, err := http.Get(ts.URL + "/api/signal/offer")
	if err != nil {
		t.Fatalf("failed to fetch offer: %v", err)
	}
	defer res.Body.Close()

	var offerResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&offerResp); err != nil {
		t.Fatalf("failed to decode offer response: %v", err)
	}

	if offerResp["sdp"] != session.rawOffer {
		t.Fatalf("expected SDP %s, got %s", session.rawOffer, offerResp["sdp"])
	}

	// Test GET /api/signal/candidates
	resCands, err := http.Get(ts.URL + "/api/signal/candidates")
	if err != nil {
		t.Fatalf("failed to fetch candidates: %v", err)
	}
	defer resCands.Body.Close()

	var candidates []webrtc.ICECandidateInit
	if err := json.NewDecoder(resCands.Body).Decode(&candidates); err != nil {
		t.Fatalf("failed to decode candidates: %v", err)
	}

	pcReceiver, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create receiver peer connection: %v", err)
	}
	defer pcReceiver.Close()

	if err := pcReceiver.SetRemoteDescription(offer); err != nil {
		t.Fatalf("receiver failed to set remote description: %v", err)
	}

	answerSDP, err := pcReceiver.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("receiver failed to create answer: %v", err)
	}

	answerBytes, _ := json.Marshal(answerSDP)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	answerDone := make(chan error, 1)
	go func() {
		answerDone <- session.WaitForAnswer(ctx)
	}()

	resAns, err := http.Post(ts.URL+"/api/signal/answer", "application/json", bytes.NewReader(answerBytes))
	if err != nil {
		t.Fatalf("failed to post answer: %v", err)
	}
	resAns.Body.Close()

	if resAns.StatusCode != http.StatusOK {
		t.Fatalf("expected answer POST status 200, got %d", resAns.StatusCode)
	}

	select {
	case err := <-answerDone:
		if err != nil {
			t.Fatalf("WaitForAnswer returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for answer channel to unblock")
	}
}
