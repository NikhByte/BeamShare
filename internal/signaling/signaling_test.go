package signaling

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalHTTPSignaling(t *testing.T) {
	// Create signaling session with empty iceServers for offline execution
	session, err := NewSession([]webrtc.ICEServer{}, 100*time.Millisecond)
	require.NoError(t, err)
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	compressedOffer, err := session.CreateOffer(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, compressedOffer)
	assert.NotEmpty(t, session.RawOffer())

	mux := http.NewServeMux()
	session.RegisterHandlers(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	t.Run("GET /api/signal/offer", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/signal/offer")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var body map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&body)
		require.NoError(t, err)

		assert.Equal(t, "offer", body["type"])
		assert.Equal(t, session.RawOffer(), body["sdp"])
	})

	t.Run("GET /api/signal/candidates", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/signal/candidates")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var candidates []webrtc.ICECandidateInit
		err = json.NewDecoder(resp.Body).Decode(&candidates)
		require.NoError(t, err)
	})

	t.Run("POST /api/signal/answer", func(t *testing.T) {
		// Create a receiver peer connection to generate a valid SDP answer
		rxPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
		require.NoError(t, err)
		defer rxPC.Close()

		offerSDP := session.RawOffer()
		err = rxPC.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  offerSDP,
		})
		require.NoError(t, err)

		answerSDP, err := rxPC.CreateAnswer(nil)
		require.NoError(t, err)

		answerJSON, err := json.Marshal(answerSDP)
		require.NoError(t, err)

		// Test OPTIONS preflight
		reqOpt, err := http.NewRequest(http.MethodOptions, ts.URL+"/api/signal/answer", nil)
		require.NoError(t, err)
		respOpt, err := http.DefaultClient.Do(reqOpt)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, respOpt.StatusCode)
		respOpt.Body.Close()

		// Test POST answer
		resp, err := http.Post(ts.URL+"/api/signal/answer", "application/json", bytes.NewReader(answerJSON))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify WaitForAnswer unblocks
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer waitCancel()
		err = session.WaitForAnswer(waitCtx)
		require.NoError(t, err)
	})
}

func TestOpticalSDPExchange(t *testing.T) {
	session, err := NewSession([]webrtc.ICEServer{}, 100*time.Millisecond)
	require.NoError(t, err)
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	compressedOffer, err := session.CreateOffer(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, compressedOffer)

	// Verify decompression matches minified SDP
	decompressedSDP, err := DecompressSDP(compressedOffer)
	require.NoError(t, err)
	assert.Contains(t, decompressedSDP, "v=0")

	// Simulate QR optical payload URL construction & parsing
	targetURL := "http://127.0.0.1:8080/?mode=webrtc&sdp=" + url.QueryEscape(compressedOffer) + "&timeout=5000"
	parsedURL, err := url.Parse(targetURL)
	require.NoError(t, err)

	queryParams := parsedURL.Query()
	assert.Equal(t, "webrtc", queryParams.Get("mode"))
	assert.Equal(t, "5000", queryParams.Get("timeout"))

	sdpParam := queryParams.Get("sdp")
	require.NotEmpty(t, sdpParam)

	restoredSDP, err := DecompressSDP(sdpParam)
	require.NoError(t, err)
	assert.Equal(t, decompressedSDP, restoredSDP)

	t.Run("DecompressSDP with malformed inputs", func(t *testing.T) {
		_, err := DecompressSDP("!!!not-base64!!!")
		assert.Error(t, err)

		_, err = DecompressSDP("AAAA") // valid base64, invalid zlib
		assert.Error(t, err)
	})
}

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
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for answer channel to unblock")
	}
}

func TestParseICEURL(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectScheme string
		expectHost   string
		expectErr    bool
	}{
		{"Valid STUN with port", "stun:stun.l.google.com:19302", "stun", "stun.l.google.com:19302", false},
		{"Valid STUN default port", "stun:stun.example.com", "stun", "stun.example.com:3478", false},
		{"Valid STUNS default port", "stuns:stun.example.com", "stuns", "stun.example.com:5349", false},
		{"Valid TURN with query", "turn:turn.example.com:443?transport=tcp", "turn", "turn.example.com:443", false},
		{"Valid TURNS default port", "turns:turn.example.com", "turns", "turn.example.com:5349", false},
		{"Invalid scheme http", "http://example.com", "", "", true},
		{"Empty string", "", "", "", true},
		{"Missing scheme", "example.com:3478", "", "", true},
		{"Empty host", "stun:", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme, hostPort, err := ParseICEURL(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got nil", tc.input)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for input %q: %v", tc.input, err)
				}
				if scheme != tc.expectScheme {
					t.Errorf("expected scheme %q, got %q", tc.expectScheme, scheme)
				}
				if hostPort != tc.expectHost {
					t.Errorf("expected hostPort %q, got %q", tc.expectHost, hostPort)
				}
			}
		})
	}
}
