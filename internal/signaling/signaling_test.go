package signaling

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalHTTPSignaling(t *testing.T) {
	// Create signaling session with empty iceServers for offline execution
	session, err := NewSession([]webrtc.ICEServer{}, 10*time.Second)
	require.NoError(t, err)
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		rxPC, err := NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
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
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer waitCancel()
		err = session.WaitForAnswer(waitCtx)
		require.NoError(t, err)
	})
}

func TestOpticalSDPExchange(t *testing.T) {
	session, err := NewSession([]webrtc.ICEServer{}, 10*time.Second)
	require.NoError(t, err)
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	session, err := NewSession(iceServers, 10*time.Second)
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

	pcReceiver, err := NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

func TestConcurrentCandidatesPolling(t *testing.T) {
	session, err := NewSession([]webrtc.ICEServer{}, 10*time.Second)
	require.NoError(t, err)
	defer session.Close()

	mux := http.NewServeMux()
	session.RegisterHandlers(mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Spawn writer goroutines simulating OnICECandidate callbacks appending candidates
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			candIdx := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
					session.mu.Lock()
					session.candidates = append(session.candidates, webrtc.ICECandidateInit{
						Candidate: fmt.Sprintf("candidate:%d-%d 1 UDP 2122260223 127.0.0.1 50000 typ host", id, candIdx),
					})
					session.mu.Unlock()
					candIdx++
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Spawn reader goroutines polling /api/signal/candidates
	var decodeErrors int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					resp, err := client.Get(ts.URL + "/api/signal/candidates")
					if err == nil {
						var cands []webrtc.ICECandidateInit
						decodeErr := json.NewDecoder(resp.Body).Decode(&cands)
						resp.Body.Close()
						if decodeErr != nil {
							atomic.AddInt32(&decodeErrors, 1)
						}
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(0), atomic.LoadInt32(&decodeErrors))
	assert.NotEmpty(t, session.GetCandidates())
}

func TestMinifySDPRelayCandidate(t *testing.T) {
	// Mock outbound IP finder to ensure deterministic IP selection across platforms
	oldFinder := outboundIPFinder
	outboundIPFinder = func() net.IP {
		return net.ParseIP("192.168.1.100")
	}
	defer func() { outboundIPFinder = oldFinder }()

	rawSDP := "v=0\r\n" +
		"o=- 123456 2 IN IP4 127.0.0.1\r\n" +
		"s=-\r\n" +
		"t=0 0\r\n" +
		"a=group:BUNDLE 0\r\n" +
		"a=candidate:1 1 UDP 2122260223 192.168.1.100 50000 typ host\r\n" +
		"a=candidate:2 1 UDP 1694498815 203.0.113.1 50001 typ srflx raddr 192.168.1.100 rport 50000\r\n" +
		"a=candidate:3 1 UDP 16777215 198.51.100.1 54321 typ relay raddr 192.168.1.100 rport 50000 generation 0\r\n"

	minified := minifySDP(rawSDP)

	// Verify relay candidate is present
	assert.Contains(t, minified, "typ relay")
	assert.Contains(t, minified, "198.51.100.1")

	// Verify host and srflx candidates are present
	assert.Contains(t, minified, "typ host")
	assert.Contains(t, minified, "typ srflx")

	// Verify raddr/rport truncation for relay candidate
	assert.NotContains(t, minified, "raddr")
	assert.NotContains(t, minified, "rport")

	// Verify roundtrip compression/decompression
	compressed, err := CompressSDP(rawSDP)
	require.NoError(t, err)
	assert.NotEmpty(t, compressed)

	decompressed, err := DecompressSDP(compressed)
	require.NoError(t, err)
	assert.Contains(t, decompressed, "typ relay")
	assert.Contains(t, decompressed, "198.51.100.1")

	// Compare compressed size with vs without relay candidate to ensure overhead is small (<80 bytes increase)
	rawSDPNoRelay := "v=0\r\n" +
		"o=- 123456 2 IN IP4 127.0.0.1\r\n" +
		"s=-\r\n" +
		"t=0 0\r\n" +
		"a=group:BUNDLE 0\r\n" +
		"a=candidate:1 1 UDP 2122260223 192.168.1.100 50000 typ host\r\n" +
		"a=candidate:2 1 UDP 1694498815 203.0.113.1 50001 typ srflx raddr 192.168.1.100 rport 50000\r\n"

	compressedNoRelay, err := CompressSDP(rawSDPNoRelay)
	require.NoError(t, err)

	sizeDiff := len(compressed) - len(compressedNoRelay)
	assert.LessOrEqual(t, sizeDiff, 80, "Compressed SDP length increase with TURN candidate should be <= 80 bytes")
}

func TestMinifySDPMultiTransportRelayCandidate(t *testing.T) {
	oldFinder := outboundIPFinder
	outboundIPFinder = func() net.IP {
		return net.ParseIP("192.168.1.100")
	}
	defer func() { outboundIPFinder = oldFinder }()

	rawSDP := "v=0\r\n" +
		"o=- 123456 2 IN IP4 127.0.0.1\r\n" +
		"s=-\r\n" +
		"t=0 0\r\n" +
		"a=group:BUNDLE 0\r\n" +
		"a=candidate:1 1 UDP 2122260223 192.168.1.100 50000 typ host\r\n" +
		"a=candidate:2 1 UDP 1694498815 203.0.113.1 50001 typ srflx raddr 192.168.1.100 rport 50000\r\n" +
		"a=candidate:3 1 UDP 16777215 198.51.100.1 54321 typ relay raddr 192.168.1.100 rport 50000 generation 0\r\n" +
		"a=candidate:4 1 UDP 16777215 198.51.100.2 54322 typ relay raddr 192.168.1.100 rport 50000 generation 0\r\n" +
		"a=candidate:5 1 TCP 150995711 198.51.100.1 3478 typ relay raddr 192.168.1.100 rport 50000 tcptype active\r\n" +
		"a=candidate:6 1 TLS 150995711 198.51.100.1 5349 typ relay raddr 192.168.1.100 rport 50000\r\n"

	minified := minifySDP(rawSDP)

	// Verify all three active transport relay candidates are preserved
	assert.Contains(t, minified, "1 UDP 16777215 198.51.100.1 54321 typ relay")
	assert.Contains(t, minified, "1 TCP 150995711 198.51.100.1 3478 typ relay")
	assert.Contains(t, minified, "1 TLS 150995711 198.51.100.1 5349 typ relay")

	// Verify duplicate UDP relay candidate was deduplicated
	assert.NotContains(t, minified, "198.51.100.2")

	// Verify raddr/rport truncation for all relay candidates
	assert.NotContains(t, minified, "raddr")
	assert.NotContains(t, minified, "rport")

	// Verify compressed SDP size overhead is < 120 bytes compared to single relay
	rawSDPSingleRelay := "v=0\r\n" +
		"o=- 123456 2 IN IP4 127.0.0.1\r\n" +
		"s=-\r\n" +
		"t=0 0\r\n" +
		"a=group:BUNDLE 0\r\n" +
		"a=candidate:1 1 UDP 2122260223 192.168.1.100 50000 typ host\r\n" +
		"a=candidate:2 1 UDP 1694498815 203.0.113.1 50001 typ srflx raddr 192.168.1.100 rport 50000\r\n" +
		"a=candidate:3 1 UDP 16777215 198.51.100.1 54321 typ relay raddr 192.168.1.100 rport 50000 generation 0\r\n"

	compressedMulti, err := CompressSDP(rawSDP)
	require.NoError(t, err)

	compressedSingle, err := CompressSDP(rawSDPSingleRelay)
	require.NoError(t, err)

	sizeDiff := len(compressedMulti) - len(compressedSingle)
	assert.Less(t, sizeDiff, 120, "Compressed SDP size increase with multi-transport relay candidates must be < 120 bytes")

	// Verify roundtrip compression/decompression
	decompressed, err := DecompressSDP(compressedMulti)
	require.NoError(t, err)
	assert.Contains(t, decompressed, "1 UDP 16777215 198.51.100.1 54321 typ relay")
	assert.Contains(t, decompressed, "1 TCP 150995711 198.51.100.1 3478 typ relay")
	assert.Contains(t, decompressed, "1 TLS 150995711 198.51.100.1 5349 typ relay")
}

func BenchmarkDecompressSDP(b *testing.B) {
	sampleSDP := "v=0\r\n" +
		"o=- 1234567890 2 IN IP4 127.0.0.1\r\n" +
		"s=-\r\n" +
		"t=0 0\r\n" +
		"a=group:BUNDLE 0\r\n" +
		"m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n" +
		"c=IN IP4 0.0.0.0\r\n" +
		"a=setup:actpass\r\n" +
		"a=mid:0\r\n" +
		"a=sctp-port:5000\r\n" +
		"a=max-message-size:262144\r\n" +
		"a=candidate:1 1 UDP 2122260223 192.168.1.50 54321 typ host\r\n" +
		"a=candidate:2 1 UDP 1694498815 203.0.113.1 54322 typ srflx raddr 192.168.1.50 rport 54321\r\n" +
		"a=candidate:3 1 UDP 84215039 198.51.100.1 54323 typ relay\r\n"

	compressed, err := CompressSDP(sampleSDP)
	if err != nil {
		b.Fatalf("failed to compress sample SDP: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DecompressSDP(compressed)
		if err != nil {
			b.Fatalf("decompression failed: %v", err)
		}
	}
}

func BenchmarkCompressSDP(b *testing.B) {
	sampleSDP := "v=0\r\n" +
		"o=- 1234567890 2 IN IP4 127.0.0.1\r\n" +
		"s=-\r\n" +
		"t=0 0\r\n" +
		"a=group:BUNDLE 0\r\n" +
		"m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n" +
		"c=IN IP4 0.0.0.0\r\n" +
		"a=setup:actpass\r\n" +
		"a=mid:0\r\n" +
		"a=sctp-port:5000\r\n" +
		"a=max-message-size:262144\r\n" +
		"a=candidate:1 1 UDP 2122260223 192.168.1.50 54321 typ host\r\n" +
		"a=candidate:2 1 UDP 1694498815 203.0.113.1 54322 typ srflx raddr 192.168.1.50 rport 54321\r\n"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := CompressSDP(sampleSDP)
		if err != nil {
			b.Fatalf("compression failed: %v", err)
		}
	}
}

