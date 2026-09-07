package signaling

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalHTTPSignaling(t *testing.T) {
	// Create signaling session with empty iceServers for offline execution
	session, err := NewSession([]webrtc.ICEServer{}, 2*time.Second)
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
	session, err := NewSession([]webrtc.ICEServer{}, 2*time.Second)
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
