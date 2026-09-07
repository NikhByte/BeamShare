package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelayServerAndClient(t *testing.T) {
	relaySrv := NewServer()
	ts := httptest.NewServer(relaySrv)
	defer ts.Close()

	client := NewClient(ts.URL)

	// 1. Session registration
	sessionID, err := client.Register()
	require.NoError(t, err)
	assert.NotEmpty(t, sessionID)

	// 2. State pushing
	offerSDP := "v=0\r\no=- 1000 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
	candidates := []map[string]interface{}{
		{"candidate": "candidate:1 1 UDP 2013266431 127.0.0.1 50000 typ host"},
	}
	meta := map[string]interface{}{
		"name": "testfile.txt",
		"size": float64(1024),
	}

	err = client.PushState(offerSDP, candidates, meta)
	require.NoError(t, err)

	// Query receiver endpoints
	t.Run("Receiver GET /api/meta", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/meta?s=" + sessionID)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var resMeta map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&resMeta)
		require.NoError(t, err)
		assert.Equal(t, "testfile.txt", resMeta["name"])
	})

	t.Run("Receiver GET /api/signal/offer", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/signal/offer?s=" + sessionID)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var resOffer map[string]string
		err = json.NewDecoder(resp.Body).Decode(&resOffer)
		require.NoError(t, err)
		assert.Equal(t, offerSDP, resOffer["sdp"])
	})

	t.Run("Receiver GET /api/signal/candidates", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/signal/candidates?s=" + sessionID)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var resCands []map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&resCands)
		require.NoError(t, err)
		assert.Len(t, resCands, 1)
	})

	// 3. Long-polling Mechanics
	t.Run("Poll Answer Action", func(t *testing.T) {
		pollCh := make(chan *PollCommand, 1)
		errCh := make(chan error, 1)

		go func() {
			cmd, err := client.Poll()
			if err != nil {
				errCh <- err
			} else {
				pollCh <- cmd
			}
		}()

		time.Sleep(50 * time.Millisecond) // Ensure poll is active

		// Receiver posts answer
		answerSDP := "{\"type\":\"answer\",\"sdp\":\"v=0\\r\\n\"}"
		resp, err := http.Post(ts.URL+"/api/signal/answer?s="+sessionID, "application/json", bytes.NewReader([]byte(answerSDP)))
		require.NoError(t, err)
		resp.Body.Close()

		select {
		case cmd := <-pollCh:
			assert.Equal(t, "answer", cmd.Action)
			assert.Equal(t, answerSDP, cmd.Answer)
		case err := <-errCh:
			t.Fatalf("poll failed: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("poll timed out waiting for answer")
		}
	})

	t.Run("Poll Context Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/relay/poll?session="+sessionID, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		// Context cancellation should terminate the poll request without erroring or hanging server
	})

	t.Run("Poll Download Action", func(t *testing.T) {
		pollCh := make(chan *PollCommand, 1)
		errCh := make(chan error, 1)

		go func() {
			cmd, err := client.Poll()
			if err != nil {
				errCh <- err
			} else {
				pollCh <- cmd
			}
		}()

		time.Sleep(50 * time.Millisecond)

		// Receiver requests download with a context that cancels after getting the response header
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/download?s="+sessionID, nil)
		require.NoError(t, err)

		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()

		select {
		case cmd := <-pollCh:
			assert.Equal(t, "download", cmd.Action)
			cancel() // cancel download request to unblock handler
		case err := <-errCh:
			t.Fatalf("poll failed: %v", err)
			cancel()
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("poll timed out waiting for download")
		}
	})
}

func TestEncryptedRelayDataStreaming(t *testing.T) {
	relaySrv := NewServer()
	ts := httptest.NewServer(relaySrv)
	defer ts.Close()

	// Generate temp file with 150KB random payload (spanning multiple 64KB frames)
	payloadSize := 150 * 1024
	testData := make([]byte, payloadSize)
	_, err := io.ReadFull(rand.Reader, testData)
	require.NoError(t, err)

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "stream_test.dat")
	err = os.WriteFile(filePath, testData, 0644)
	require.NoError(t, err)

	client := NewClient(ts.URL)
	key := make([]byte, 32)
	_, err = io.ReadFull(rand.Reader, key)
	require.NoError(t, err)
	client.Key = key

	sessionID, err := client.Register()
	require.NoError(t, err)

	err = client.PushState("offer", nil, map[string]interface{}{"name": "stream_test.dat", "size": float64(payloadSize)})
	require.NoError(t, err)

	// Channel for downloaded bytes
	receivedCh := make(chan []byte, 1)
	errCh := make(chan error, 1)

	// Receiver initiates download
	go func() {
		resp, err := http.Get(ts.URL + "/api/download?s=" + sessionID)
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()

		decReader, err := NewDecryptingReader(resp.Body, key)
		if err != nil {
			errCh <- err
			return
		}

		downloaded, err := io.ReadAll(decReader)
		if err != nil {
			errCh <- err
			return
		}
		receivedCh <- downloaded
	}()

	// Wait for download request signal on poll
	cmd, err := client.Poll()
	require.NoError(t, err)
	assert.Equal(t, "download", cmd.Action)

	// Sender uploads encrypted data
	err = client.UploadData(filePath)
	require.NoError(t, err)

	select {
	case downloaded := <-receivedCh:
		assert.Equal(t, testData, downloaded)
	case err := <-errCh:
		t.Fatalf("decrypted stream read failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("encrypted data transfer timed out")
	}
}
