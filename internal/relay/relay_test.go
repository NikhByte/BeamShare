package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
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
	sessionID, err := client.Register(context.Background())
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

	err = client.PushState(context.Background(), offerSDP, candidates, meta)
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
			cmd, err := client.Poll(context.Background())
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
			cmd, err := client.Poll(context.Background())
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

	sessionID, err := client.Register(context.Background())
	require.NoError(t, err)

	err = client.PushState(context.Background(), "offer", nil, map[string]interface{}{"name": "stream_test.dat", "size": float64(payloadSize)})
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
	cmd, err := client.Poll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "download", cmd.Action)

	// Sender uploads encrypted data
	err = client.UploadData(context.Background(), filePath)
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

func TestClient_RegistrationAndStateTimeout(t *testing.T) {
	// Slow server that hangs until request context is cancelled
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer slowServer.Close()

	client := NewClient(slowServer.URL)

	t.Run("Register timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, err := client.Register(ctx)
		elapsed := time.Since(start)

		require.Error(t, err)
		assert.Less(t, elapsed, 1*time.Second)
	})

	t.Run("PushState timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := client.PushState(ctx, "offer", nil, nil)
		elapsed := time.Since(start)

		require.Error(t, err)
		assert.Less(t, elapsed, 1*time.Second)
	})
}

func TestServer_CentralTickerSweeper(t *testing.T) {
	// Short TTL (100ms) and cleanup interval (20ms)
	srv := NewServerWithConfig(100*time.Millisecond, 20*time.Millisecond)
	defer srv.Stop()

	// Rapidly create 100 sessions
	initialGoroutines := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		srv.createSession()
	}

	// Confirm sessions exist
	srv.mu.Lock()
	countBefore := len(srv.sessions)
	srv.mu.Unlock()
	assert.Equal(t, 100, countBefore)

	// Verify no per-session goroutines were spawned
	// Total goroutines should be bounded (initial + sweeper goroutine)
	goroutinesAfter := runtime.NumGoroutine()
	assert.LessOrEqual(t, goroutinesAfter-initialGoroutines, 5)

	// Wait for sweeper to clean up expired sessions
	assert.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.sessions) == 0
	}, 3*time.Second, 10*time.Millisecond, "Sweeper failed to clean up expired sessions")
}

func TestServer_DisconnectStreamCleanup(t *testing.T) {
	srv := NewServerWithConfig(1*time.Hour, 1*time.Minute)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sessID, err := client.Register(ctx)
	require.NoError(t, err)

	err = client.PushState(ctx, "offer-sdp", nil, map[string]interface{}{"size": 1024 * 1024 * 100})
	require.NoError(t, err)

	// Receiver starts downloading
	receiverClient := &http.Client{}

	downloadCtx, downloadCancel := context.WithCancel(context.Background())
	defer downloadCancel()

	reqDownload, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, ts.URL+"/api/download?s="+sessID, nil)
	require.NoError(t, err)

	downloadErrCh := make(chan error, 1)
	var downloadBytesReceived atomic.Int64

	go func() {
		resp, err := receiverClient.Do(reqDownload)
		if err != nil {
			downloadErrCh <- err
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, 32*1024)
		for {
			n, errRead := resp.Body.Read(buf)
			if n > 0 {
				downloadBytesReceived.Add(int64(n))
			}
			if errRead != nil {
				downloadErrCh <- errRead
				return
			}
		}
	}()

	// Wait until download handler has attached pipes to session
	require.Eventually(t, func() bool {
		sess := srv.getSession(sessID)
		if sess == nil {
			return false
		}
		return sess.IsPipeReady()
	}, 3*time.Second, 10*time.Millisecond, "Download pipe not initialized in time")

	// Sender starts uploading infinite data
	senderClient := &http.Client{}

	uploadCtx, uploadCancel := context.WithCancel(context.Background())
	defer uploadCancel()

	infiniteReader, writer := io.Pipe()

	go func() {
		defer writer.Close()
		buf := make([]byte, 64*1024)
		for {
			_, errWrite := writer.Write(buf)
			if errWrite != nil {
				return
			}
		}
	}()

	reqUpload, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, ts.URL+"/relay/data?session="+sessID, infiniteReader)
	require.NoError(t, err)

	uploadErrCh := make(chan error, 1)
	go func() {
		resp, err := senderClient.Do(reqUpload)
		if err != nil {
			uploadErrCh <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			uploadErrCh <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			return
		}
		uploadErrCh <- nil
	}()

	// Wait until download stream has actively received streaming data
	require.Eventually(t, func() bool {
		return downloadBytesReceived.Load() > 0
	}, 5*time.Second, 10*time.Millisecond, "Data streaming did not start in time")

	// Simulate abrupt receiver disconnect by canceling download context
	startDisconnect := time.Now()
	downloadCancel()

	// Sender upload should terminate immediately
	select {
	case err := <-uploadErrCh:
		elapsed := time.Since(startDisconnect)
		t.Logf("Upload terminated after %v on receiver disconnect (err: %v)", elapsed, err)
		assert.Less(t, elapsed, 2*time.Second, "Upload did not terminate within 2 seconds")
	case <-time.After(5 * time.Second):
		t.Fatal("Upload timed out and did not terminate after receiver disconnect")
	}

	infiniteReader.Close()
}

func TestSessionIDGenerationAndCollisionResolution(t *testing.T) {
	srv := NewServer()

	// Verify default session ID has 128-bit hex entropy (32 characters)
	sess1 := srv.createSession()
	assert.Len(t, sess1.ID, 32)
	assert.Regexp(t, "^[0-9a-f]{32}$", sess1.ID)

	sess2 := srv.createSession()
	assert.NotEqual(t, sess1.ID, sess2.ID)

	// Test collision resolution retry loop
	callCount := 0
	srv.SessionIDGenerator = func() string {
		callCount++
		if callCount == 1 {
			return sess1.ID // Force a collision on first attempt
		}
		return "unique-session-id-after-retry"
	}

	sess3 := srv.createSession()
	assert.Equal(t, "unique-session-id-after-retry", sess3.ID)
	assert.Equal(t, 2, callCount)
	// Ensure original session was NOT overwritten
	assert.Equal(t, sess1, srv.GetSession(sess1.ID))
}
