package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_RegistrationAndStateTimeout(t *testing.T) {
	// Slow server that hangs forever
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
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
	time.Sleep(200 * time.Millisecond)

	srv.mu.Lock()
	countAfter := len(srv.sessions)
	srv.mu.Unlock()
	assert.Equal(t, 0, countAfter)
}

func TestServer_DisconnectStreamCleanup(t *testing.T) {
	srv := NewServerWithConfig(1*time.Hour, 1*time.Minute)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sessID, err := client.Register(ctx)
	require.NoError(t, err)

	err = client.PushState(ctx, "offer-sdp", nil, map[string]interface{}{"size": 1024 * 1024 * 100})
	require.NoError(t, err)

	// Receiver starts downloading
	testHTTPClient := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	downloadCtx, downloadCancel := context.WithCancel(context.Background())
	reqDownload, err := http.NewRequestWithContext(downloadCtx, http.MethodGet, ts.URL+"/api/download?s="+sessID, nil)
	require.NoError(t, err)

	downloadStarted := make(chan struct{})
	downloadErrCh := make(chan error, 1)

	go func() {
		close(downloadStarted)
		resp, err := testHTTPClient.Do(reqDownload)
		if err != nil {
			downloadErrCh <- err
			return
		}
		defer resp.Body.Close()
		_, err = io.Copy(io.Discard, resp.Body)
		downloadErrCh <- err
	}()

	<-downloadStarted
	time.Sleep(50 * time.Millisecond)

	// Sender starts uploading infinite data
	uploadCtx, uploadCancel := context.WithCancel(context.Background())
	defer uploadCancel()

	infiniteReader, writer := io.Pipe()
	go func() {
		defer writer.Close()
		buf := make([]byte, 64*1024)
		for {
			_, err := writer.Write(buf)
			if err != nil {
				return
			}
		}
	}()

	reqUpload, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, ts.URL+"/relay/data?session="+sessID, infiniteReader)
	require.NoError(t, err)

	uploadErrCh := make(chan error, 1)
	go func() {
		resp, err := testHTTPClient.Do(reqUpload)
		if err != nil {
			uploadErrCh <- err
			return
		}
		resp.Body.Close()
		uploadErrCh <- nil
	}()

	time.Sleep(100 * time.Millisecond)

	// Simulate abrupt receiver disconnect by canceling download context
	startDisconnect := time.Now()
	downloadCancel()

	// Sender upload should terminate immediately
	select {
	case err := <-uploadErrCh:
		elapsed := time.Since(startDisconnect)
		t.Logf("Upload terminated after %v on receiver disconnect (err: %v)", elapsed, err)
		assert.Less(t, elapsed, 2*time.Second, "Upload did not terminate within 2 seconds")
	case <-time.After(3 * time.Second):
		t.Fatal("Upload timed out and did not terminate after receiver disconnect")
	}

	infiniteReader.Close()
}
