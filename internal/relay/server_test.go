package relay

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
			DisableKeepAlives:   true,
		},
	}
}

func TestServer_RapidSuccessiveDownloadRequestsQueued(t *testing.T) {
	relayServer := NewServer()
	defer relayServer.Stop()

	ts := httptest.NewServer(relayServer)
	defer ts.Close()

	client := NewClient(ts.URL)
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()

	sessID, err := client.Register(testCtx)
	require.NoError(t, err)

	err = client.PushState(testCtx, "sdp-offer", nil, map[string]interface{}{
		"name": "testfile.bin",
		"size": float64(10000),
	}, nil)
	require.NoError(t, err)

	ranges := []string{
		"bytes=0-1023",
		"bytes=1024-2047",
		"bytes=2048-3071",
		"bytes=3072-4095",
		"bytes=4096-5119",
	}

	httpClient := newTestHTTPClient()
	cancels := make([]context.CancelFunc, 0, len(ranges))

	for _, rng := range ranges {
		reqCtx, reqCancel := context.WithCancel(testCtx)
		cancels = append(cancels, reqCancel)

		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, ts.URL+"/api/download?s="+sessID, nil)
		require.NoError(t, err)
		req.Header.Set("Range", rng)

		go func(r *http.Request) {
			resp, errDo := httpClient.Do(r)
			if errDo == nil {
				resp.Body.Close()
			}
		}(req)

		// Brief delay to guarantee sequential HTTP request arrival order
		time.Sleep(10 * time.Millisecond)
	}

	sess := relayServer.getSession(sessID)
	require.NotNil(t, sess)
	assert.Equal(t, len(ranges), sess.DownloadQueueLen())

	// Long-poll 5 times and verify FIFO ordering
	for i, expectedRange := range ranges {
		cmd, errPoll := client.Poll(testCtx)
		require.NoError(t, errPoll, "Poll failed at index %d", i)
		assert.Equal(t, "download", cmd.Action)
		assert.Equal(t, expectedRange, cmd.Range)
	}

	for _, cancel := range cancels {
		cancel()
	}
	sess.ClosePipes(nil)

	assert.Equal(t, 0, sess.DownloadQueueLen())
}

func TestServer_ConcurrentDownloadRequestsThreadSafety(t *testing.T) {
	relayServer := NewServer()
	defer relayServer.Stop()

	ts := httptest.NewServer(relayServer)
	defer ts.Close()

	client := NewClient(ts.URL)
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()

	sessID, err := client.Register(testCtx)
	require.NoError(t, err)

	err = client.PushState(testCtx, "sdp-offer", nil, map[string]interface{}{
		"name": "concurrent.bin",
		"size": float64(100000),
	}, nil)
	require.NoError(t, err)

	numGoroutines := 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	httpClient := newTestHTTPClient()
	cancels := make([]context.CancelFunc, 0, numGoroutines)
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			reqCtx, reqCancel := context.WithCancel(testCtx)
			mu.Lock()
			cancels = append(cancels, reqCancel)
			mu.Unlock()

			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf("%s/api/download?s=%s", ts.URL, sessID), nil)
			if err != nil {
				return
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", idx*100))

			resp, errDo := httpClient.Do(req)
			if errDo == nil {
				resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()

	sess := relayServer.getSession(sessID)
	require.NotNil(t, sess)
	assert.Equal(t, numGoroutines, sess.DownloadQueueLen())

	// Poll all items
	for i := 0; i < numGoroutines; i++ {
		cmd, errPoll := client.Poll(testCtx)
		require.NoError(t, errPoll)
		assert.Equal(t, "download", cmd.Action)
	}

	for _, cancel := range cancels {
		cancel()
	}
	sess.ClosePipes(nil)

	assert.Equal(t, 0, sess.DownloadQueueLen())
}

func TestServer_DownloadQueueMaxCapacityLimit(t *testing.T) {
	sess := &Session{
		ID:             "test-cap-sess",
		downloadNotify: make(chan struct{}, maxDownloadQueueSize),
	}

	// Fill queue up to max capacity
	for i := 0; i < maxDownloadQueueSize; i++ {
		ok := sess.EnqueueDownload(DownloadRequest{Offset: int64(i)})
		assert.True(t, ok, "Enqueue failed before reaching capacity at %d", i)
	}

	assert.Equal(t, maxDownloadQueueSize, sess.DownloadQueueLen())

	// Overflow request beyond capacity should be rejected
	ok := sess.EnqueueDownload(DownloadRequest{Offset: 9999})
	assert.False(t, ok, "Enqueue should return false when queue is full")
	assert.Equal(t, maxDownloadQueueSize, sess.DownloadQueueLen())

	// Dequeue all items
	for i := 0; i < maxDownloadQueueSize; i++ {
		req, ok := sess.DequeueDownload()
		assert.True(t, ok)
		assert.Equal(t, int64(i), req.Offset)
	}

	assert.Equal(t, 0, sess.DownloadQueueLen())
}

func TestServer_DownloadQueueCleanupOnSessionExpiration(t *testing.T) {
	srv := NewServerWithConfig(50*time.Millisecond, 10*time.Millisecond)
	defer srv.Stop()

	sess := srv.createSession()
	for i := 0; i < 10; i++ {
		sess.EnqueueDownload(DownloadRequest{Offset: int64(i)})
	}
	assert.Equal(t, 10, sess.DownloadQueueLen())

	// Wait for sweeper to clean up expired session
	assert.Eventually(t, func() bool {
		return srv.GetSession(sess.ID) == nil
	}, 2*time.Second, 10*time.Millisecond, "Session did not expire in time")

	assert.Equal(t, 0, sess.DownloadQueueLen())
}

func TestServer_NotFoundHandler(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	req := httptest.NewRequest(http.MethodGet, "/unknown-page", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, "text/html; charset=utf-8", rr.Header().Get("Content-Type"))
	assert.Contains(t, rr.Body.String(), "404")
	assert.Contains(t, rr.Body.String(), "Page Not Found")
}

func TestServer_SessionIDEntropyAndUniqueness(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		sess := srv.createSession()
		require.NotNil(t, sess)
		// 16 bytes = 32 hex characters
		assert.Equal(t, 32, len(sess.ID), "Session ID should be 32 hex characters (128-bit entropy)")
		assert.False(t, seen[sess.ID], "Session IDs must be distinct and non-repeating")
		seen[sess.ID] = true
	}
}

func TestServer_SessionEnumerationRateLimited(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	// Simulate repeated failed session lookups from the same IP
	rateLimited := false
	for i := 0; i < 40; i++ {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/signal/offer?s=nonexistent_%d", i), nil)
		req.RemoteAddr = "192.0.2.1:12345"
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, req)

		if rr.Code == http.StatusTooManyRequests {
			rateLimited = true
			break
		}
	}

	assert.True(t, rateLimited, "Brute force session enumeration should trigger HTTP 429 Too Many Requests")
}

func TestServer_UploadPipeCleanupOnSessionExpiration(t *testing.T) {
	srv := NewServerWithConfig(50*time.Millisecond, 10*time.Millisecond)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestHTTPClient()
	defer client.CloseIdleConnections()

	sess := srv.createSession()
	sessID := sess.ID

	// Attach Data pipes and Upload pipes to test clearing all pipe references
	prData, pwData := io.Pipe()
	sess.SetPipes(prData, pwData)

	// Record initial goroutines
	initialGoroutines := runtime.NumGoroutine()

	// Prepare streaming multipart request for upload
	prBody, pwBody := io.Pipe()
	writer := multipart.NewWriter(pwBody)

	uploadErrCh := make(chan error, 1)
	go func() {
		defer close(uploadErrCh)
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload?s=%s", ts.URL, sessID), prBody)
		if err != nil {
			uploadErrCh <- err
			return
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		resp, err := client.Do(req)
		if err != nil {
			uploadErrCh <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			uploadErrCh <- fmt.Errorf("expected 200, got %d", resp.StatusCode)
			return
		}
	}()

	// Write multipart header and begin streaming file content
	go func() {
		part, err := writer.CreateFormFile("file", "test.bin")
		if err != nil {
			pwBody.CloseWithError(err)
			return
		}
		buf := make([]byte, 1024)
		for {
			_, err := part.Write(buf)
			if err != nil {
				pwBody.CloseWithError(err)
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Wait until handleUpload creates UploadPipeR / UploadPipeW
	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil && sess.UploadPipeW != nil
	}, 2*time.Second, 10*time.Millisecond, "Upload pipes were not set")

	// Wait for session to expire and sweeper to run
	require.Eventually(t, func() bool {
		return srv.GetSession(sessID) == nil
	}, 2*time.Second, 10*time.Millisecond, "Session did not expire")

	// Verify that all pipe references on session are set to nil
	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR, "UploadPipeR should be nil after expiration")
	assert.Nil(t, sess.UploadPipeW, "UploadPipeW should be nil after expiration")
	assert.Nil(t, sess.DataPipeR, "DataPipeR should be nil after expiration")
	assert.Nil(t, sess.DataPipeW, "DataPipeW should be nil after expiration")
	sess.mu.Unlock()

	// Verify blocked handleUpload unblocks immediately
	select {
	case err := <-uploadErrCh:
		assert.Error(t, err, "Upload request should fail due to session expiration")
	case <-time.After(2 * time.Second):
		t.Fatal("handleUpload did not unblock within timeout")
	}

	pwBody.Close()

	// Verify no goroutines leaked
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= initialGoroutines+2
	}, 2*time.Second, 20*time.Millisecond, "Goroutines leaked after session upload pipe cleanup")
}

func TestServer_ReinitiatingUploadClosesPreexistingPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestHTTPClient()
	defer client.CloseIdleConnections()

	sess := srv.createSession()
	sessID := sess.ID

	// First upload request
	prBody1, pwBody1 := io.Pipe()
	writer1 := multipart.NewWriter(pwBody1)

	uploadErrCh1 := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload?s=%s", ts.URL, sessID), prBody1)
		if err != nil {
			uploadErrCh1 <- err
			return
		}
		req.Header.Set("Content-Type", writer1.FormDataContentType())
		resp, err := client.Do(req)
		if err != nil {
			uploadErrCh1 <- err
			return
		}
		defer resp.Body.Close()
		uploadErrCh1 <- nil
	}()

	go func() {
		part, err := writer1.CreateFormFile("file", "file1.bin")
		if err != nil {
			pwBody1.CloseWithError(err)
			return
		}
		buf := make([]byte, 1024)
		for {
			if _, err := part.Write(buf); err != nil {
				pwBody1.CloseWithError(err)
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil && sess.UploadPipeW != nil
	}, 2*time.Second, 10*time.Millisecond)

	oldPr := sess.UploadPipeR

	// Second upload request re-initiating upload for the same session
	prBody2, pwBody2 := io.Pipe()
	writer2 := multipart.NewWriter(pwBody2)

	uploadErrCh2 := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/upload?s=%s", ts.URL, sessID), prBody2)
		if err != nil {
			uploadErrCh2 <- err
			return
		}
		req.Header.Set("Content-Type", writer2.FormDataContentType())
		resp, err := client.Do(req)
		if err != nil {
			uploadErrCh2 <- err
			return
		}
		defer resp.Body.Close()
		uploadErrCh2 <- nil
	}()

	go func() {
		part, err := writer2.CreateFormFile("file", "file2.bin")
		if err != nil {
			pwBody2.CloseWithError(err)
			return
		}
		part.Write([]byte("hello world"))
		writer2.Close()
		pwBody2.Close()
	}()

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil && sess.UploadPipeR != oldPr
	}, 2*time.Second, 10*time.Millisecond, "Upload pipe was not replaced")

	// Verify oldPr was closed
	buf := make([]byte, 10)
	_, err := oldPr.Read(buf)
	assert.Error(t, err, "Old PipeReader should be closed when replaced")

	// Clean up second upload request
	sess.ClosePipes(nil)
	select {
	case <-uploadErrCh1:
	case <-time.After(1 * time.Second):
	}
	select {
	case <-uploadErrCh2:
	case <-time.After(1 * time.Second):
	}

	pwBody1.Close()
}


