package relay

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
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

func TestServer_RateLimiterIPEntriesSweptAfterOneMinute(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	// Record failed attempt for IP
	ip := "192.0.2.100"
	srv.recordFailedAttempt(ip)

	srv.failedAttemptsMu.Lock()
	fa, ok := srv.failedAttempts[ip]
	require.True(t, ok)
	// Artificially age the entry past 1 minute
	fa.firstSeen = time.Now().Add(-2 * time.Minute)
	srv.failedAttemptsMu.Unlock()

	// Sweep expired IPs
	srv.SweepExpiredIPs()

	srv.failedAttemptsMu.Lock()
	_, ok = srv.failedAttempts[ip]
	srv.failedAttemptsMu.Unlock()

	assert.False(t, ok, "Stale IP rate-limiter record should be swept")
}

func TestServer_UploadPipeTeardownOnSessionExpiration(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	sess := srv.createSession()
	pr, pw := io.Pipe()

	sess.mu.Lock()
	sess.UploadPipeR = pr
	sess.UploadPipeW = pw
	sess.expiresAt = time.Now().Add(-10 * time.Second)
	sess.mu.Unlock()

	srv.SweepExpiredSessions()

	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)
	sess.mu.Unlock()

	// Ensure pipes were closed with error
	buf := make([]byte, 10)
	_, err := pr.Read(buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestServer_UploadHandlerDisconnectUnblocksGoroutine(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	sess := srv.createSession()

	bodyR, bodyW := io.Pipe()
	mw := multipart.NewWriter(bodyW)

	reqCtx, reqCancel := context.WithCancel(context.Background())

	req := httptest.NewRequest(http.MethodPost, "/api/upload?s="+sess.ID, bodyR)
	req = req.WithContext(reqCtx)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	rec := httptest.NewRecorder()
	uploadDone := make(chan struct{})

	go func() {
		defer close(uploadDone)
		srv.ServeHTTP(rec, req)
	}()

	// Write field header to start the multipart upload
	fw, err := mw.CreateFormFile("file", "test.txt")
	require.NoError(t, err)
	fw.Write([]byte("initial chunk"))

	// Give handler time to process part and block writing
	time.Sleep(50 * time.Millisecond)

	// Cancel context to simulate client disconnect
	reqCancel()

	select {
	case <-uploadDone:
		// Success: handler unblocked and returned within 2 seconds
	case <-time.After(2 * time.Second):
		t.Fatal("Upload handler did not unblock within 2 seconds after context cancellation")
	}

	bodyW.Close()
	bodyR.Close()
}

func TestServer_PullHandlerDisconnectUnblocksGoroutine(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	sess := srv.createSession()

	bodyR, bodyW := io.Pipe()
	mw := multipart.NewWriter(bodyW)

	uploadReq := httptest.NewRequest(http.MethodPost, "/api/upload?s="+sess.ID, bodyR)
	uploadReq.Header.Set("Content-Type", mw.FormDataContentType())
	uploadRec := httptest.NewRecorder()

	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		srv.ServeHTTP(uploadRec, uploadReq)
	}()

	// Start multipart upload
	fw, err := mw.CreateFormFile("file", "test.txt")
	require.NoError(t, err)
	fw.Write([]byte("chunk 1"))

	time.Sleep(50 * time.Millisecond)

	// Now connect pull handler with a cancellable context
	pullCtx, pullCancel := context.WithCancel(context.Background())
	pullReq := httptest.NewRequest(http.MethodGet, "/relay/pull?session="+sess.ID, nil)
	pullReq = pullReq.WithContext(pullCtx)
	pullRec := httptest.NewRecorder()

	pullDone := make(chan struct{})
	go func() {
		defer close(pullDone)
		srv.ServeHTTP(pullRec, pullReq)
	}()

	time.Sleep(50 * time.Millisecond)

	// Cancel pull client context
	pullCancel()

	select {
	case <-pullDone:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Pull handler did not unblock within 2 seconds after pull client disconnect")
	}

	bodyW.Close()

	select {
	case <-uploadDone:
		// Success: pull cancellation also tore down upload pipe, unblocking upload handler
	case <-time.After(2 * time.Second):
		t.Fatal("Upload handler did not unblock within 2 seconds after pull client disconnect")
	}

	bodyR.Close()
}


