package relay

import (
	"context"
	"encoding/json"
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
	assert.Equal(t, 1, sess.DownloadQueueLen())

	// Poll should yield the latest request in single-slot buffer
	cmd, errPoll := client.Poll(testCtx)
	require.NoError(t, errPoll)
	assert.Equal(t, "download", cmd.Action)
	assert.Equal(t, ranges[len(ranges)-1], cmd.Range)

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
	assert.Equal(t, 1, sess.DownloadQueueLen())

	// Poll item
	cmd, errPoll := client.Poll(testCtx)
	require.NoError(t, errPoll)
	assert.Equal(t, "download", cmd.Action)

	for _, cancel := range cancels {
		cancel()
	}
	sess.ClosePipes(nil)

	assert.Equal(t, 0, sess.DownloadQueueLen())
}

func TestServer_DownloadQueueCleanupOnSessionExpiration(t *testing.T) {
	srv := NewServerWithConfig(50*time.Millisecond, 10*time.Millisecond)
	defer srv.Stop()

	sess := srv.createSession()
	for i := 0; i < 10; i++ {
		sess.EnqueueDownload(DownloadRequest{Offset: int64(i)})
	}
	assert.Equal(t, 1, sess.DownloadQueueLen())

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
		req.RemoteAddr = "198.51.100.1:12345"
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, req)

		if rr.Code == http.StatusTooManyRequests {
			rateLimited = true
			break
		}
	}

	assert.True(t, rateLimited, "Brute force session enumeration should trigger HTTP 429 Too Many Requests")
}

func TestServer_LongPollUnblocksOnSessionExpiration(t *testing.T) {
	srv := NewServerWithConfig(50*time.Millisecond, 10*time.Millisecond)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestHTTPClient()

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()

	// Register session
	regReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, ts.URL+"/relay/register", nil)
	require.NoError(t, err)

	resp, err := client.Do(regReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var regData map[string]string
	require.NoError(t, reqCtx.Err())
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&regData))
	resp.Body.Close()

	sessID := regData["session"]
	require.NotEmpty(t, sessID)

	sess := srv.GetSession(sessID)
	require.NotNil(t, sess)
	require.NotNil(t, sess.ctx, "Session must maintain a context.Context created at initialization")
	require.NotNil(t, sess.cancel, "Session must maintain a CancelCauseFunc created at initialization")

	// Start long-poll in a goroutine
	pollRespCh := make(chan *http.Response, 1)
	pollErrCh := make(chan error, 1)

	go func() {
		pollReq, pollErr := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf("%s/relay/poll?session=%s", ts.URL, sessID), nil)
		if pollErr != nil {
			pollErrCh <- pollErr
			return
		}
		pResp, pErr := client.Do(pollReq)
		if pErr != nil {
			pollErrCh <- pErr
			return
		}
		pollRespCh <- pResp
	}()

	// Ensure long-poll has started and is waiting
	time.Sleep(20 * time.Millisecond)

	// Wait for session to expire via sweeper
	select {
	case pResp := <-pollRespCh:
		defer pResp.Body.Close()
		assert.Equal(t, http.StatusNotFound, pResp.StatusCode, "Long poll should respond with HTTP 404 upon session expiration")
	case pErr := <-pollErrCh:
		t.Fatalf("Unexpected error during long-poll: %v", pErr)
	case <-time.After(3 * time.Second):
		t.Fatal("Long poll did not unblock within timeout after session expiration")
	}

	assert.Nil(t, srv.GetSession(sessID), "Session should be removed from server after expiration")
}

func TestServer_SeekingReaderCheckSessionContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	sess := &Session{
		ID:     "test-sr-sess",
		ctx:    ctx,
		cancel: cancel,
	}

	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	sr := &seekingReader{
		pr:   pr,
		sess: sess,
		ctx:  context.Background(),
	}

	// Cancel session context
	cancel(fmt.Errorf("session expired"))

	// Read should return session context cancellation error
	buf := make([]byte, 10)
	n, err := sr.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestServer_UploadPipeTransferTimeout(t *testing.T) {
	srv := NewServer()
	srv.UploadTimeout = 50 * time.Millisecond
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sessID, err := client.Register(ctx)
	require.NoError(t, err)

	bodyPr, bodyPw := io.Pipe()
	writer := multipart.NewWriter(bodyPw)

	go func() {
		defer bodyPw.Close()
		part, err := writer.CreateFormFile("file", "timeout.txt")
		if err != nil {
			return
		}
		// Write initial chunk
		_, _ = part.Write([]byte("initial chunk"))
		// Do not close writer or send more data - simulate network stall / receiver no-read
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyPr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	start := time.Now()
	resp, errDo := http.DefaultClient.Do(req)
	elapsed := time.Since(start)

	require.NoError(t, errDo)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Less(t, elapsed, 1*time.Second, "Upload write timeout should occur promptly")
}

func TestServer_RateLimiterBackgroundSweeper(t *testing.T) {
	srv := NewServerWithConfig(1*time.Hour, 20*time.Millisecond)
	defer srv.Stop()

	// Record failed attempts for two IPs
	srv.recordFailedAttempt("192.0.2.1")
	srv.recordFailedAttempt("192.0.2.2")

	srv.failedAttemptsMu.Lock()
	// Manually age 192.0.2.1 past 1 minute
	if fa, ok := srv.failedAttempts["192.0.2.1"]; ok {
		fa.firstSeen = time.Now().Add(-2 * time.Minute)
	}
	srv.failedAttemptsMu.Unlock()

	// Direct call test
	srv.SweepExpiredIPs()

	srv.failedAttemptsMu.Lock()
	_, existsStale := srv.failedAttempts["192.0.2.1"]
	_, existsFresh := srv.failedAttempts["192.0.2.2"]
	srv.failedAttemptsMu.Unlock()

	assert.False(t, existsStale, "Stale IP should be evicted by SweepExpiredIPs")
	assert.True(t, existsFresh, "Fresh IP should remain in rate limiter map")

	// Test automatic sweeper tick
	srv.failedAttemptsMu.Lock()
	if fa, ok := srv.failedAttempts["192.0.2.2"]; ok {
		fa.firstSeen = time.Now().Add(-2 * time.Minute)
	}
	srv.failedAttemptsMu.Unlock()

	assert.Eventually(t, func() bool {
		srv.failedAttemptsMu.Lock()
		defer srv.failedAttemptsMu.Unlock()
		return len(srv.failedAttempts) == 0
	}, 2*time.Second, 10*time.Millisecond, "Background ticker sweeper should purge stale rate limit records")
}
