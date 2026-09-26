package relay

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
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

func TestServer_RateLimiterSweeperPurge(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	// Record failed attempt for IP
	srv.recordFailedAttempt("10.0.0.1")

	// Verify attempt recorded
	srv.failedAttemptsMu.Lock()
	fa, ok := srv.failedAttempts["10.0.0.1"]
	require.True(t, ok)
	// Backdate firstSeen > 1 minute
	fa.firstSeen = time.Now().Add(-2 * time.Minute)
	srv.failedAttemptsMu.Unlock()

	// Run sweeper purge
	srv.SweepExpiredSessions()

	// Verify IP record was purged
	srv.failedAttemptsMu.Lock()
	_, okAfter := srv.failedAttempts["10.0.0.1"]
	srv.failedAttemptsMu.Unlock()
	assert.False(t, okAfter, "Failed attempt record > 1 min old should be purged by sweeper")
}

func TestServer_RateLimiterBoundedMemory(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	// Simulate 100,000 unique failed IP lookups
	for i := 0; i < 100000; i++ {
		srv.recordFailedAttempt(fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xFF, (i>>8)&0xFF, i&0xFF))
	}

	srv.failedAttemptsMu.Lock()
	count := len(srv.failedAttempts)
	srv.failedAttemptsMu.Unlock()

	assert.LessOrEqual(t, count, maxFailedAttemptsEntries, "Rate limiter map size must be bounded by maxFailedAttemptsEntries")
	assert.Equal(t, maxFailedAttemptsEntries, count, "Rate limiter map size should reach maxFailedAttemptsEntries capacity")
}

func TestServer_ClosePipesClosesUploadPipes(t *testing.T) {
	sess := &Session{
		ID: "test-close-upload-pipes",
	}

	pr, pw := io.Pipe()
	sess.UploadPipeR = pr
	sess.UploadPipeW = pw

	sess.ClosePipes(fmt.Errorf("session error"))

	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)
	sess.mu.Unlock()

	// Verify pipes are closed
	buf := make([]byte, 10)
	_, errRead := pr.Read(buf)
	assert.Error(t, errRead)
	_, errWrite := pw.Write([]byte("test"))
	assert.Error(t, errWrite)
}

func TestServer_UploadContextCancellationGoroutineLeak(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	sess := srv.createSession()

	initialGoroutines := runtime.NumGoroutine()

	// Perform multiple abandoned upload requests
	for i := 0; i < 10; i++ {
		pipeR, pipeW := io.Pipe()
		body := bytes.NewBuffer(nil)
		mw := multipart.NewWriter(body)
		_, err := mw.CreateFormFile("file", "test.bin")
		require.NoError(t, err)

		reqCtx, reqCancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, pipeR)
		require.NoError(t, err)
		req.Header.Set("Content-Type", mw.FormDataContentType())

		errCh := make(chan error, 1)
		go func() {
			resp, errDo := http.DefaultClient.Do(req)
			if errDo == nil {
				resp.Body.Close()
			}
			errCh <- errDo
		}()

		// Write initial header bytes into pipe then cancel request context
		go func() {
			_, _ = pipeW.Write([]byte("--" + mw.Boundary() + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"test.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"))
			time.Sleep(20 * time.Millisecond)
			reqCancel()
			pipeW.Close()
		}()

		<-errCh
	}

	// Verify goroutine count returns to baseline
	assert.Eventually(t, func() bool {
		currentGoroutines := runtime.NumGoroutine()
		return currentGoroutines-initialGoroutines <= 2
	}, 2*time.Second, 10*time.Millisecond, "Goroutines leaked after abandoned uploads")
}

func TestServer_ReinitiateUploadClosesExistingPipe(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	sess := srv.createSession()

	// Upload 1 (started asynchronously as an unconsumed upload stream)
	body1 := &bytes.Buffer{}
	mw1 := multipart.NewWriter(body1)
	part1, err := mw1.CreateFormFile("file", "file1.bin")
	require.NoError(t, err)
	part1.Write([]byte("data1"))
	mw1.Close()

	req1, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, body1)
	require.NoError(t, err)
	req1.Header.Set("Content-Type", mw1.FormDataContentType())
	req1.Close = true

	go func() {
		resp1, errDo := http.DefaultClient.Do(req1)
		if errDo == nil {
			resp1.Body.Close()
		}
	}()

	time.Sleep(50 * time.Millisecond)

	oldPipeR := sess.UploadPipeR
	require.NotNil(t, oldPipeR)

	// Upload 2 (re-initiate upload on active session)
	body2 := &bytes.Buffer{}
	mw2 := multipart.NewWriter(body2)
	part2, err := mw2.CreateFormFile("file", "file2.bin")
	require.NoError(t, err)
	part2.Write([]byte("data2"))
	mw2.Close()

	req2, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, body2)
	require.NoError(t, err)
	req2.Header.Set("Content-Type", mw2.FormDataContentType())
	req2.Close = true

	go func() {
		resp2, err2 := http.DefaultClient.Do(req2)
		if err2 == nil {
			resp2.Body.Close()
		}
	}()

	time.Sleep(50 * time.Millisecond)

	reqPull, err := http.NewRequest(http.MethodGet, ts.URL+"/relay/pull?session="+sess.ID, nil)
	require.NoError(t, err)
	reqPull.Close = true

	respPull, errPull := http.DefaultClient.Do(reqPull)
	require.NoError(t, errPull)
	data, errRead := io.ReadAll(respPull.Body)
	respPull.Body.Close()
	require.NoError(t, errRead)
	assert.Equal(t, "data2", string(data))

	// Verify old pipe was closed
	buf := make([]byte, 10)
	_, errReadOld := oldPipeR.Read(buf)
	assert.Error(t, errReadOld, "Old upload pipe should be closed when re-initiating an upload")
	assert.True(t, strings.Contains(errReadOld.Error(), "replaced by new upload request") || strings.Contains(errReadOld.Error(), "closed pipe"), "Error should indicate pipe closed")
}


