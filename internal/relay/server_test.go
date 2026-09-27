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

func TestSession_ClosePipesClosesUploadPipes(t *testing.T) {
	sess := &Session{
		ID: "test-pipes-sess",
	}

	uPr, uPw := io.Pipe()
	dPr, dPw := io.Pipe()

	sess.UploadPipeR = uPr
	sess.UploadPipeW = uPw
	sess.DataPipeR = dPr
	sess.DataPipeW = dPw

	testErr := fmt.Errorf("session expired")
	sess.ClosePipes(testErr)

	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)
	assert.Nil(t, sess.DataPipeR)
	assert.Nil(t, sess.DataPipeW)

	// Verify pipes were closed
	_, err := uPw.Write([]byte("data"))
	require.Error(t, err)

	_, err = dPw.Write([]byte("data"))
	require.Error(t, err)
}

func TestSession_ClosePipesIfMatchWithUploadPipes(t *testing.T) {
	sess := &Session{
		ID: "test-pipes-match-sess",
	}

	uPr, uPw := io.Pipe()
	dPr, dPw := io.Pipe()

	sess.UploadPipeR = uPr
	sess.UploadPipeW = uPw
	sess.DataPipeR = dPr
	sess.DataPipeW = dPw

	testErr := fmt.Errorf("upload error")
	sess.ClosePipesIfMatch(uPr, uPw, testErr)

	// Upload pipes should be nil and closed, Data pipes should remain intact
	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)
	assert.NotNil(t, sess.DataPipeR)
	assert.NotNil(t, sess.DataPipeW)

	_, err := uPw.Write([]byte("data"))
	require.Error(t, err)

	// Cleanup data pipes
	sess.ClosePipes(nil)
}

func TestServer_SweepExpiredSessionsUnblocksUploadAndPullHandlers(t *testing.T) {
	srv := NewServerWithConfig(10*time.Hour, 0) // cleanup disabled, manual sweep
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	sess := srv.createSession()

	// Prepare a multipart request body using a pipe so it blocks on upload
	bodyPr, bodyPw := io.Pipe()
	t.Cleanup(func() { bodyPw.Close() })
	mw := multipart.NewWriter(bodyPw)

	uploadErrCh := make(chan error, 1)
	uploadRespCh := make(chan *http.Response, 1)

	go func() {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, bodyPr)
		if err != nil {
			uploadErrCh <- err
			return
		}
		req.Header.Set("Content-Type", mw.FormDataContentType())

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			uploadErrCh <- err
			return
		}
		uploadRespCh <- resp
	}()

	// Write multipart header and field in a separate goroutine
	go func() {
		fw, err := mw.CreateFormFile("file", "large.bin")
		if err != nil {
			return
		}
		// Write some initial chunk
		_, _ = fw.Write([]byte("start of file..."))
		// Do not close mw or bodyPw so io.Copy blocks waiting for more data
	}()

	// Wait until handleUpload sets UploadPipeR on the session
	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil
	}, 2*time.Second, 10*time.Millisecond, "Upload pipe was not initialized")

	// Now start handlePull
	pullRespCh := make(chan *http.Response, 1)
	pullErrCh := make(chan error, 1)

	go func() {
		resp, err := http.Get(ts.URL + "/relay/pull?session=" + sess.ID)
		if err != nil {
			pullErrCh <- err
			return
		}
		pullRespCh <- resp
	}()

	time.Sleep(50 * time.Millisecond)

	initialGoroutines := runtime.NumGoroutine()

	// Expire the session manually and sweep
	sess.mu.Lock()
	sess.expiresAt = time.Now().Add(-1 * time.Second)
	sess.mu.Unlock()

	srv.SweepExpiredSessions()

	// Verify handleUpload receives error / terminates
	select {
	case resp := <-uploadRespCh:
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		assert.Contains(t, string(body), "upload error")
	case err := <-uploadErrCh:
		t.Fatalf("upload request failed with transport error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("handleUpload did not unblock and terminate upon session expiration")
	}

	// Verify handlePull terminates
	select {
	case resp := <-pullRespCh:
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		t.Logf("Pull response status: %d, body: %s", resp.StatusCode, string(body))
	case err := <-pullErrCh:
		t.Logf("Pull failed with transport error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("handlePull did not unblock and terminate upon session expiration")
	}

	bodyPw.Close()

	// Verify goroutines terminated
	assert.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= initialGoroutines
	}, 2*time.Second, 50*time.Millisecond, "Goroutines leaked after session expiration sweep")
}

func TestServer_HandleUploadReplacesActiveUploadPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	sess := srv.createSession()

	// Start first upload request
	bodyPr1, bodyPw1 := io.Pipe()
	t.Cleanup(func() { bodyPw1.Close() })
	mw1 := multipart.NewWriter(bodyPw1)

	respCh1 := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, bodyPr1)
		req.Header.Set("Content-Type", mw1.FormDataContentType())
		resp, _ := http.DefaultClient.Do(req)
		respCh1 <- resp
	}()

	go func() {
		fw, _ := mw1.CreateFormFile("file", "first.bin")
		_, _ = fw.Write([]byte("first file chunk"))
	}()

	// Wait until first upload pipe is ready
	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Start second upload request replacing the first
	respCh2 := make(chan *http.Response, 1)
	go func() {
		var body2 bytes.Buffer
		mw2 := multipart.NewWriter(&body2)
		fw2, err := mw2.CreateFormFile("file", "second.bin")
		if err != nil {
			return
		}
		_, _ = fw2.Write([]byte("second file content"))
		mw2.Close()

		req2, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, &body2)
		if err != nil {
			return
		}
		req2.Header.Set("Content-Type", mw2.FormDataContentType())

		resp2, err := http.DefaultClient.Do(req2)
		if err == nil {
			respCh2 <- resp2
		}
	}()

	// Verify first upload returned error indicating replacement
	select {
	case resp1 := <-respCh1:
		if resp1 != nil {
			defer resp1.Body.Close()
			assert.Equal(t, http.StatusInternalServerError, resp1.StatusCode)
			body, _ := io.ReadAll(resp1.Body)
			assert.Contains(t, string(body), "upload error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first upload handler did not terminate when replaced")
	}

	// Pull second upload so req2 completes
	pullResp, err := http.Get(ts.URL + "/relay/pull?session=" + sess.ID)
	require.NoError(t, err)
	pullData, err := io.ReadAll(pullResp.Body)
	pullResp.Body.Close()
	assert.Equal(t, "second file content", string(pullData))

	select {
	case resp2 := <-respCh2:
		defer resp2.Body.Close()
		assert.Equal(t, http.StatusOK, resp2.StatusCode)
	case <-time.After(2 * time.Second):
		t.Fatal("second upload handler did not complete")
	}

	bodyPw1.Close()
}


