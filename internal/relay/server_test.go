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

func TestSession_ClosePipesClosesUploadPipes(t *testing.T) {
	pr, pw := io.Pipe()
	sess := &Session{
		ID:          "test-upload-pipes",
		UploadPipeR: pr,
		UploadPipeW: pw,
	}

	testErr := fmt.Errorf("custom cleanup error")
	sess.ClosePipes(testErr)

	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR, "UploadPipeR should be nil after ClosePipes")
	assert.Nil(t, sess.UploadPipeW, "UploadPipeW should be nil after ClosePipes")
	sess.mu.Unlock()

	// Verify writing to pw returns error
	_, err := pw.Write([]byte("test"))
	assert.Error(t, err)

	// Verify reading from pr returns error
	buf := make([]byte, 10)
	_, err = pr.Read(buf)
	assert.Error(t, err)
}

func TestServer_SweepExpiredSessionsUnblocksUploadAndPull(t *testing.T) {
	srv := NewServerWithConfig(50*time.Millisecond, 10*time.Millisecond)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	ctx := context.Background()

	sessID, err := client.Register(ctx)
	require.NoError(t, err)

	// Create a pipe for body to simulate a continuous upload
	bodyR, bodyW := io.Pipe()

	// Build multipart request
	bodyPr, bodyPw := io.Pipe()
	writer := multipart.NewWriter(bodyPw)

	go func() {
		defer bodyPw.Close()
		partWriter, err := writer.CreateFormFile("file", "upload_test.dat")
		if err != nil {
			return
		}
		io.Copy(partWriter, bodyR)
		writer.Close()
	}()

	uploadErrCh := make(chan error, 1)
	uploadStatusCh := make(chan int, 1)

	uploadCtx, uploadCancel := context.WithCancel(context.Background())
	defer uploadCancel()

	req, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyPr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	httpClient := newTestHTTPClient()
	defer httpClient.CloseIdleConnections()

	go func() {
		defer bodyR.Close()
		defer bodyW.Close()
		resp, err := httpClient.Do(req)
		if err != nil {
			uploadErrCh <- err
			return
		}
		uploadStatusCh <- resp.StatusCode
		resp.Body.Close()
	}()

	// Write initial chunk to trigger part receiving in handleUpload
	_, err = bodyW.Write([]byte("initial chunk\n"))
	require.NoError(t, err)

	// Wait until session creates UploadPipeR
	assert.Eventually(t, func() bool {
		sess := srv.GetSession(sessID)
		if sess == nil {
			return false
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil
	}, 2*time.Second, 10*time.Millisecond, "UploadPipeR should be set")

	// Start handlePull in background
	pullErrCh := make(chan error, 1)
	pullStatusCh := make(chan int, 1)
	pullReq, err := http.NewRequest(http.MethodGet, ts.URL+"/relay/pull?session="+sessID, nil)
	require.NoError(t, err)

	go func() {
		resp, err := httpClient.Do(pullReq)
		if err != nil {
			pullErrCh <- err
			return
		}
		pullStatusCh <- resp.StatusCode
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}()

	// Wait for session to expire via sweeper
	assert.Eventually(t, func() bool {
		return srv.GetSession(sessID) == nil
	}, 2*time.Second, 10*time.Millisecond, "Session should expire and be removed")

	// Send another chunk and close upload body so client request body reaches EOF
	bodyW.Write([]byte("chunk 2\n"))
	bodyR.Close()
	bodyW.Close()

	select {
	case status := <-uploadStatusCh:
		assert.Equal(t, http.StatusInternalServerError, status, "handleUpload should return HTTP 500 when session expires during upload")
	case err := <-uploadErrCh:
		t.Fatalf("unexpected upload HTTP do error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("upload handler did not unblock after session expiration")
	}

	select {
	case status := <-pullStatusCh:
		assert.Equal(t, http.StatusOK, status)
	case err := <-pullErrCh:
		t.Fatalf("unexpected pull HTTP do error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("pull handler did not unblock after session expiration")
	}
}

func TestServer_ReplacedUploadClosesPreviousPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	ctx := context.Background()

	sessID, err := client.Register(ctx)
	require.NoError(t, err)

	sess := srv.GetSession(sessID)
	require.NotNil(t, sess)

	// First upload request
	bodyR1, bodyW1 := io.Pipe()
	bodyPr1, bodyPw1 := io.Pipe()
	writer1 := multipart.NewWriter(bodyPw1)

	go func() {
		defer bodyPw1.Close()
		partWriter, err := writer1.CreateFormFile("file", "first.dat")
		if err != nil {
			return
		}
		io.Copy(partWriter, bodyR1)
		writer1.Close()
	}()

	httpClient := newTestHTTPClient()
	defer httpClient.CloseIdleConnections()

	req1, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyPr1)
	require.NoError(t, err)
	req1.Header.Set("Content-Type", writer1.FormDataContentType())

	upload1StatusCh := make(chan int, 1)
	go func() {
		defer bodyR1.Close()
		defer bodyW1.Close()
		resp, err := httpClient.Do(req1)
		if err == nil {
			upload1StatusCh <- resp.StatusCode
			resp.Body.Close()
		}
	}()

	bodyW1.Write([]byte("chunk 1"))

	var oldPipeW *io.PipeWriter
	assert.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		oldPipeW = sess.UploadPipeW
		return oldPipeW != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Second upload request replaces first
	bodyR2, bodyW2 := io.Pipe()
	bodyPr2, bodyPw2 := io.Pipe()
	writer2 := multipart.NewWriter(bodyPw2)

	go func() {
		defer bodyPw2.Close()
		partWriter, err := writer2.CreateFormFile("file", "second.dat")
		if err != nil {
			return
		}
		io.Copy(partWriter, bodyR2)
		writer2.Close()
	}()

	req2, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyPr2)
	require.NoError(t, err)
	req2.Header.Set("Content-Type", writer2.FormDataContentType())

	upload2StatusCh := make(chan int, 1)
	go func() {
		defer bodyR2.Close()
		defer bodyW2.Close()
		resp, err := httpClient.Do(req2)
		if err == nil {
			upload2StatusCh <- resp.StatusCode
			resp.Body.Close()
		}
	}()

	go func() {
		bodyW2.Write([]byte("chunk 2"))
		bodyW2.Close()
		bodyR2.Close()
	}()

	select {
	case status := <-upload1StatusCh:
		assert.Equal(t, http.StatusInternalServerError, status, "First upload should be terminated with error when replaced")
	case <-time.After(2 * time.Second):
		t.Fatal("First upload did not unblock after replacement")
	}

	// Verify oldPipeW returns error on Write
	_, err = oldPipeW.Write([]byte("after replace"))
	assert.Error(t, err)

	sess.ClosePipes(nil)
}


