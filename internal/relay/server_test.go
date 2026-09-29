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

func TestServer_ClosePipesClosesUploadPipes(t *testing.T) {
	pr, pw := io.Pipe()
	sess := &Session{
		ID:          "test-upload-pipes-close",
		UploadPipeR: pr,
		UploadPipeW: pw,
	}

	expErr := fmt.Errorf("session closed test")
	sess.ClosePipes(expErr)

	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)

	_, err := pw.Write([]byte("data"))
	require.Error(t, err)

	buf := make([]byte, 10)
	_, err = pr.Read(buf)
	require.Error(t, err)
}

func TestServer_SessionSweeperUnblocksUploads(t *testing.T) {
	srv := NewServerWithConfig(50*time.Millisecond, 10*time.Millisecond)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	sess := srv.createSession()

	uploadCtx, uploadCancel := context.WithCancel(context.Background())
	defer uploadCancel()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer mw.Close()
		part, err := mw.CreateFormFile("file", "testfile.txt")
		if err != nil {
			return
		}
		part.Write([]byte("hello world long content that blocks when pipe fills"))
		bigBuf := make([]byte, 128*1024)
		part.Write(bigBuf)
	}()

	req, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, pr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	errChan := make(chan error, 1)
	go func() {
		resp, errDo := http.DefaultClient.Do(req)
		if errDo != nil {
			errChan <- errDo
			return
		}
		resp.Body.Close()
		errChan <- nil
	}()

	assert.Eventually(t, func() bool {
		return srv.GetSession(sess.ID) == nil
	}, 2*time.Second, 10*time.Millisecond, "Session should expire and be swept")

	select {
	case err := <-errChan:
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("Upload request did not unblock on session sweep")
	}
}

func TestServer_ReallocatingUploadPipesClosesPreviousPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	sess := srv.createSession()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	pr1, pw1 := io.Pipe()
	mw1 := multipart.NewWriter(pw1)
	go func() {
		defer pw1.Close()
		defer mw1.Close()
		part, err := mw1.CreateFormFile("file", "first.txt")
		if err != nil {
			return
		}
		part.Write(make([]byte, 128*1024))
	}()
	go func() {
		<-ctx1.Done()
		pw1.CloseWithError(ctx1.Err())
	}()

	req1, _ := http.NewRequestWithContext(ctx1, http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, pr1)
	req1.Header.Set("Content-Type", mw1.FormDataContentType())

	upload1Done := make(chan struct{})
	go func() {
		resp, err := http.DefaultClient.Do(req1)
		if err == nil {
			resp.Body.Close()
		}
		close(upload1Done)
	}()

	assert.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil
	}, 1*time.Second, 10*time.Millisecond)

	sess.mu.Lock()
	oldPipeR := sess.UploadPipeR
	sess.mu.Unlock()

	pr2, pw2 := io.Pipe()
	mw2 := multipart.NewWriter(pw2)
	go func() {
		defer pw2.Close()
		defer mw2.Close()
		part, err := mw2.CreateFormFile("file", "second.txt")
		if err != nil {
			return
		}
		part.Write([]byte("second file data"))
	}()

	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, pr2)
	req2.Header.Set("Content-Type", mw2.FormDataContentType())

	upload2Done := make(chan struct{})
	go func() {
		resp, err := http.DefaultClient.Do(req2)
		if err == nil {
			resp.Body.Close()
		}
		close(upload2Done)
	}()

	select {
	case <-upload1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("First upload request did not unblock when second upload arrived")
	}

	buf := make([]byte, 10)
	_, readErr := oldPipeR.Read(buf)
	require.Error(t, readErr)

	respPull, err := http.Get(ts.URL + "/relay/pull?session=" + sess.ID)
	require.NoError(t, err)
	pullData, err := io.ReadAll(respPull.Body)
	respPull.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, "second file data", string(pullData))

	select {
	case <-upload2Done:
	case <-time.After(2 * time.Second):
		t.Fatal("Second upload request did not finish after pull")
	}
}

func TestServer_UploadContextCancellationClosesPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	sess := srv.createSession()

	uploadCtx, uploadCancel := context.WithCancel(context.Background())

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		defer mw.Close()
		part, err := mw.CreateFormFile("file", "cancel.txt")
		if err != nil {
			return
		}
		buf := make([]byte, 32*1024)
		for {
			select {
			case <-uploadCtx.Done():
				return
			default:
				_, err := part.Write(buf)
				if err != nil {
					return
				}
			}
		}
	}()

	req := httptest.NewRequest(http.MethodPost, "/api/upload?s="+sess.ID, pr)
	req = req.WithContext(uploadCtx)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rr := httptest.NewRecorder()

	uploadDone := make(chan struct{})
	go func() {
		srv.ServeHTTP(rr, req)
		close(uploadDone)
	}()

	assert.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil
	}, 1*time.Second, 10*time.Millisecond)

	sess.mu.Lock()
	activePipeR := sess.UploadPipeR
	sess.mu.Unlock()

	uploadCancel()

	select {
	case <-uploadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Upload handler did not exit after context cancellation")
	}

	buf := make([]byte, 10)
	_, readErr := activePipeR.Read(buf)
	require.Error(t, readErr)
}

func TestServer_PullContextCancellationClosesPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	sess := srv.createSession()

	uploadCtx, uploadCancel := context.WithCancel(context.Background())
	defer uploadCancel()

	prUpload, pwUpload := io.Pipe()
	mw := multipart.NewWriter(pwUpload)
	go func() {
		defer pwUpload.Close()
		defer mw.Close()
		part, err := mw.CreateFormFile("file", "pullcancel.txt")
		if err != nil {
			return
		}
		buf := make([]byte, 32*1024)
		for {
			select {
			case <-uploadCtx.Done():
				return
			default:
				_, err := part.Write(buf)
				if err != nil {
					return
				}
			}
		}
	}()

	reqUpload := httptest.NewRequest(http.MethodPost, "/api/upload?s="+sess.ID, prUpload)
	reqUpload = reqUpload.WithContext(uploadCtx)
	reqUpload.Header.Set("Content-Type", mw.FormDataContentType())
	rrUpload := httptest.NewRecorder()

	uploadDone := make(chan struct{})
	go func() {
		srv.ServeHTTP(rrUpload, reqUpload)
		close(uploadDone)
	}()

	assert.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil
	}, 1*time.Second, 10*time.Millisecond)

	pullCtx, pullCancel := context.WithCancel(context.Background())
	reqPull := httptest.NewRequest(http.MethodGet, "/relay/pull?session="+sess.ID, nil)
	reqPull = reqPull.WithContext(pullCtx)
	rrPull := httptest.NewRecorder()

	pullDone := make(chan struct{})
	go func() {
		srv.ServeHTTP(rrPull, reqPull)
		close(pullDone)
	}()

	time.Sleep(50 * time.Millisecond)

	pullCancel()

	select {
	case <-pullDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Pull handler did not exit on context cancellation")
	}

	uploadCancel()

	select {
	case <-uploadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Upload handler did not exit after pull context cancellation")
	}
}


