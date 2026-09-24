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

func TestServer_LongPollingUnblockedAndGoroutinesCleanedOnExpiration(t *testing.T) {
	srv := NewServerWithConfig(100*time.Millisecond, 20*time.Millisecond)
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	ctx := context.Background()

	sessID, err := client.Register(ctx)
	require.NoError(t, err)

	sess := srv.GetSession(sessID)
	require.NotNil(t, sess)

	// Baseline goroutine count
	baselineGoroutines := runtime.NumGoroutine()

	// Launch multiple long-poll handlers blocking on AnswerReady/UploadReq/downloadNotify
	numPollers := 5
	errChan := make(chan int, numPollers)

	for i := 0; i < numPollers; i++ {
		go func() {
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/relay/poll?session=%s", ts.URL, sessID), nil)
			if reqErr != nil {
				errChan <- -1
				return
			}
			httpClient := newTestHTTPClient()
			defer httpClient.CloseIdleConnections()

			resp, doErr := httpClient.Do(req)
			if doErr != nil {
				errChan <- -1
				return
			}
			defer resp.Body.Close()
			errChan <- resp.StatusCode
		}()
	}

	// Verify goroutine count increased while polls are active
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() >= baselineGoroutines+numPollers
	}, 2*time.Second, 10*time.Millisecond, "Goroutine count should increase when long-polling HTTP handlers are blocked")

	// Verify AnswerReady and UploadReq channels are closed upon session expiration
	assert.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.closed
	}, 2*time.Second, 10*time.Millisecond, "Session should be marked closed after expiration sweep")

	// Verify all long-poll requests complete with HTTP 410 StatusGone
	for i := 0; i < numPollers; i++ {
		select {
		case statusCode := <-errChan:
			assert.Equal(t, http.StatusGone, statusCode, "Polling request should terminate with HTTP 410 StatusGone on session expiration")
		case <-time.After(3 * time.Second):
			t.Fatalf("Timeout waiting for long-poll request %d to unblock", i)
		}
	}

	// Verify channels are closed
	_, answerChanOpen := <-sess.AnswerReady
	assert.False(t, answerChanOpen, "AnswerReady channel must be closed on session expiration")

	_, uploadChanOpen := <-sess.UploadReq
	assert.False(t, uploadChanOpen, "UploadReq channel must be closed on session expiration")

	// Verify goroutine count returns to baseline post-expiration
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baselineGoroutines+1
	}, 3*time.Second, 20*time.Millisecond, "Goroutine count must return to baseline after long-poll handlers unblock on session expiration")
}

func TestServer_UploadPipeTeardownOnSessionExpiration(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()

	sessID, err := client.Register(testCtx)
	require.NoError(t, err)

	bodyReader, bodyWriter := io.Pipe()
	mw := multipart.NewWriter(bodyWriter)

	uploadErrCh := make(chan error, 1)

	go func() {
		defer bodyWriter.Close()
		pw, err := mw.CreateFormFile("file", "test.bin")
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		for {
			select {
			case <-testCtx.Done():
				return
			default:
				_, errWrite := pw.Write(buf)
				if errWrite != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	httpClient := newTestHTTPClient()
	go func() {
		req, err := http.NewRequestWithContext(testCtx, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyReader)
		if err != nil {
			uploadErrCh <- err
			return
		}
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Close = true
		resp, err := httpClient.Do(req)
		bodyWriter.Close()
		if err != nil {
			uploadErrCh <- err
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			uploadErrCh <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			return
		}
		uploadErrCh <- nil
	}()

	sess := srv.getSession(sessID)
	require.NotNil(t, sess)

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil && sess.UploadPipeW != nil
	}, 3*time.Second, 10*time.Millisecond, "Upload pipe not set")

	// Expire session and sweep
	sess.mu.Lock()
	sess.expiresAt = time.Now().Add(-1 * time.Hour)
	sess.mu.Unlock()

	srv.SweepExpiredSessions()
	bodyWriter.Close()

	select {
	case err := <-uploadErrCh:
		t.Logf("Upload handler terminated as expected: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Upload handler did not terminate after session expiration")
	}

	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)
	sess.mu.Unlock()
}

func TestServer_UploadAndPullContextCancellation(t *testing.T) {
	t.Run("Upload_Context_Cancelled", func(t *testing.T) {
		srv := NewServer()
		defer srv.Stop()

		ts := httptest.NewServer(srv)
		defer ts.Close()

		client := NewClient(ts.URL)
		testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer testCancel()

		sessID, err := client.Register(testCtx)
		require.NoError(t, err)

		bodyReader, bodyWriter := io.Pipe()
		mw := multipart.NewWriter(bodyWriter)

		go func() {
			defer bodyWriter.Close()
			pw, err := mw.CreateFormFile("file", "test.bin")
			if err != nil {
				return
			}
			buf := make([]byte, 1024)
			for {
				_, errWrite := pw.Write(buf)
				if errWrite != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		uploadCtx, uploadCancel := context.WithCancel(testCtx)
		req, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyReader)
		require.NoError(t, err)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Close = true

		go func() {
			<-uploadCtx.Done()
			bodyWriter.CloseWithError(uploadCtx.Err())
		}()

		uploadErrCh := make(chan error, 1)
		httpClient := newTestHTTPClient()
		go func() {
			resp, err := httpClient.Do(req)
			if err != nil {
				uploadErrCh <- err
				return
			}
			resp.Body.Close()
			uploadErrCh <- nil
		}()

		sess := srv.getSession(sessID)
		require.NotNil(t, sess)

		require.Eventually(t, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return sess.UploadPipeR != nil && sess.UploadPipeW != nil
		}, 3*time.Second, 10*time.Millisecond)

		uploadCancel()

		select {
		case <-uploadErrCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Upload handler did not exit on context cancellation")
		}

		require.Eventually(t, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return sess.UploadPipeR == nil && sess.UploadPipeW == nil
		}, 3*time.Second, 10*time.Millisecond, "Upload pipes were not cleared after upload context cancellation")
	})

	t.Run("Pull_Context_Cancelled", func(t *testing.T) {
		srv := NewServer()
		defer srv.Stop()

		ts := httptest.NewServer(srv)
		defer ts.Close()

		client := NewClient(ts.URL)
		testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer testCancel()

		sessID, err := client.Register(testCtx)
		require.NoError(t, err)

		bodyReader, bodyWriter := io.Pipe()
		mw := multipart.NewWriter(bodyWriter)

		go func() {
			defer bodyWriter.Close()
			pw, err := mw.CreateFormFile("file", "test.bin")
			if err != nil {
				return
			}
			buf := make([]byte, 1024)
			for {
				_, errWrite := pw.Write(buf)
				if errWrite != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		pullCtx, pullCancel := context.WithCancel(testCtx)

		go func() {
			select {
			case <-pullCtx.Done():
			case <-testCtx.Done():
			}
			bodyWriter.CloseWithError(fmt.Errorf("cancelled"))
		}()

		reqUpload, err := http.NewRequestWithContext(testCtx, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyReader)
		require.NoError(t, err)
		reqUpload.Header.Set("Content-Type", mw.FormDataContentType())
		reqUpload.Close = true

		uploadErrCh := make(chan error, 1)
		httpClient := newTestHTTPClient()
		go func() {
			resp, err := httpClient.Do(reqUpload)
			if err != nil {
				uploadErrCh <- err
				return
			}
			resp.Body.Close()
			uploadErrCh <- nil
		}()

		sess := srv.getSession(sessID)
		require.NotNil(t, sess)

		require.Eventually(t, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return sess.UploadPipeR != nil && sess.UploadPipeW != nil
		}, 3*time.Second, 10*time.Millisecond)

		reqPull, err := http.NewRequestWithContext(pullCtx, http.MethodGet, ts.URL+"/relay/pull?session="+sessID, nil)
		require.NoError(t, err)

		pullErrCh := make(chan error, 1)
		go func() {
			resp, err := httpClient.Do(reqPull)
			if err != nil {
				pullErrCh <- err
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 1024)
			for {
				_, errRead := resp.Body.Read(buf)
				if errRead != nil {
					pullErrCh <- errRead
					return
				}
			}
		}()

		time.Sleep(50 * time.Millisecond)
		pullCancel()

		select {
		case <-pullErrCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Pull handler did not exit on context cancellation")
		}

		select {
		case <-uploadErrCh:
		case <-time.After(3 * time.Second):
			t.Fatal("Upload handler did not exit after pull context cancellation")
		}

		require.Eventually(t, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return sess.UploadPipeR == nil && sess.UploadPipeW == nil
		}, 3*time.Second, 10*time.Millisecond, "Upload pipes were not cleared after pull context cancellation")
	})
}

func TestServer_RepeatedUploadRequestsClosePreviousPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := NewClient(ts.URL)
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()

	sessID, err := client.Register(testCtx)
	require.NoError(t, err)

	bodyReader1, bodyWriter1 := io.Pipe()
	mw1 := multipart.NewWriter(bodyWriter1)

	go func() {
		defer bodyWriter1.Close()
		pw, err := mw1.CreateFormFile("file", "file1.bin")
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		for {
			select {
			case <-testCtx.Done():
				return
			default:
				_, errWrite := pw.Write(buf)
				if errWrite != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	req1, err := http.NewRequestWithContext(testCtx, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyReader1)
	require.NoError(t, err)
	req1.Header.Set("Content-Type", mw1.FormDataContentType())
	req1.Close = true

	httpClient := newTestHTTPClient()
	uploadErrCh1 := make(chan error, 1)
	go func() {
		resp, err := httpClient.Do(req1)
		bodyWriter1.Close()
		if err != nil {
			uploadErrCh1 <- err
			return
		}
		resp.Body.Close()
		uploadErrCh1 <- nil
	}()

	sess := srv.getSession(sessID)
	require.NotNil(t, sess)

	var firstPipeR *io.PipeReader
	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		firstPipeR = sess.UploadPipeR
		return sess.UploadPipeR != nil && sess.UploadPipeW != nil
	}, 3*time.Second, 10*time.Millisecond)

	bodyReader2, bodyWriter2 := io.Pipe()
	mw2 := multipart.NewWriter(bodyWriter2)
	bodyWriter1.Close()

	go func() {
		defer bodyWriter2.Close()
		pw, err := mw2.CreateFormFile("file", "file2.bin")
		if err != nil {
			return
		}
		pw.Write([]byte("short file content"))
		mw2.Close()
	}()

	uploadCtx2, uploadCancel2 := context.WithCancel(testCtx)
	req2, err := http.NewRequestWithContext(uploadCtx2, http.MethodPost, ts.URL+"/api/upload?s="+sessID, bodyReader2)
	require.NoError(t, err)
	req2.Header.Set("Content-Type", mw2.FormDataContentType())
	req2.Close = true

	uploadErrCh2 := make(chan error, 1)
	go func() {
		resp, err := httpClient.Do(req2)
		bodyWriter2.Close()
		if err != nil {
			uploadErrCh2 <- err
			return
		}
		resp.Body.Close()
		uploadErrCh2 <- nil
	}()

	select {
	case <-uploadErrCh1:
	case <-time.After(3 * time.Second):
		t.Fatal("First upload did not terminate when replaced by second upload")
	}

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil && sess.UploadPipeR != firstPipeR
	}, 3*time.Second, 10*time.Millisecond, "Upload pipe was not replaced")

	uploadCancel2()
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

func TestSession_ClosePipesClosesUploadPipes(t *testing.T) {
	pr, pw := io.Pipe()
	sess := &Session{
		ID:          "test-pipe-close",
		UploadPipeR: pr,
		UploadPipeW: pw,
	}

	testErr := fmt.Errorf("session expired")
	sess.ClosePipes(testErr)

	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR, "UploadPipeR should be reset to nil")
	assert.Nil(t, sess.UploadPipeW, "UploadPipeW should be reset to nil")
	sess.mu.Unlock()

	buf := make([]byte, 10)
	_, errRead := pr.Read(buf)
	assert.Error(t, errRead, "Reading from closed UploadPipeR should return error")

	_, errWrite := pw.Write([]byte("test"))
	assert.Error(t, errWrite, "Writing to closed UploadPipeW should return error")
}

func TestServer_SessionExpirationUnblocksBlockedUpload(t *testing.T) {
	srv := NewServerWithConfig(10*time.Second, 10*time.Millisecond)
	defer srv.Stop()

	sess := srv.createSession()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	reqR, reqW := io.Pipe()
	mw := multipart.NewWriter(reqW)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		part, err := mw.CreateFormFile("file", "test.bin")
		if err != nil {
			reqW.CloseWithError(err)
			return
		}
		part.Write([]byte("initial chunk"))
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, reqR)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	client := newTestHTTPClient()
	respChan := make(chan *http.Response, 1)
	errChan := make(chan error, 1)

	go func() {
		resp, errDo := client.Do(req)
		if errDo != nil {
			errChan <- errDo
		} else {
			respChan <- resp
		}
	}()

	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.UploadPipeR != nil && sess.UploadPipeW != nil
	}, 2*time.Second, 10*time.Millisecond, "handleUpload did not set upload pipes")

	sess.mu.Lock()
	sess.expiresAt = time.Now().Add(-1 * time.Second)
	sess.mu.Unlock()

	srv.SweepExpiredSessions()

	go func() {
		time.Sleep(100 * time.Millisecond)
		reqW.CloseWithError(fmt.Errorf("client cleanup"))
	}()

	select {
	case resp := <-respChan:
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	case errDo := <-errChan:
		t.Logf("Client received error: %v", errDo)
	case <-ctx.Done():
		t.Fatal("Test timed out waiting for upload request to be unblocked by session expiration")
	}

	sess.mu.Lock()
	assert.Nil(t, sess.UploadPipeR)
	assert.Nil(t, sess.UploadPipeW)
	sess.mu.Unlock()
}

func TestServer_ReassignUploadPipesClosesPreviousPipes(t *testing.T) {
	srv := NewServer()
	defer srv.Stop()

	sess := srv.createSession()

	pr1, pw1 := io.Pipe()
	sess.mu.Lock()
	sess.UploadPipeR = pr1
	sess.UploadPipeW = pw1
	sess.mu.Unlock()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	go func() {
		for {
			sess.mu.Lock()
			pr := sess.UploadPipeR
			sess.mu.Unlock()
			if pr != nil && pr != pr1 {
				resp, err := http.Get(ts.URL + "/relay/pull?session=" + sess.ID)
				if err == nil {
					io.ReadAll(resp.Body)
					resp.Body.Close()
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	reqR, reqW := io.Pipe()
	mw := multipart.NewWriter(reqW)
	go func() {
		defer reqW.Close()
		defer mw.Close()
		part, _ := mw.CreateFormFile("file", "second.bin")
		part.Write([]byte("hello"))
	}()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload?s="+sess.ID, reqR)
	require.NoError(t, err)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	client := newTestHTTPClient()
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	_, errRead := pr1.Read(make([]byte, 10))
	assert.Error(t, errRead, "Previous UploadPipeR should be closed")

	_, errWrite := pw1.Write([]byte("data"))
	assert.Error(t, errWrite, "Previous UploadPipeW should be closed")
}
